// Copyright 2026 VNXME
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hybrid

import (
	"errors"
	"math"
	"net"
	"net/netip"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/vnxme/hybrid/internal/pktinfo"
)

// packetInfoUDPConn is a FromToUDPConn wrapping a UDPConn with packet info enabled for every IP version it accepts.
type packetInfoUDPConn struct {
	UDPConn

	net string // Equals `udp`, `udp4` or `udp6`

	rdOOBSize atomic.Int32 // Size of control message buffers for reading, if grown from pktinfo.BufferSize
	port      uint16       // Contains the local port number

	v4Only bool // Equals true if no IPv6 could be expected
	v6Only bool // Equals true if no IPv4 could be expected
}

// NeedPacketInfoUDPConn reports whether conn is bound to an unspecified address, so that the local
// address a packet comes to is only known from packet info, e.g. by wrapping conn with
// [PacketInfoUDPConn]. It returns false if conn already implements FromToUDPConn,
// or if it doesn't provide access to the underlying socket.
func NeedPacketInfoUDPConn(conn UDPConn) bool {
	if isNilUDPConn(conn) {
		return false
	}

	if _, ok := conn.(FromToUDPConn); ok {
		return false
	}

	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return false
	}

	if _, err := conn.SyscallConn(); err != nil {
		return false
	}

	return addr.AddrPort().Addr().Unmap().IsUnspecified()
}

// PacketInfoUDPConn enables packet info on conn for every IP version it accepts, and wraps it
// to read the local address and interface index of each packet, and to write them with each packet.
// It returns conn as is if it already implements FromToUDPConn.
func PacketInfoUDPConn(conn UDPConn) (FromToUDPConn, error) {
	if isNilUDPConn(conn) {
		return nil, errors.New("hybrid: nil UDPConn")
	}

	if u, ok := conn.(FromToUDPConn); ok {
		return u, nil
	}

	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return nil, errors.New("hybrid: UDPConn has no local *net.UDPAddr")
	}

	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}

	// Detect whether the connection is IPv4/IPv6-only or dual-stack. Like any address obtained
	// from a socket, the local address has 4 bytes for an IPv4 socket and 16 bytes for an IPv6 one.
	var v4Only, v6Only bool
	switch ip := addr.AddrPort().Addr(); {
	case ip.Is4(): // An IPv4 socket
		v4Only = true
	case !ip.Unmap().IsUnspecified(): // An IPv6 socket bound to a specific address
		v4Only, v6Only = ip.Is4In6(), !ip.Is4In6()
	default: // An IPv6 socket bound to the unspecified address, dual-stack unless IPV6_V6ONLY is set
		if v6Only, err = pktinfo.IsV6Only(raw); err != nil {
			return nil, err
		}
	}

	// Make platform-specific socket configuration calls to enable packet info
	err = pktinfo.Enable(raw, v4Only, v6Only)
	if err != nil {
		return nil, err
	}

	network := "udp"
	switch {
	case v4Only:
		network = "udp4"
	case v6Only:
		network = "udp6"
	}

	return &packetInfoUDPConn{
		UDPConn: conn,
		net:     network,
		port:    addr.AddrPort().Port(),
		v4Only:  v4Only,
		v6Only:  v6Only,
	}, nil
}

// ReadFromTo wraps around ReadFromToUDPAddrPort. Like [net.UDPConn.ReadFrom], it returns nil
// instead of a nil *net.UDPAddr for an address it doesn't know, so that it compares equal to nil.
func (u *packetInfoUDPConn) ReadFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
	n, src, dst, idx, err := u.ReadFromToUDPAddrPort(b)
	return n, addrFromValidAddrPort(src), addrFromValidAddrPort(dst), idx, err
}

// ReadFromToUDP wraps around ReadFromToUDPAddrPort.
func (u *packetInfoUDPConn) ReadFromToUDP(b []byte) (int, *net.UDPAddr, *net.UDPAddr, int, error) {
	n, src, dst, idx, err := u.ReadFromToUDPAddrPort(b)
	return n, UDPAddrFromValidAddrPort(src), UDPAddrFromValidAddrPort(dst), idx, err
}

// ReadFromToUDPAddrPort calls [net.UDPConn.ReadMsgUDPAddrPort] and a platform-specific packet info parser
// to obtain dst and idx from the control messages read into a pooled buffer. If they are truncated,
// e.g. as other socket options add control messages, the buffers for subsequent reads grow up to 64 KiB.
// The truncated read returns dst and idx only if packet info fits, and on Windows, it fails like
// [net.UDPConn.ReadMsgUDPAddrPort] does. Unlike the latter, it returns IPv4 addresses unmapped
// on dual-stack sockets, so that src and dst are in the same form on any platform.
func (u *packetInfoUDPConn) ReadFromToUDPAddrPort(b []byte) (int, netip.AddrPort, netip.AddrPort, int, error) {
	stored := u.rdOOBSize.Load()
	size := int(stored)
	if size == 0 {
		size = pktinfo.BufferSize
	}
	oob, ref := getBuf(size)
	defer putBuf(ref)

	n, oobN, flags, src, err := u.ReadMsgUDPAddrPort(b, oob)
	if flags&pktinfo.Truncated != 0 && size < 1<<bufPoolMaxBits {
		// Windows reports the size control messages take, while other platforms report the size read
		u.rdOOBSize.CompareAndSwap(stored, int32(min(max(2*size, oobN), 1<<bufPoolMaxBits))) //nolint:gosec // G115
	}
	if err != nil { // Like [net.UDPConn.ReadFrom], return no addresses, as src is made of an empty sockaddr
		return n, netip.AddrPort{}, netip.AddrPort{}, 0, err
	}
	src = netip.AddrPortFrom(src.Addr().Unmap(), src.Port())
	if dst, idx, ok := pktinfo.Parse(oob[:min(oobN, len(oob))]); ok {
		return n, src, netip.AddrPortFrom(dst.Unmap(), u.port), idx, nil
	}
	return n, src, netip.AddrPort{}, 0, nil
}

// WriteToFrom wraps around WriteToFromUDP. It fails with [syscall.EINVAL] unless dst is a *net.UDPAddr,
// and src is nil or a *net.UDPAddr.
func (u *packetInfoUDPConn) WriteToFrom(b []byte, dst, src net.Addr, idx int) (int, error) {
	dstAddr, dstOk := dst.(*net.UDPAddr)
	if !dstOk {
		return 0, &net.OpError{Op: "write", Net: u.net, Source: src, Addr: dst, Err: syscall.EINVAL}
	}

	srcAddr, srcOk := src.(*net.UDPAddr)
	if src != nil && !srcOk {
		return 0, &net.OpError{Op: "write", Net: u.net, Source: src, Addr: dst, Err: syscall.EINVAL}
	}

	return u.WriteToFromUDP(b, dstAddr, srcAddr, idx)
}

// WriteToFromUDP wraps around WriteToFromUDPAddrPort.
func (u *packetInfoUDPConn) WriteToFromUDP(b []byte, dst, src *net.UDPAddr, idx int) (int, error) {
	return u.WriteToFromUDPAddrPort(b, dst.AddrPort(), src.AddrPort(), idx)
}

// WriteToFromUDPAddrPort calls a platform-specific packet info marshaller to fill a pooled control
// message buffer before it proceeds to [net.UDPConn.WriteMsgUDPAddrPort], or, if src is invalid and idx
// is zero, it calls [net.UDPConn.WriteToUDPAddrPort]. On Windows, it ignores idx if src is invalid, as the system
// doesn't choose a source address for packet info. It fails with [syscall.EINVAL] if idx is negative
// or above [math.MaxInt32], the largest index every platform has room for, if dst isn't of an IP version
// the socket accepts, or if src is valid but not of the same IP version as dst. Mapped IPv4 addresses
// count as IPv4 ones.
func (u *packetInfoUDPConn) WriteToFromUDPAddrPort(b []byte, dst, src netip.AddrPort, idx int) (n int, err error) {
	// Require dst of an IP version the socket accepts, and src, if any, of the same IP version
	bits, srcBits := dst.Addr().Unmap().BitLen(), src.Addr().Unmap().BitLen()
	is4 := bits == 32
	accepted := is4 && !u.v6Only || bits == 128 && !u.v4Only
	if idx < 0 || idx > math.MaxInt32 || !accepted || srcBits != 0 && srcBits != bits {
		return 0, &net.OpError{Op: "write", Net: u.net, Source: addrFromValidAddrPort(src), Addr: addrFromValidAddrPort(dst), Err: syscall.EINVAL}
	}

	// Windows sends a datagram from an unspecified source address in packet info as is, i.e. from 0.0.0.0
	// or ::, rather than choosing one, so let it choose both the address and the interface instead
	if !src.IsValid() && (idx == 0 || runtime.GOOS == "windows") {
		return u.WriteToUDPAddrPort(b, dst)
	}

	oob, ref := getBuf(pktinfo.BufferSize)
	defer putBuf(ref)
	clear(oob) // A pooled buffer may contain anything, e.g. in padding bytes the marshaller skips

	var oobN int
	if is4 {
		oobN, _ = pktinfo.Marshal4(oob, src.Addr().Unmap(), idx)
	} else {
		oobN, _ = pktinfo.Marshal6(oob, src.Addr().Unmap(), idx)
	}
	n, _, err = u.WriteMsgUDPAddrPort(b, oob[:oobN], dst)

	// Since Go 1.26, WriteMsgUDPAddrPort returns 0 bytes written on Windows, though it sends
	// the whole datagram, as WriteMsgInet4 and WriteMsgInet6 in internal/poll ignore the count.
	if err == nil && n == 0 {
		n = len(b) // A datagram is sent either whole or not at all
	}
	return n, err
}

// addrFromValidAddrPort returns a *net.UDPAddr if the given netip.AddrPort is valid, or nil.
// Unlike [UDPAddrFromValidAddrPort], it returns a nil net.Addr, not a nil *net.UDPAddr in it.
func addrFromValidAddrPort(ap netip.AddrPort) net.Addr {
	if ap.IsValid() {
		return net.UDPAddrFromAddrPort(ap)
	}
	return nil
}

// isNilUDPConn reports whether conn is nil, including a nil *net.UDPConn,
// whose methods would panic instead of returning an error.
func isNilUDPConn(conn UDPConn) bool {
	return conn == nil || conn == (*net.UDPConn)(nil)
}

// UDPAddrFromValidAddrPort returns a pointer to net.UDPAddr if the given netip.AddrPort is valid, or nil.
func UDPAddrFromValidAddrPort(ap netip.AddrPort) *net.UDPAddr {
	if ap.IsValid() {
		return net.UDPAddrFromAddrPort(ap)
	}
	return nil
}

// Interface guards
var (
	_ FromToUDPConn = (*packetInfoUDPConn)(nil)

	_ UDPConn = (*net.UDPConn)(nil)
)

// ControlUDPPacketInfo enables packet info on a UDP socket for every IP version it accepts.
// It's meant to be [net.ListenConfig.Control], or to be called from another Control function.
func ControlUDPPacketInfo(network, _ string, c syscall.RawConn) error {
	if !strings.HasPrefix(network, "udp") {
		return errors.New("hybrid: packet info on non-UDP network " + network)
	}
	v4Only := strings.HasSuffix(network, "4")
	v6Only := !v4Only && strings.HasSuffix(network, "6")
	if v6Only { // Network is udp6 for a dual-stack socket too, so check whether it accepts IPv4
		only, err := pktinfo.IsV6Only(c)
		if err != nil {
			return err
		}
		v6Only = only
	}
	return pktinfo.Enable(c, v4Only, v6Only)
}
