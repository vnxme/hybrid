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
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"syscall"
)

// udpConnWithDualStackPacketInfo is a UDP connection with dual-stack packet info support.
type udpConnWithDualStackPacketInfo struct {
	UDPConn

	net string // Equals `udp`, `udp4` or `udp6`

	rdOOBSize atomic.Int32 // Size of control message buffers for reading, if grown from cMsgBufferSize
	port      uint16       // Contains the local port number

	v4Only bool // Equals true if no IPv6 could be expected
	v6Only bool // Equals true if no IPv4 could be expected
}

// NeedSelectiveUDPConn reports whether conn is bound to an unspecified address, so that the local
// address a packet comes to is only known from packet info, e.g. by wrapping conn with
// [NewSelectiveUDPConn]. It returns false if conn already implements SelectiveUDPConn,
// or if it doesn't provide access to the underlying socket.
func NeedSelectiveUDPConn(conn UDPConn) bool {
	if isNilUDPConn(conn) {
		return false
	}

	if _, ok := conn.(SelectiveUDPConn); ok {
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

func NewSelectiveUDPConn(conn UDPConn) (SelectiveUDPConn, error) {
	if isNilUDPConn(conn) {
		return nil, errors.New("UDPConn must not be nil")
	}

	if u, ok := conn.(SelectiveUDPConn); ok {
		return u, nil
	}

	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return nil, errors.New("UDPConn must have a local *net.UDPAddr")
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
		if v6Only, err = rawConnIsV6Only(raw); err != nil {
			return nil, err
		}
	}

	// Make platform-specific socket configuration calls to enable packet info
	err = rawConnEnablePacketInfo(raw, v4Only, v6Only)
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

	return &udpConnWithDualStackPacketInfo{
		UDPConn: conn,
		net:     network,
		port:    addr.AddrPort().Port(),
		v4Only:  v4Only,
		v6Only:  v6Only,
	}, nil
}

// ReadFromTo wraps around ReadFromToUDPAddrPort. Like [net.UDPConn.ReadFrom], it returns nil
// instead of a nil *net.UDPAddr for an address it doesn't know, so that it compares equal to nil.
func (u *udpConnWithDualStackPacketInfo) ReadFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
	n, src, dst, idx, err := u.ReadFromToUDPAddrPort(b)
	return n, addrFromValidAddrPort(src), addrFromValidAddrPort(dst), idx, err
}

// ReadFromToUDP wraps around ReadFromToUDPAddrPort.
func (u *udpConnWithDualStackPacketInfo) ReadFromToUDP(b []byte) (int, *net.UDPAddr, *net.UDPAddr, int, error) {
	n, src, dst, idx, err := u.ReadFromToUDPAddrPort(b)
	return n, UDPAddrFromValidAddrPort(src), UDPAddrFromValidAddrPort(dst), idx, err
}

// ReadFromToUDPAddrPort calls [net.UDPConn.ReadMsgUDPAddrPort] and a platform-specific packet info parser
// to obtain dst and idx from the control messages read into a pooled buffer. If they are truncated,
// e.g. as other socket options add control messages, the buffers for subsequent reads grow up to 64 KiB.
// The truncated read returns dst and idx only if packet info fits, and on Windows, it fails like
// [net.UDPConn.ReadMsgUDPAddrPort] does.
func (u *udpConnWithDualStackPacketInfo) ReadFromToUDPAddrPort(b []byte) (int, netip.AddrPort, netip.AddrPort, int, error) {
	stored := u.rdOOBSize.Load()
	size := int(stored)
	if size == 0 {
		size = cMsgBufferSize
	}
	oob, ref := getBuf(size)
	defer putBuf(ref)

	n, oobN, flags, src, err := u.ReadMsgUDPAddrPort(b, oob)
	if flags&cMsgTruncated != 0 && size < 1<<bufPoolMaxBits {
		// Windows reports the size control messages take, while other platforms report the size read
		u.rdOOBSize.CompareAndSwap(stored, int32(min(max(2*size, oobN), 1<<bufPoolMaxBits))) //nolint:gosec // G115
	}
	if err != nil { // Like [net.UDPConn.ReadFrom], return no addresses, as src is made of an empty sockaddr
		return n, netip.AddrPort{}, netip.AddrPort{}, 0, err
	}
	if dst, idx, ok := parsePacketInfo(oob[:min(oobN, len(oob))]); ok {
		return n, src, netip.AddrPortFrom(dst, u.port), idx, nil
	}
	return n, src, netip.AddrPort{}, 0, nil
}

// WriteToFrom wraps around WriteToFromUDP.
func (u *udpConnWithDualStackPacketInfo) WriteToFrom(b []byte, dst, src net.Addr, idx int) (int, error) {
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
func (u *udpConnWithDualStackPacketInfo) WriteToFromUDP(b []byte, dst, src *net.UDPAddr, idx int) (int, error) {
	return u.WriteToFromUDPAddrPort(b, dst.AddrPort(), src.AddrPort(), idx)
}

// WriteToFromUDPAddrPort does a few checks and calls a platform/protocol-specific marshaller
// to fill a pooled control message buffer before it proceeds to [net.UDPConn.WriteMsgUDPAddrPort].
// If idx is zero and src is invalid, it calls [net.UDPConn.WriteToUDPAddrPort].
func (u *udpConnWithDualStackPacketInfo) WriteToFromUDPAddrPort(b []byte, dst, src netip.AddrPort, idx int) (n int, err error) {
	var is4 bool
	switch {
	case u.v4Only:
		if idx >= 0 && dst.Addr().Unmap().BitLen() == 32 && src.Addr().Unmap().BitLen() != 128 {
			is4 = true
			goto proceed
		}
	case u.v6Only:
		if idx >= 0 && dst.Addr().Unmap().BitLen() == 128 && src.Addr().Unmap().BitLen() != 32 {
			goto proceed
		}
	default:
		switch dst.Addr().Unmap().BitLen() {
		case 32:
			if idx >= 0 && src.Addr().Unmap().BitLen() != 128 {
				is4 = true
				goto proceed
			}
		case 128:
			if idx >= 0 && src.Addr().Unmap().BitLen() != 32 {
				goto proceed
			}
		}
	}
	return 0, &net.OpError{Op: "write", Net: u.net, Source: addrFromValidAddrPort(src), Addr: addrFromValidAddrPort(dst), Err: syscall.EINVAL}

proceed:
	if idx == 0 && !src.IsValid() {
		return u.WriteToUDPAddrPort(b, dst)
	}

	oob, ref := getBuf(cMsgBufferSize)
	defer putBuf(ref)
	clear(oob) // A pooled buffer may contain anything, e.g. in padding bytes the marshaller skips

	var oobN int
	if is4 {
		oobN, _ = marshalPacketInfo4(oob, src.Addr().Unmap(), idx, true)
	} else {
		oobN, _ = marshalPacketInfo6(oob, src.Addr().Unmap(), idx, true)
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
	_ SelectiveUDPConn = (*udpConnWithDualStackPacketInfo)(nil)

	_ UDPConn = (*net.UDPConn)(nil)
)

var UDPListenWithPacketInfo = net.ListenConfig{Control: func(network, _ string, c syscall.RawConn) error {
	if !strings.HasPrefix(network, "udp") {
		return errors.New("network must begin with udp")
	}
	v4Only := strings.HasSuffix(network, "4")
	v6Only := !v4Only && strings.HasSuffix(network, "6")
	if v6Only { // Network is udp6 for a dual-stack socket too, so check whether it accepts IPv4
		only, err := rawConnIsV6Only(c)
		if err != nil {
			return err
		}
		v6Only = only
	}
	return rawConnEnablePacketInfo(c, v4Only, v6Only)
}}
