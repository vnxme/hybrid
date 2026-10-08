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
	"sync"
	"syscall"
)

// udpConnWithDualStackPacketInfo is a UDP connection with dual-stack packet info support.
type udpConnWithDualStackPacketInfo struct {
	UDPConn

	rdOOB  []byte // May contain OOB to parse while reading
	wr4OOB []byte // Filled with OOB if v6Only equals false
	wr6OOB []byte // Filled with OOB if v4Only equals false

	rdMutex sync.Mutex // Guards rdOOB
	wrMutex sync.Mutex // Guards wr4OOB and wr6OOB

	net  string // Equals `udp`, `udp4` or `udp6`
	port uint16 // Contains the local port number

	v4Only bool // Equals true if no IPv6 could be expected
	v6Only bool // Equals true if no IPv4 could be expected
}

// NeedSelectiveUDPConn reports whether conn is bound to an unspecified address, so that the local
// address a packet comes to is only known from packet info, e.g. by wrapping conn with
// [NewSelectiveUDPConn]. It returns false if conn already implements SelectiveUDPConn,
// or if it doesn't provide access to the underlying socket.
func NeedSelectiveUDPConn(conn UDPConn) bool {
	if conn == nil {
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
	if conn == nil {
		return nil, errors.New("UDPConn must not be nil")
	}

	if u, ok := conn.(SelectiveUDPConn); ok {
		return u, nil
	}

	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}

	// Make an invalid write call to retrieve [conn.fd.net]
	var opErr *net.OpError
	_, err = conn.WriteTo(nil, nil)
	if !errors.As(err, &opErr) {
		return nil, err
	}

	// Detect whether the given connection is IPv4/IPv6-only or dual-stack
	v4Only := strings.HasSuffix(opErr.Net, "4")
	v6Only := !v4Only && strings.HasSuffix(opErr.Net, "6")

	// If conn.fd.net is `udp`, the local address might still be specified,
	// so it might be possible to limit the connection to IPv4 or IPv6 only.
	if !v4Only && !v6Only {
		if addr, ok := opErr.Source.(*net.UDPAddr); ok {
			a := addr.AddrPort().Addr().Unmap()
			if !a.IsUnspecified() {
				v4Only, v6Only = a.Is4(), a.Is6()
			}
		}
	}

	// Make platform-specific socket configuration calls to enable packet info
	err = rawConnEnablePacketInfo(raw, v4Only, v6Only)
	if err != nil {
		return nil, err
	}

	u := udpConnWithDualStackPacketInfo{
		UDPConn: conn,
		net:     opErr.Net,
		port:    uint16(conn.LocalAddr().(*net.UDPAddr).Port), //nolint:gosec // G115
		v4Only:  v4Only,
		v6Only:  v6Only,
	}

	// Initialize three internal control message buffers and pre-fill two of them
	u.rdOOB, u.wr4OOB, u.wr6OOB = makeBuffersForPacketInfo(v4Only, v6Only)

	return &u, nil
}

// ReadFromTo wraps around ReadFromToUDP.
func (u *udpConnWithDualStackPacketInfo) ReadFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
	return u.ReadFromToUDP(b)
}

// ReadFromToUDP wraps around ReadFromToUDPAddrPort.
func (u *udpConnWithDualStackPacketInfo) ReadFromToUDP(b []byte) (int, *net.UDPAddr, *net.UDPAddr, int, error) {
	n, src, dst, idx, err := u.ReadFromToUDPAddrPort(b)
	return n, UDPAddrFromValidAddrPort(src), UDPAddrFromValidAddrPort(dst), idx, err
}

// ReadFromToUDPAddrPort calls [net.UDPConn.ReadMsgUDPAddrPort] and a platform-specific packet info parser
// to obtain dst and idx from the internal OOB read buffer. It grows the buffer, if it isn't large enough.
func (u *udpConnWithDualStackPacketInfo) ReadFromToUDPAddrPort(b []byte) (int, netip.AddrPort, netip.AddrPort, int, error) {
	u.rdMutex.Lock()
	defer u.rdMutex.Unlock()

	n, oobN, _, src, err := u.ReadMsgUDPAddrPort(b, u.rdOOB)
	if oobN > 0 && oobN <= len(u.rdOOB) && src.IsValid() {
		if dst, idx, ok := parsePacketInfo(u.rdOOB[:oobN]); ok {
			return n, src, netip.AddrPortFrom(dst, u.port), idx, err
		}
	} else if oobN > len(u.rdOOB) {
		u.rdOOB = make([]byte, oobN)
	}

	return n, src, netip.AddrPort{}, 0, err
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
// to fill the internal OOB write buffer before it proceeds to [net.UDPConn.WriteMsgUDPAddrPort].
// It grows the buffer if it isn't large enough. If idx is zero and src is invalid,
// it calls [net.UDPConn.WriteToUDPAddrPort].
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
	return 0, &net.OpError{Op: "write", Net: u.net, Source: UDPAddrFromValidAddrPort(src), Addr: UDPAddrFromValidAddrPort(dst), Err: syscall.EINVAL}

proceed:
	if idx == 0 && !src.IsValid() {
		return u.WriteToUDPAddrPort(b, dst)
	}

	u.wrMutex.Lock()
	defer u.wrMutex.Unlock()

	var ok, once bool
	if is4 {
	marshal4:
		if n, ok = marshalPacketInfo4(u.wr4OOB, src.Addr().Unmap(), idx, once); !ok && !once {
			u.wr4OOB, once = make([]byte, n), true
			goto marshal4
		}
		n, _, err = u.WriteMsgUDPAddrPort(b, u.wr4OOB[:n], dst)
	} else {
	marshal6:
		if n, ok = marshalPacketInfo6(u.wr6OOB, src.Addr().Unmap(), idx, once); !ok && !once {
			u.wr6OOB, once = make([]byte, n), true
			goto marshal6
		}
		n, _, err = u.WriteMsgUDPAddrPort(b, u.wr6OOB[:n], dst)
	}

	return n, err
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
