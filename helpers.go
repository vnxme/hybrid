// Copyright 2025 VNXME
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

// The isClosedChan function in this file was copied from
// https://github.com/golang/go/tree/go1.25.5 in December 2025.
// Source: https://github.com/golang/go/blob/go1.25.5/src/net/pipe.go
// License: https://github.com/golang/go/blob/go1.25.5/LICENSE
// The original copyright is preserved below.

// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hybrid

import (
	"net"
	"net/netip"
	"slices"
)

// addrKey identifies an address, e.g. as a map key: a *net.UDPAddr, a *net.TCPAddr or a *net.IPAddr
// by its netip.AddrPort, with a zero port for a *net.IPAddr and an IPv4 address unmapped, as packet info
// reports it, so that it matches however it's written, or any other address by its string. As it doesn't
// hold the network, keys should only compare addresses of a single network, e.g. of a single connection.
type addrKey struct {
	ap  netip.AddrPort
	str string
}

// addrKeyOf returns the key of an address. A nil pointer of a type held by its netip.AddrPort gives
// an empty key.
func addrKeyOf(a net.Addr) addrKey {
	var ap netip.AddrPort
	switch a := a.(type) {
	case *net.UDPAddr:
		ap = a.AddrPort()
	case *net.TCPAddr:
		ap = a.AddrPort()
	case *net.IPAddr:
		if a != nil {
			ip, _ := netip.AddrFromSlice(a.IP)
			ap = netip.AddrPortFrom(ip.WithZone(a.Zone), 0)
		}
	default:
		return addrKey{str: a.String()}
	}
	return addrKey{ap: netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())}
}

// valid reports whether the key holds an address.
func (k addrKey) valid() bool { return k.ap.IsValid() || k.str != "" }

// addrPortToAddr is addrPortToUDPAddr returning a net.Addr, which is nil for an invalid netip.AddrPort,
// rather than holding a nil *net.UDPAddr, which wouldn't compare equal to nil.
func addrPortToAddr(ap netip.AddrPort) net.Addr {
	if a := addrPortToUDPAddr(ap); a != nil {
		return a
	}
	return nil
}

// addrPortToUDPAddr returns a *net.UDPAddr for a valid netip.AddrPort, or nil.
func addrPortToUDPAddr(ap netip.AddrPort) *net.UDPAddr {
	if ap.IsValid() {
		return net.UDPAddrFromAddrPort(ap)
	}
	return nil
}

// cloneAddr returns a copy of a standard address, so that they don't share memory.
// Any other address, including a nil pointer of a standard type, is returned as is.
func cloneAddr(addr net.Addr) net.Addr {
	switch a := addr.(type) {
	case *net.UDPAddr:
		if a != nil {
			c := *a
			c.IP = slices.Clone(a.IP)
			return &c
		}
	case *net.TCPAddr:
		if a != nil {
			c := *a
			c.IP = slices.Clone(a.IP)
			return &c
		}
	case *net.IPAddr:
		if a != nil {
			c := *a
			c.IP = slices.Clone(a.IP)
			return &c
		}
	case *net.UnixAddr:
		if a != nil {
			c := *a
			return &c
		}
	}
	return addr
}

// equalAddr reports whether a and b are equal, handling nil.
func equalAddr(a, b net.Addr) bool { //nolint:unused // used by tests
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.String() == b.String() && a.Network() == b.Network()
}

// isClosedChan reports whether c is closed, without blocking.
func isClosedChan(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}
