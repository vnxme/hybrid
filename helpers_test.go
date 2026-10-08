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

package hybrid

import (
	"net"
	"testing"
)

// logfIf calls [testing.T].Logf() if condition is true.
func logfIf(t *testing.T, condition bool, format string, args ...any) {
	t.Helper()
	if condition {
		t.Logf(format, args...)
	}
}

// equalNetAddr compares two net.Addr values, handling nil.
func equalNetAddr(a, b net.Addr) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.String() == b.String() && a.Network() == b.Network()
}

func TestCloneAddr(t *testing.T) {
	ip := func() net.IP { return net.IPv4(192, 0, 2, 1).To4() }
	for _, addr := range []net.Addr{
		&net.UDPAddr{IP: ip(), Port: 1, Zone: "z"},
		&net.TCPAddr{IP: ip(), Port: 2, Zone: "z"},
		&net.IPAddr{IP: ip(), Zone: "z"},
		&net.UnixAddr{Name: "/tmp/sock", Net: "unixgram"},
	} {
		c := cloneAddr(addr)
		if c == addr || !equalNetAddr(c, addr) {
			t.Errorf("cloneAddr(%v) = %v, want an equal copy", addr, c)
		}
		want := addr.String()
		switch a := c.(type) {
		case *net.UDPAddr:
			a.IP[3] = 99
		case *net.TCPAddr:
			a.IP[3] = 99
		case *net.IPAddr:
			a.IP[3] = 99
		case *net.UnixAddr:
			a.Name = "/tmp/other"
		}
		if addr.String() != want {
			t.Errorf("modifying a copy changed the original to %v, want %v", addr, want)
		}
	}

	// Other addresses, including nil ones, are returned as is
	for _, addr := range []net.Addr{nil, (*net.UDPAddr)(nil), oneTrunkAddr, pipeAddr{}} {
		if c := cloneAddr(addr); c != addr {
			t.Errorf("cloneAddr(%#v) = %#v, want it as is", addr, c)
		}
	}
}
