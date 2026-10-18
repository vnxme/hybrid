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

//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris || windows || zos

package pktinfo

import (
	"math"
	"net"
	"net/netip"
	"runtime"
	"strconv"
	"testing"
)

func BenchmarkIsLocalIP6(b *testing.B) {
	ip0 := netip.Addr{}
	ip4 := netip.AddrFrom4([4]byte{127, 0, 0, 1})
	ip6 := netip.AddrFrom16([16]byte{15: 1})
	ip6l1 := netip.AddrFrom16([16]byte{0: 0xfe, 1: 0x80, 15: 0x01})
	ip6l2 := netip.AddrFrom16([16]byte{0: 0xff, 1: 0x01, 15: 0x01})
	ip6l3 := netip.AddrFrom16([16]byte{0: 0xff, 1: 0x02, 15: 0x01})

	b.Run("Main", func(b *testing.B) {
		f := isLocalIP6
		for i := 0; i < b.N; i++ {
			_ = f(ip0)
			_ = f(ip4)
			_ = f(ip6)
			_ = f(ip6l1)
			_ = f(ip6l2)
			_ = f(ip6l3)
		}
	})

	b.Run("Naive", func(b *testing.B) {
		f := func(x netip.Addr) bool {
			return x.IsLinkLocalUnicast() || x.IsInterfaceLocalMulticast() || x.IsLinkLocalMulticast()
		}
		for i := 0; i < b.N; i++ {
			_ = f(ip0)
			_ = f(ip4)
			_ = f(ip6)
			_ = f(ip6l1)
			_ = f(ip6l2)
			_ = f(ip6l3)
		}
	})
}

// TestMarshalParse6 ensures IPv6 packet info, which has the same layout for sending and receiving,
// is parsed as it has been marshaled, except that a mapped IPv4 address is parsed unmapped.
// IPv4 packet info isn't parsed, as it has different meanings or even types for sending and receiving,
// e.g. on Linux, in_pktinfo carries the source address in ipi_spec_dst for sending, but the destination
// one in ipi_addr for receiving, while BSD sends IP_SENDSRCADDR with no interface index. Real IPv4
// control messages are covered by TestControlUDPPacketInfo and TestPacketInfoUDPConnInterfaces
// in the hybrid package.
func TestMarshalParse6(t *testing.T) {
	for _, test := range []struct {
		src  netip.Addr
		idx  int
		want netip.Addr
	}{
		{netip.Addr{}, 0, netip.IPv6Unspecified()},
		{netip.Addr{}, 1, netip.IPv6Unspecified()},
		{netip.Addr{}, math.MaxInt32, netip.IPv6Unspecified()},
		{netip.IPv6Unspecified(), 0, netip.IPv6Unspecified()},
		{netip.IPv6Loopback(), 0, netip.IPv6Loopback()},
		{netip.IPv6Loopback(), 1, netip.IPv6Loopback()},
		{netip.MustParseAddr("2001:db8::1"), math.MaxInt32, netip.MustParseAddr("2001:db8::1")},
		{netip.MustParseAddr("::ffff:10.1.2.3"), 1, netip.MustParseAddr("10.1.2.3")}, // Unmapped
	} {
		oob := make([]byte, BufferSize)
		for i := range oob {
			oob[i] = 0xff // Any byte left unwritten is noticed
		}
		n := Marshal(oob, false, test.src, test.idx)
		if dst, idx, ok := Parse(oob[:n]); dst != test.want || idx != test.idx || !ok {
			t.Errorf("Parse() of Marshal(%s, %d) = %s, %d, %v, want %s, %d, true",
				test.src, test.idx, dst, idx, ok, test.want, test.idx)
		}
	}
}

// TestIsLocalIP6 ensures link-local unicast, interface-local multicast and link-local multicast addresses,
// whatever multicast flags they have, are local, unlike the others.
func TestIsLocalIP6(t *testing.T) {
	for _, test := range []struct {
		addr string
		want bool
	}{
		{"fe80::1", true},
		{"febf::1", true}, // The end of fe80::/10
		{"fec0::1", false},
		{"ff01::1", true},
		{"ff02::1", true},
		{"ff12::1", true}, // Transient
		{"ff32::1", true}, // Prefix-based
		{"ff05::1", false},
		{"::1", false},
		{"2001:db8::1", false},
		{"::ffff:169.254.0.1", false},
	} {
		if got := isLocalIP6(netip.MustParseAddr(test.addr)); got != test.want {
			t.Errorf("isLocalIP6(%s) = %v, want %v", test.addr, got, test.want)
		}
	}
}

// TestMarshalParse6Zone ensures the zone of a local source address takes precedence over idx, whether
// it's a name or an index, and that a local destination address is parsed with the name of its interface.
func TestMarshalParse6Zone(t *testing.T) {
	ifs, err := net.Interfaces()
	if err != nil || len(ifs) == 0 {
		t.Skipf("net.Interfaces() = %d, %v", len(ifs), err)
	}
	ifc := ifs[0] // So that it's the first of any names sharing its index

	for _, test := range []struct {
		src, want    string
		idx, wantIdx int
	}{
		{"fe80::1%" + ifc.Name, "fe80::1%" + ifc.Name, 99, ifc.Index},
		{"fe80::1%" + strconv.Itoa(ifc.Index), "fe80::1%" + ifc.Name, 99, ifc.Index},
		{"ff02::1%" + ifc.Name, "ff02::1%" + ifc.Name, 99, ifc.Index},
		{"2001:db8::1%" + ifc.Name, "2001:db8::1", 99, 99}, // Not local, so the zone is ignored
	} {
		oob := make([]byte, BufferSize)
		n := Marshal(oob, false, netip.MustParseAddr(test.src), test.idx)
		if dst, idx, ok := Parse(oob[:n]); dst.String() != test.want || idx != test.wantIdx || !ok {
			t.Errorf("Parse() of Marshal(%s, %d) = %s, %d, %v, want %s, %d, true",
				test.src, test.idx, dst, idx, ok, test.want, test.wantIdx)
		}
	}
}

// TestIsV6Only ensures IPV6_V6ONLY is read as Go sets it on IPv6 sockets: enabled for udp6,
// and disabled for dual-stack udp, except where the system doesn't support IPv4-mapped addresses.
// IPv4 sockets aren't tested, as the option is only defined for IPv6 ones, e.g. Linux fails to get it.
func TestIsV6Only(t *testing.T) {
	noMapping := runtime.GOOS == "dragonfly" || runtime.GOOS == "openbsd" // Every IPv6 socket is IPv6-only
	tests := []struct {
		network, address string
		want             bool
	}{
		{"udp6", "[::1]:0", true},
		{"udp6", "[::]:0", true},
		{"udp", "[::]:0", noMapping},
		{"udp", ":0", noMapping},
	}
	for _, test := range tests {
		t.Run(test.network+","+test.address, func(t *testing.T) {
			pc, err := net.ListenPacket(test.network, test.address)
			if err != nil {
				t.Skipf("ListenPacket() = %v", err) // E.g. no IPv6 support
			}
			defer func() { _ = pc.Close() }()
			if pc.LocalAddr().(*net.UDPAddr).AddrPort().Addr().Is4() {
				t.Skip("an IPv4 socket, e.g. on a host without IPv6")
			}

			raw, err := pc.(*net.UDPConn).SyscallConn()
			if err != nil {
				t.Fatal(err)
			}
			if got, err := IsV6Only(raw); got != test.want || err != nil {
				t.Errorf("IsV6Only() = %v, %v, want %v, nil", got, err, test.want)
			}
		})
	}
}
