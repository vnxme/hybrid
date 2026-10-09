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

package hybrid

import (
	"math"
	"net"
	"net/netip"
	"syscall"
	"testing"
)

func BenchmarkControlMessageAlignOf(b *testing.B) {
	b.Run("Decent", func(b *testing.B) {
		f := cMsgAlignOf
		for i := 0; i < b.N; i++ {
			_ = f(i)
		}
	})

	// This alternative is slightly faster (by 0.01-0.05 ns/op)
	b.Run("Faster", func(b *testing.B) {
		x := cMsgAlign - 1
		y := ^x
		f := func(n int) int {
			return (n + x) & y
		}
		for i := 0; i < b.N; i++ {
			_ = f(i)
		}
	})
}

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

// TestMarshalParsePacketInfo ensures the marshallers report the size they need, and that IPv6 packet info,
// which has the same layout for sending and receiving, is parsed as it has been marshalled. IPv4 packet info
// isn't parsed, as it has different meanings or even types for sending and receiving, e.g. on Linux,
// in_pktinfo carries the source address in ipi_spec_dst for sending, but the destination one in ipi_addr
// for receiving, while BSD sends IP_SENDSRCADDR with no interface index. Real IPv4 control messages are
// covered by TestControlUDPPacketInfo and TestPacketInfoUDPConnInterfaces.
func TestMarshalParsePacketInfo(t *testing.T) {
	for _, marshal := range []struct {
		name string
		f    func(oob []byte, src netip.Addr, idx int, full bool) (int, bool)
	}{
		{"marshalPacketInfo4", marshalPacketInfo4},
		{"marshalPacketInfo6", marshalPacketInfo6},
	} {
		n, ok := marshal.f(make([]byte, cMsgBufferSize), netip.Addr{}, 0, true)
		if n <= 0 || n > cMsgBufferSize || !ok {
			t.Errorf("%s() into %d bytes = %d, %v, want up to %d, true", marshal.name, cMsgBufferSize, n, ok, cMsgBufferSize)
		}
		if got, ok := marshal.f(make([]byte, n-1), netip.Addr{}, 0, true); got != n || ok {
			t.Errorf("%s() into %d bytes = %d, %v, want %d, false", marshal.name, n-1, got, ok, n)
		}
	}

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
	} {
		oob := make([]byte, cMsgBufferSize)
		for i := range oob {
			oob[i] = 0xff // Any byte left unwritten is noticed
		}
		n, _ := marshalPacketInfo6(oob, test.src, test.idx, true)
		if dst, idx, ok := parsePacketInfo(oob[:n]); dst != test.want || idx != test.idx || !ok {
			t.Errorf("parsePacketInfo() of marshalPacketInfo6(%s, %d) = %s, %d, %v, want %s, %d, true",
				test.src, test.idx, dst, idx, ok, test.want, test.idx)
		}
	}
}

func TestRawConnIsV6Only(t *testing.T) {
	var err error
	var ok bool
	var pc net.PacketConn
	var raw syscall.RawConn
	for _, test := range []struct {
		net  string
		only bool
	}{
		{net: "udp", only: false},
		{net: "udp4", only: true},
		{net: "udp6", only: true},
	} {
		pc, err = net.ListenPacket(test.net, ":0")
		if err != nil {
			t.Error(err)
			continue
		}

		raw, err = pc.(*net.UDPConn).SyscallConn()
		if err != nil {
			t.Error(err)
			_ = pc.Close()
			continue
		}

		ok, err = rawConnIsV6Only(raw)
		if err != nil {
			t.Error(err)
			_ = pc.Close()
			continue
		}

		if ok != test.only {
			t.Errorf("rawConnIsV6Only() returned %v; want %v", ok, test.only)
		}

		err = pc.Close()
		if err != nil {
			t.Error(err)
		}
	}
}
