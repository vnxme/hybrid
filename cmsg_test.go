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
	"net"
	"net/netip"
	"runtime"
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

func TestMarshalParsePacketInfo(t *testing.T) {
	rd, wr4, wr6 := makeBuffersForPacketInfo(false, false)
	if len(rd) == 0 || len(wr4) == 0 || len(wr6) == 0 {
		t.Error("not implemented on " + runtime.GOOS + "/" + runtime.GOARCH)
	}

	var addr netip.Addr
	var idx int
	var n int
	var ok bool
	var oob []byte
	for _, test := range []struct {
		addr netip.Addr
		idx  int
		v6   bool
	}{
		{netip.Addr{}, 0, false},
		{netip.Addr{}, 1, false},
		{netip.Addr{}, 2 ^ 31 - 1, false},
		{netip.IPv4Unspecified(), 0, false},
		{netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0, false},
		{netip.Addr{}, 0, true},
		{netip.Addr{}, 1, true},
		{netip.Addr{}, 2 ^ 31 - 1, true},
		{netip.IPv6Unspecified(), 0, true},
		{netip.AddrFrom16([16]byte{15: 1}), 0, true},
	} {
		if test.v6 {
			n, ok = marshalPacketInfo6(wr6, test.addr, test.idx, false)
			if !ok {
				t.Errorf("marshalPacketInfo6(%v, %v, %v) failed", wr6, test.addr, test.idx)
			}
			oob = wr6[:n]
		} else {
			n, ok = marshalPacketInfo4(wr4, test.addr, test.idx, false)
			if !ok {
				t.Errorf("marshalPacketInfo4(%v, %v, %v) failed", wr4, test.addr, test.idx)
			}
			oob = wr4[:n]
		}

		addr, idx, ok = parsePacketInfo(oob)
		switch {
		case !ok:
			t.Errorf("parsePacketInfo(%v) failed", oob)
		case test.addr.IsValid() && addr != test.addr:
			t.Errorf("parsePacketInfo(%v) returned %v; want %v", oob, addr, test.addr)
		case !test.addr.IsValid() && !test.v6 && addr != netip.IPv4Unspecified():
			t.Errorf("parsePacketInfo(%v) returned %v; want %v", oob, addr, netip.IPv4Unspecified())
		case !test.addr.IsValid() && test.v6 && addr != netip.IPv6Unspecified():
			t.Errorf("parsePacketInfo(%v) returned %v; want %v", oob, addr, netip.IPv6Unspecified())
		case idx != test.idx:
			t.Errorf("parsePacketInfo(%v) returned %v; want %v", oob, idx, test.idx)
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
