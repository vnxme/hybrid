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
	"bytes"
	"net/netip"
	"testing"
)

// FuzzParse ensures Parse reads any OOB buffer without panicking or changing it, and reports nothing unless
// it has found packet info. The seeds are packet info of either IP version and messages it should skip.
func FuzzParse(f *testing.F) {
	for _, v4 := range []bool{true, false} {
		oob := make([]byte, BufferSize)
		f.Add(oob[:Marshal(oob, v4, netip.MustParseAddr("192.0.2.1"), 1)])
	}
	oob := make([]byte, BufferSize)
	f.Add(oob[:Marshal(oob, false, netip.MustParseAddr("fe80::1"), 1)])
	f.Add(appendCMsg(appendCMsg(nil, 0, 0, []byte{1, 2, 3}), cMsgIPv6PacketInfoLevel, cMsgIPv6PacketInfoType, nil))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		// A real OOB buffer is aligned for the headers, as a fuzzed one may not be, if it's tiny
		oob := make([]byte, max(len(data), 16))[:len(data)]
		copy(oob, data)

		dst, idx, ok := Parse(oob)
		if !ok && (dst.IsValid() || idx != 0) {
			t.Errorf("Parse(% x) = %v, %d, false, want nothing", data, dst, idx)
		}
		if !bytes.Equal(oob, data) {
			t.Errorf("Parse(% x) changed the buffer to % x", data, oob)
		}
	})
}

// FuzzMarshal ensures Marshal writes packet info of either IP version within BufferSize bytes, and for IPv6,
// whose packet info has the same layout for sending and receiving, that Parse reads the source address
// back unmapped, and the interface index, as TestMarshalParse6 does for a few of them.
func FuzzMarshal(f *testing.F) {
	f.Add(netip.MustParseAddr("192.0.2.1").AsSlice(), int32(1))
	f.Add(netip.MustParseAddr("::ffff:192.0.2.1").AsSlice(), int32(0))
	f.Add(netip.MustParseAddr("2001:db8::1").AsSlice(), int32(-1))
	f.Add(netip.MustParseAddr("fe80::1").AsSlice(), int32(1))
	f.Add([]byte{}, int32(7))
	f.Fuzz(func(t *testing.T, ip []byte, idx int32) {
		src, _ := netip.AddrFromSlice(ip) // Invalid unless of 4 or 16 bytes, i.e. left to the system
		for _, v4 := range []bool{true, false} {
			oob := bytes.Repeat([]byte{0xff}, BufferSize+1)
			n := Marshal(oob, v4, src, int(idx))
			if n <= 0 || n > BufferSize || oob[n] != 0xff {
				t.Fatalf("Marshal(v4 %t, %v, %d) = %d, want up to %d bytes written", v4, src, idx, n, BufferSize)
			}
			if v4 {
				continue
			}

			want := netip.IPv6Unspecified() // Unless src is an IPv6 address, mapped or not
			if src.Is6() {
				want = src.Unmap()
			}
			dst, gotIdx, ok := Parse(oob[:n])
			if !ok || dst.WithZone("") != want || gotIdx != max(0, int(idx)) {
				t.Errorf("Parse() of Marshal(%v, %d) = %v, %d, %t, want %v, %d, true", src, idx, dst, gotIdx, ok,
					want, max(0, int(idx)))
			}
		}
	})
}
