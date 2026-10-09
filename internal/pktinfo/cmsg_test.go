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
	"net/netip"
	"reflect"
	"testing"
	"unsafe"
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

// TestMarshalSize ensures the marshallers report the size they need.
func TestMarshalSize(t *testing.T) {
	for _, marshal := range []struct {
		name string
		f    func(oob []byte, src netip.Addr, idx int) (int, bool)
	}{
		{"Marshal4", Marshal4},
		{"Marshal6", Marshal6},
	} {
		n, ok := marshal.f(make([]byte, BufferSize), netip.Addr{}, 0)
		if n <= 0 || n > BufferSize || !ok {
			t.Errorf("%s() into %d bytes = %d, %v, want up to %d, true", marshal.name, BufferSize, n, ok, BufferSize)
		}
		if got, ok := marshal.f(make([]byte, n-1), netip.Addr{}, 0); got != n || ok {
			t.Errorf("%s() into %d bytes = %d, %v, want %d, false", marshal.name, n-1, got, ok, n)
		}
	}
}

// TestParseBounds ensures packet info is parsed only from within a message and the buffer,
// including the last message without padding, and that a message too short for packet info is skipped.
func TestParseBounds(t *testing.T) {
	ip := netip.MustParseAddr("2001:db8::1")
	msgLen := cMsgHeaderSpace + cMsgIPv6PacketInfoSize // Unpadded

	// message returns an IPv6 packet info message for ip and index 7 in a buffer of the given size,
	// with its length field set to n
	message := func(size, n int) []byte {
		oob := make([]byte, max(size, cMsgIPv6PacketInfoPayload))
		Marshal6(oob, ip, 7)
		reflect.ValueOf(&(*cMsgHeader)(unsafe.Pointer(&oob[0])).Len).Elem().SetUint(uint64(n))
		return oob[:size]
	}

	tests := []struct {
		name string
		oob  []byte
		ok   bool
	}{
		{"Empty", nil, false},
		{"HeaderOnly", message(cMsgHeaderSpace, cMsgHeaderSpace), false},
		{"Whole", message(cMsgIPv6PacketInfoPayload, msgLen), true},
		{"Unpadded", message(msgLen, msgLen), true},
		{"Truncated", message(cMsgAlignOf(cMsgHeaderSpace+4), cMsgHeaderSpace+4), false},
		{"LongerThanBuffer", message(msgLen-1, msgLen), false},
		{"ZeroLength", message(cMsgIPv6PacketInfoPayload, 0), false},
		{"AfterTooShort", append(message(cMsgAlignOf(cMsgHeaderSpace+1), cMsgHeaderSpace+1),
			message(cMsgIPv6PacketInfoPayload, msgLen)...), true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dst, idx, ok := Parse(test.oob)
			if ok != test.ok || ok && (dst != ip || idx != 7) {
				t.Errorf("Parse() = %s, %d, %v, want ok %v", dst, idx, ok, test.ok)
			}
		})
	}
}
