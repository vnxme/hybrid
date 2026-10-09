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
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
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

// TestMarshalSize ensures Marshal fits in BufferSize bytes and clears the bytes it skips, e.g. padding,
// and that it panics before writing anything into a buffer too short for the message.
func TestMarshalSize(t *testing.T) {
	for _, v4 := range []bool{true, false} {
		oob := bytes.Repeat([]byte{0xff}, BufferSize) // Any byte left as is is noticed
		n := Marshal(oob, v4, netip.Addr{}, 0)
		if n <= 0 || n > BufferSize {
			t.Fatalf("Marshal(v4 %v) into %d bytes = %d, want up to %d", v4, BufferSize, n, BufferSize)
		}
		if i := bytes.IndexByte(oob[:n], 0xff); i >= 0 {
			t.Errorf("Marshal(v4 %v) = % x, want byte %d cleared", v4, oob[:n], i)
		}

		short := bytes.Repeat([]byte{0xff}, n-1)
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("Marshal(v4 %v) into %d bytes didn't panic", v4, n-1)
				}
			}()
			Marshal(short, v4, netip.Addr{}, 0)
		}()
		if !bytes.Equal(short, bytes.Repeat([]byte{0xff}, n-1)) {
			t.Errorf("Marshal(v4 %v) into %d bytes wrote % x before panicking", v4, n-1, short)
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
		Marshal(oob, false, ip, 7)
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

// appendCMsg appends a control message with the given level, type and data to b, padded for alignment.
func appendCMsg(b []byte, level, typ int32, data []byte) []byte {
	off := len(b)
	b = append(b, make([]byte, cMsgAlignOf(cMsgHeaderSpace+len(data)))...)
	hdr := (*cMsgHeader)(unsafe.Pointer(&b[off]))
	hdr.SetLen(cMsgHeaderSpace + len(data))
	hdr.Level, hdr.Type = level, typ
	copy(b[off+cMsgHeaderSpace:], data)
	return b
}

// bytesOf returns the memory of v as a byte slice.
func bytesOf[T any](v *T) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), unsafe.Sizeof(*v))
}

// TestMarshal4 ensures an IPv4 source address is written into the message, whichever field it takes.
func TestMarshal4(t *testing.T) {
	src := netip.MustParseAddr("10.1.2.3")
	oob := make([]byte, BufferSize)
	n := Marshal(oob, true, src, 0)
	if want := src.As4(); !bytes.Contains(oob[cMsgHeaderSpace:n], want[:]) {
		t.Errorf("Marshal(%s) = % x, want % x in the data", src, oob[:n], want)
	}
}

// failingRawConn is a syscall.RawConn whose Control fails for the given calls, counted from 0,
// and does nothing for the others, as if a socket option has been set.
type failingRawConn struct {
	syscall.RawConn
	calls int
	fail  []int
}

func (c *failingRawConn) Control(func(fd uintptr)) error {
	defer func() { c.calls++ }()
	if slices.Contains(c.fail, c.calls) {
		return fmt.Errorf("call %d failed", c.calls)
	}
	return nil
}

// TestEnableErrors ensures IPv4 packet info fails if any of its options fails, i.e. the calls
// in enableCalls4, and that a dual-stack socket fails only if IPv6 packet info fails too.
func TestEnableErrors(t *testing.T) {
	type test struct {
		name           string
		v4Only, v6Only bool
		fail           []int
		wantErr        bool
	}
	call6 := len(enableCalls4) // IPv6 packet info is enabled after IPv4 packet info
	tests := []test{
		{"DualStack", false, false, nil, false},
		{"DualStackNoIPv4", false, false, enableCalls4, false},
		{"DualStackNoIPv6", false, false, []int{call6}, false},
		{"IPv4Only", true, false, nil, false},
		{"IPv6Only", false, true, nil, false},
		{"IPv6OnlyNoIPv6", false, true, []int{0}, true}, // No IPv4 calls on an IPv6-only socket
	}
	for _, call := range enableCalls4 { // Each IPv4 option failing alone fails IPv4 packet info
		tests = append(tests,
			test{fmt.Sprintf("IPv4OnlyNoOption%d", call), true, false, []int{call}, true},
			test{fmt.Sprintf("DualStackNoIPv6NoOption%d", call), false, false, []int{call, call6}, true},
		)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := Enable(&failingRawConn{fail: test.fail}, test.v4Only, test.v6Only); (err != nil) != test.wantErr {
				t.Errorf("Enable() = %v, want an error %v", err, test.wantErr)
			}
		})
	}

	t.Run("DualStackNeither", func(t *testing.T) { // Both errors are reported
		err := Enable(&failingRawConn{fail: append(slices.Clone(enableCalls4), call6)}, false, false)
		for _, call := range []int{0, call6} {
			if want := fmt.Sprintf("call %d failed", call); err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("Enable() = %v, want an error with %q", err, want)
			}
		}
	})

	t.Run("BothOnly", func(t *testing.T) { // Neither IP version, so no options are set
		c := &failingRawConn{}
		if err := Enable(c, true, true); err == nil || c.calls != 0 {
			t.Errorf("Enable(true, true) = %v after %d calls, want an error after none", err, c.calls)
		}
	})

	t.Run("IsV6Only", func(t *testing.T) {
		if _, err := IsV6Only(&failingRawConn{fail: []int{0}}); err == nil {
			t.Errorf("IsV6Only() = _, nil, want an error")
		}
	})
}

// TestEnable ensures a packet sent to a socket with packet info enabled is parsed
// with its destination address and a local interface index.
func TestEnable(t *testing.T) {
	lo4, lo6 := netip.MustParseAddr("127.0.0.1"), netip.IPv6Loopback()
	tests := []struct {
		network, address string
		v4Only, v6Only   bool
		dsts             []netip.Addr
	}{
		{"udp4", "127.0.0.1:0", true, false, []netip.Addr{lo4}},
		{"udp6", "[::1]:0", false, true, []netip.Addr{lo6}},
		{"udp", "[::]:0", false, false, []netip.Addr{lo4, lo6}},
	}
	for _, test := range tests {
		t.Run(test.network+","+test.address, func(t *testing.T) {
			pc, err := net.ListenPacket(test.network, test.address)
			if err != nil {
				t.Skipf("ListenPacket() = %v", err) // E.g. no IPv6 support
			}
			defer func() { _ = pc.Close() }()
			conn := pc.(*net.UDPConn)
			raw, err := conn.SyscallConn()
			if err != nil {
				t.Fatal(err)
			}
			v6Only := test.v6Only
			if !test.v4Only && !v6Only { // E.g. no IPv4-mapped addresses on some systems
				if v6Only, err = IsV6Only(raw); err != nil {
					t.Fatal(err)
				}
			}
			if err = Enable(raw, test.v4Only, v6Only); err != nil {
				t.Fatalf("Enable() = %v, want nil", err)
			}

			port := conn.LocalAddr().(*net.UDPAddr).AddrPort().Port()
			for _, dst := range test.dsts {
				if dst.Is4() && v6Only {
					continue
				}
				client, err := net.DialUDP("udp", nil, net.UDPAddrFromAddrPort(netip.AddrPortFrom(dst, port)))
				if err != nil {
					t.Fatal(err)
				}
				_, err = client.Write([]byte{1})
				_ = client.Close()
				if err != nil {
					t.Fatal(err)
				}

				_ = conn.SetReadDeadline(time.Now().Add(time.Second))
				oob := make([]byte, BufferSize)
				_, oobN, _, _, err := conn.ReadMsgUDPAddrPort(make([]byte, 1), oob)
				if err != nil {
					t.Fatalf("ReadMsgUDPAddrPort() = %v", err)
				}
				if got, idx, ok := Parse(oob[:oobN]); got != dst || idx <= 0 || !ok {
					t.Errorf("Parse() of a packet to %s = %s, %d, %v, want %s, a positive index, true", dst, got, idx, ok, dst)
				}
			}
		})
	}
}
