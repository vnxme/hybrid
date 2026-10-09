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

//go:build aix || dragonfly || freebsd || netbsd || openbsd

package pktinfo

import (
	"net/netip"
	"testing"
)

// enableCalls4 are the calls Enable makes for IPv4 packet info: IP_RECVDSTADDR, then IP_RECVIF.
var enableCalls4 = []int{0, 1}

// TestParse4 ensures IPv4 packet info is parsed from the destination address and the link-level
// address messages, in any order, and from a link-level address shorter than cMsgIPv4Interface,
// as it is for an interface without a name or a hardware address.
func TestParse4(t *testing.T) {
	addr := []byte{10, 1, 2, 3}
	var link cMsgIPv4Interface
	link.Index = 7
	full := bytesOf(&link)
	short := full[:cMsgIPv4InterfaceIndexEnd]
	dstMsg := func(b []byte) []byte {
		return appendCMsg(b, cMsgIPv4DestAddrLevel, cMsgIPv4DestAddrType, addr)
	}
	linkMsg := func(b []byte, data []byte) []byte {
		return appendCMsg(b, cMsgIPv4InterfaceLevel, cMsgIPv4InterfaceType, data)
	}

	want := netip.MustParseAddr("10.1.2.3")
	tests := []struct {
		name string
		oob  []byte
		dst  netip.Addr
		idx  int
	}{
		{"Both", linkMsg(dstMsg(nil), full), want, 7},
		{"BothShortLink", linkMsg(dstMsg(nil), short), want, 7},
		{"Reversed", dstMsg(linkMsg(nil, short)), want, 7},
		{"AddressOnly", dstMsg(nil), want, 0},
		{"LinkOnly", linkMsg(nil, short), netip.Addr{}, 7},
		{"LinkTooShort", linkMsg(dstMsg(nil), short[:len(short)-1]), want, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if dst, idx, ok := Parse(test.oob); dst != test.dst || idx != test.idx || !ok {
				t.Errorf("Parse() = %s, %d, %v, want %s, %d, true", dst, idx, ok, test.dst, test.idx)
			}
		})
	}
}
