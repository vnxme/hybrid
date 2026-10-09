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

//go:build darwin || illumos || linux || solaris || windows || zos

package pktinfo

import (
	"net/netip"
	"testing"
)

// enableCalls4 are the calls Enable makes for IPv4 packet info: IP_PKTINFO alone.
var enableCalls4 = []int{0}

// TestParse4 ensures IPv4 packet info is parsed as received, i.e. with the destination address
// in ipi_addr, also after an unrelated message and after one too short for packet info.
func TestParse4(t *testing.T) {
	var info cMsgIPv4PacketInfo
	info.Addr, info.Ifindex = [4]byte{10, 1, 2, 3}, 7
	data := bytesOf(&info)
	message := func(b []byte, data []byte) []byte {
		return appendCMsg(b, cMsgIPv4PacketInfoLevel, cMsgIPv4PacketInfoType, data)
	}

	tests := []struct {
		name string
		oob  []byte
	}{
		{"Alone", message(nil, data)},
		{"AfterUnrelated", message(appendCMsg(nil, -1, -1, []byte{1, 2, 3}), data)},
		{"AfterTooShort", message(message(nil, data[:len(data)-1]), data)},
	}
	want := netip.MustParseAddr("10.1.2.3")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if dst, idx, ok := Parse(test.oob); dst != want || idx != 7 || !ok {
				t.Errorf("Parse() = %s, %d, %v, want %s, 7, true", dst, idx, ok, want)
			}
		})
	}
}
