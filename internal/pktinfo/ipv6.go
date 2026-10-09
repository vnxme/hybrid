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
	"net"
	"net/netip"
	"unsafe"
)

var (
	cMsgIPv6PacketInfoLen     = cMsgHeaderSpace + cMsgIPv6PacketInfoSize
	cMsgIPv6PacketInfoPayload = cMsgHeaderSpace + cMsgAlignOf(cMsgIPv6PacketInfoSize)
)

// Marshal6 writes an IPv6 packet info control message into the given OOB buffer.
// It returns the number of bytes written/required, and true if the buffer has been large enough.
func Marshal6(oob []byte, src netip.Addr, idx int) (int, bool) {
	if len(oob) < cMsgIPv6PacketInfoPayload {
		return cMsgIPv6PacketInfoPayload, false
	}

	hdr := (*cMsgHeader)(unsafe.Pointer(&oob[0]))
	hdr.SetLen(cMsgIPv6PacketInfoLen)
	hdr.Level, hdr.Type = cMsgIPv6PacketInfoLevel, cMsgIPv6PacketInfoType

	info := (*cMsgIPv6PacketInfo)(unsafe.Pointer(&oob[cMsgHeaderSpace]))
	if src.Is6() {
		copy(info.Addr[:], src.AsSlice())
		if len(src.Zone()) > 0 && isLocalIP6(src) {
			info.Ifindex = uint32(zoneCache.index(src.Zone())) //nolint:gosec // G115
		} else {
			info.Ifindex = uint32(max(0, idx)) //nolint:gosec // G115
		}
	} else {
		copy(info.Addr[:], net.IPv6zero)
		info.Ifindex = uint32(max(0, idx)) //nolint:gosec // G115
	}

	return cMsgIPv6PacketInfoPayload, true
}

// parse6 returns the destination address and the local interface index from IPv6 packet info,
// which data must be long enough to hold.
func parse6(data []byte) (dst netip.Addr, idx int, ok bool) {
	info := (*cMsgIPv6PacketInfo)(unsafe.Pointer(&data[0]))
	dst = netip.AddrFrom16(info.Addr)
	if info.Ifindex > 0 && isLocalIP6(dst) {
		dst = dst.WithZone(zoneCache.name(int(info.Ifindex)))
	}
	return dst, int(info.Ifindex), true
}

// isLocalIP6 checks if the given netip.Addr is a link-local unicast,
// interface-local multicast or link-local multicast IPv6 address.
//
// Note: x is assumed to be a valid IPv6, i.e. no extra checks are done.
func isLocalIP6(x netip.Addr) bool {
	a16 := x.As16()
	s := uint16(a16[0])<<8 | uint16(a16[1])
	if s&0xffc0 == 0xfe80 {
		return true
	}
	s = s & 0xff0f
	return s == 0xff01 || s == 0xff02
}
