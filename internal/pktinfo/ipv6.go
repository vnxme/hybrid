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
	"unsafe"
)

var (
	cMsgIPv6PacketInfoLen     = cMsgHeaderSpace + cMsgIPv6PacketInfoSize
	cMsgIPv6PacketInfoPayload = cMsgHeaderSpace + cMsgAlignOf(cMsgIPv6PacketInfoSize)
)

// marshal6 writes an IPv6 packet info control message into the given OOB buffer,
// and returns the number of bytes written. It panics if the buffer is too short.
func marshal6(oob []byte, src netip.Addr, idx int) int {
	clear(oob[:cMsgIPv6PacketInfoPayload]) // E.g. padding

	hdr := (*cMsgHeader)(unsafe.Pointer(&oob[0]))
	hdr.SetLen(cMsgIPv6PacketInfoLen)
	hdr.Level, hdr.Type = cMsgIPv6PacketInfoLevel, cMsgIPv6PacketInfoType

	info := (*cMsgIPv6PacketInfo)(unsafe.Pointer(&oob[cMsgHeaderSpace]))
	info.Ifindex = uint32(max(0, idx)) //nolint:gosec // G115
	if src.Is6() {
		info.Addr = src.As16()
		if src.Zone() != "" && isLocalIP6(src) { // The zone takes precedence over idx
			info.Ifindex = uint32(zoneCache.index(src.Zone())) //nolint:gosec // G115
		}
	}

	return cMsgIPv6PacketInfoPayload
}

// parse6 returns the destination address and the local interface index from IPv6 packet info,
// which data must be long enough to hold.
func parse6(data []byte) (dst netip.Addr, idx int, ok bool) {
	info := (*cMsgIPv6PacketInfo)(unsafe.Pointer(&data[0]))
	dst = netip.AddrFrom16(info.Addr).Unmap() // An IPv4 packet on a dual-stack socket has a mapped address
	if info.Ifindex > 0 && isLocalIP6(dst) {
		dst = dst.WithZone(zoneCache.name(int(info.Ifindex)))
	}
	return dst, int(info.Ifindex), true
}

// isLocalIP6 reports whether x is a link-local unicast,
// interface-local multicast or link-local multicast IPv6 address.
//
// Note: x is assumed to be a valid IPv6 address, i.e. no extra checks are done.
func isLocalIP6(x netip.Addr) bool {
	a16 := x.As16()
	s := uint16(a16[0])<<8 | uint16(a16[1])
	if s&0xffc0 == 0xfe80 {
		return true
	}
	s = s & 0xff0f
	return s == 0xff01 || s == 0xff02
}
