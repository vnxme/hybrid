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

// This file implements IPv4 packet info with IP_PKTINFO, which carries both the address and the interface index.

//go:build darwin || illumos || linux || solaris || windows || zos

package pktinfo

import (
	"net/netip"
	"syscall"
	"unsafe"
)

var (
	BufferSize = max(128, cMsgIPv4PacketInfoPayload, cMsgIPv6PacketInfoPayload)

	cMsgIPv4PacketInfoLen     = cMsgHeaderSpace + cMsgIPv4PacketInfoSize
	cMsgIPv4PacketInfoPayload = cMsgHeaderSpace + cMsgAlignOf(cMsgIPv4PacketInfoSize)
)

// Marshal4 writes an IPv4 packet info control message into the given OOB buffer.
// It returns the number of bytes written/required, and true if the buffer has been large enough.
func Marshal4(oob []byte, src netip.Addr, idx int) (int, bool) {
	if len(oob) < cMsgIPv4PacketInfoPayload {
		return cMsgIPv4PacketInfoPayload, false
	}

	hdr := (*cMsgHeader)(unsafe.Pointer(&oob[0]))
	hdr.SetLen(cMsgIPv4PacketInfoLen)
	hdr.Level, hdr.Type = cMsgIPv4PacketInfoLevel, cMsgIPv4PacketInfoType

	var src4 [4]byte // Unspecified unless src is an IPv4 address
	if src.Is4() {
		src4 = src.As4()
	}
	setPacketInfo4((*cMsgIPv4PacketInfo)(unsafe.Pointer(&oob[cMsgHeaderSpace])), src4, max(0, idx))

	return cMsgIPv4PacketInfoPayload, true
}

// Parse searches a packet info control message in the given OOB buffer.
// It returns a destination address, a local interface index, and true if the message has been found.
// It skips a message too short for packet info, e.g. as truncation has shortened it.
func Parse(oob []byte) (dst netip.Addr, idx int, ok bool) {
	for hdr, data := range cMsgs(oob) {
		switch {
		case hdr.Level == cMsgIPv4PacketInfoLevel && hdr.Type == cMsgIPv4PacketInfoType && len(data) >= cMsgIPv4PacketInfoSize:
			info := (*cMsgIPv4PacketInfo)(unsafe.Pointer(&data[0]))
			return netip.AddrFrom4(info.Addr), int(info.Ifindex), true
		case hdr.Level == cMsgIPv6PacketInfoLevel && hdr.Type == cMsgIPv6PacketInfoType && len(data) >= cMsgIPv6PacketInfoSize:
			return parse6(data)
		}
	}

	return
}

// Enable enables IPv4 and/or IPv6 packet info for the given raw network connection.
func Enable(c syscall.RawConn, v4Only, v6Only bool) error {
	var err4 error
	if !v6Only {
		err4 = rawConnEnableSockOpt(c, cMsgIPv4PacketInfoLevel, cMsgIPv4PacketInfoOpt)
		if err4 != nil && v4Only {
			return err4
		}
	}

	var err6 error
	if !v4Only {
		err6 = rawConnEnableSockOpt(c, cMsgIPv6PacketInfoLevel, cMsgIPv6PacketInfoOpt)
		if err6 != nil && v6Only {
			return err6
		}
	}

	if err4 != nil && err6 != nil {
		return err4
	}

	return nil
}
