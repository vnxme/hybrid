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

// This file implements IPv4 packet info with IP_RECVDSTADDR and a separate message with the interface,
// for systems without IP_PKTINFO. For sending, IP_SENDSRCADDR carries no interface index.

//go:build aix || dragonfly || freebsd || netbsd || openbsd

package pktinfo

import (
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	cMsgIPv4DestAddrLevel  = unix.IPPROTO_IP
	cMsgIPv4InterfaceLevel = unix.IPPROTO_IP

	cMsgIPv4DestAddrOpt = unix.IP_RECVDSTADDR // It also equals unix.IP_SENDSRCADDR

	cMsgIPv4DestAddrSize         = net.IPv4len
	cMsgIPv4InterfaceIndexOffset = int(unsafe.Offsetof(cMsgIPv4Interface{}.Index)) // Offset of the index in a link-level address
	cMsgIPv4InterfaceIndexEnd    = cMsgIPv4InterfaceIndexOffset + int(unsafe.Sizeof(cMsgIPv4Interface{}.Index))

	cMsgIPv4DestAddrType  = cMsgIPv4DestAddrOpt
	cMsgIPv4InterfaceType = cMsgIPv4InterfaceOpt
)

var (
	// BufferSize is the size of an OOB buffer that holds packet info of either IP version.
	BufferSize = max(128, cMsgIPv4DestAddrPayload+cMsgIPv4InterfacePayload, cMsgIPv6PacketInfoPayload)

	cMsgIPv4DestAddrLen      = cMsgHeaderSpace + cMsgIPv4DestAddrSize
	cMsgIPv4DestAddrPayload  = cMsgHeaderSpace + cMsgAlignOf(cMsgIPv4DestAddrSize)
	cMsgIPv4InterfacePayload = cMsgHeaderSpace + cMsgAlignOf(cMsgIPv4InterfaceSize)
)

// marshal4 writes an IPv4 source address control message into the given OOB buffer,
// and returns the number of bytes written. It panics if the buffer is too short.
func marshal4(oob []byte, src netip.Addr, _ int) int {
	clear(oob[:cMsgIPv4DestAddrPayload]) // E.g. padding

	hdr := (*cMsgHeader)(unsafe.Pointer(&oob[0]))
	hdr.SetLen(cMsgIPv4DestAddrLen)
	hdr.Level, hdr.Type = cMsgIPv4DestAddrLevel, cMsgIPv4DestAddrType

	if src.Is4() {
		*(*[cMsgIPv4DestAddrSize]byte)(oob[cMsgHeaderSpace:]) = src.As4()
	}

	return cMsgIPv4DestAddrPayload
}

// Parse searches packet info control messages in the given OOB buffer.
// It returns a destination address, a local interface index, and true if a message has been found.
// It skips a message too short for packet info, e.g. as truncation has shortened it.
// An IPv4 destination address comes unmapped, even in IPv6 packet info on a dual-stack socket.
func Parse(oob []byte) (dst netip.Addr, idx int, ok bool) {
	for hdr, data := range cMsgs(oob) {
		switch {
		case hdr.Level == cMsgIPv4DestAddrLevel && hdr.Type == cMsgIPv4DestAddrType && len(data) >= cMsgIPv4DestAddrSize:
			dst, ok = netip.AddrFrom4([cMsgIPv4DestAddrSize]byte(data)), true
		case hdr.Level == cMsgIPv4InterfaceLevel && hdr.Type == cMsgIPv4InterfaceType && len(data) >= cMsgIPv4InterfaceIndexEnd:
			// The link-level address may be shorter than cMsgIPv4Interface, so read the index only
			idx, ok = int(binary.NativeEndian.Uint16(data[cMsgIPv4InterfaceIndexOffset:])), true
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
		err4 = errors.Join( // Either option alone leaves IPv4 packet info incomplete
			rawConnEnableSockOpt(c, cMsgIPv4DestAddrLevel, cMsgIPv4DestAddrOpt),
			rawConnEnableSockOpt(c, cMsgIPv4InterfaceLevel, cMsgIPv4InterfaceOpt),
		)
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
