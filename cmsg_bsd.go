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

//go:build dragonfly || freebsd || netbsd || openbsd

package hybrid

import (
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

type (
	cMsgHeader = unix.Cmsghdr

	cMsgIPv4DestAddr  = [cMsgIPv4DestAddrSize]byte
	cMsgIPv4Interface = unix.SockaddrDatalink
	// cMsgIPv4PacketInfo =
	cMsgIPv6PacketInfo = unix.Inet6Pktinfo
)

const (
	cMsgHeaderSize = unix.SizeofCmsghdr

	cMsgIPv4DestAddrLevel  = unix.IPPROTO_IP
	cMsgIPv4InterfaceLevel = unix.IPPROTO_IP
	// cMsgIPv4PacketInfoLevel =
	cMsgIPv6PacketInfoLevel = unix.IPPROTO_IPV6

	cMsgIPv4DestAddrOpt  = unix.IP_RECVDSTADDR // It also equals unix.IP_SENDSRCADDR
	cMsgIPv4InterfaceOpt = unix.IP_RECVIF
	// cMsgIPv4PacketInfoOpt =
	cMsgIPv6PacketInfoOpt = unix.IPV6_RECVPKTINFO

	cMsgIPv4DestAddrSize         = net.IPv4len
	cMsgIPv4InterfaceSize        = unix.SizeofSockaddrDatalink
	cMsgIPv4InterfaceIndexOffset = int(unsafe.Offsetof(cMsgIPv4Interface{}.Index)) // Offset of the index in a link-level address
	cMsgIPv4InterfaceIndexEnd    = cMsgIPv4InterfaceIndexOffset + int(unsafe.Sizeof(cMsgIPv4Interface{}.Index))
	// cMsgIPv4PacketInfoSize =
	cMsgIPv6PacketInfoSize = unix.SizeofInet6Pktinfo

	cMsgIPv4DestAddrType  = cMsgIPv4DestAddrOpt
	cMsgIPv4InterfaceType = cMsgIPv4InterfaceOpt
	// cMsgIPv4PacketInfoType =
	cMsgIPv6PacketInfoType = unix.IPV6_PKTINFO

	cMsgTruncated = unix.MSG_CTRUNC // Set in the flags of a read whose control messages are truncated
)

var (
	cMsgAlign = unix.CmsgSpace(1) - cMsgHeaderSpace

	cMsgBufferSize = max(128, cMsgIPv4DestAddrPayload+cMsgIPv4InterfacePayload, cMsgIPv6PacketInfoPayload)

	cMsgHeaderSpace = unix.CmsgLen(0)

	cMsgIPv4DestAddrLen  = cMsgHeaderSpace + cMsgIPv4DestAddrSize
	cMsgIPv4InterfaceLen = cMsgHeaderSpace + cMsgIPv4InterfaceSize
	// cMsgIPv4PacketInfoLen =
	cMsgIPv6PacketInfoLen = cMsgHeaderSpace + cMsgIPv6PacketInfoSize

	cMsgIPv4DestAddrSpace  = cMsgAlignOf(cMsgIPv4DestAddrSize)
	cMsgIPv4InterfaceSpace = cMsgAlignOf(cMsgIPv4InterfaceSize)
	// cMsgIPv4PacketInfoSpace = cMsgAlignOf(cMsgIPv4PacketInfoSize)
	cMsgIPv6PacketInfoSpace = cMsgAlignOf(cMsgIPv6PacketInfoSize)

	cMsgIPv4DestAddrPayload  = cMsgHeaderSpace + cMsgIPv4DestAddrSpace
	cMsgIPv4InterfacePayload = cMsgHeaderSpace + cMsgIPv4InterfaceSpace
	// cMsgIPv4PacketInfoPayload =
	cMsgIPv6PacketInfoPayload = cMsgHeaderSpace + cMsgIPv6PacketInfoSpace
)

// marshalPacketInfo4 writes a platform-specific IPv4 packet info control message into the given OOB buffer.
// It returns the number of bytes written/required, and true if the buffer has been large enough.
func marshalPacketInfo4(oob []byte, src netip.Addr, idx int) (int, bool) {
	if len(oob) < cMsgIPv4DestAddrPayload {
		return cMsgIPv4DestAddrPayload, false
	}

	hdr := (*cMsgHeader)(unsafe.Pointer(&oob[0]))
	hdr.SetLen(cMsgIPv4DestAddrLen)
	hdr.Level, hdr.Type = cMsgIPv4DestAddrLevel, cMsgIPv4DestAddrType

	info := (*cMsgIPv4DestAddr)(unsafe.Pointer(&oob[cMsgHeaderSpace]))
	if src.Is4() {
		copy(info[:], src.AsSlice())
	} else {
		copy(info[:], net.IPv4zero)
	}
	// Ignore the local interface index for IPv4

	return cMsgIPv4DestAddrPayload, true
}

// marshalPacketInfo6 write a platform-specific IPv6 packet info control message into the given OOB buffer.
// It returns the number of bytes written/required, and true if the buffer has been large enough.
func marshalPacketInfo6(oob []byte, src netip.Addr, idx int) (int, bool) {
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

// parsePacketInfo searches platform-specific packet info control messages in the given OOB buffer.
// It returns a destination address, a local interface index, and true if a message has been found.
// Like [unix.ParseSocketControlMessage], it stops at a message longer than the rest of the buffer,
// and it skips a message too short for packet info, e.g. as truncation has shortened it.
func parsePacketInfo(oob []byte) (dst netip.Addr, idx int, ok bool) {
	for len(oob) >= cMsgHeaderSpace {
		hdr := (*cMsgHeader)(unsafe.Pointer(&oob[0]))
		n := int(hdr.Len)
		if n < cMsgHeaderSpace || n > len(oob) {
			break
		}
		data := oob[cMsgHeaderSpace:n]

		switch {
		case hdr.Level == cMsgIPv4DestAddrLevel && hdr.Type == cMsgIPv4DestAddrType && len(data) >= cMsgIPv4DestAddrSize:
			dst, ok = netip.AddrFrom4([cMsgIPv4DestAddrSize]byte(data)), true
		case hdr.Level == cMsgIPv4InterfaceLevel && hdr.Type == cMsgIPv4InterfaceType && len(data) >= cMsgIPv4InterfaceIndexEnd:
			// The link-level address may be shorter than cMsgIPv4Interface, so read the index only
			idx, ok = int(binary.NativeEndian.Uint16(data[cMsgIPv4InterfaceIndexOffset:])), true
		case hdr.Level == cMsgIPv6PacketInfoLevel && hdr.Type == cMsgIPv6PacketInfoType && len(data) >= cMsgIPv6PacketInfoSize:
			info := (*cMsgIPv6PacketInfo)(unsafe.Pointer(&data[0]))
			dst = netip.AddrFrom16(info.Addr)
			if info.Ifindex > 0 && isLocalIP6(dst) {
				dst = dst.WithZone(zoneCache.name(int(info.Ifindex)))
			}
			return dst, int(info.Ifindex), true
		}

		oob = oob[min(cMsgAlignOf(n), len(oob)):] // The last message may come without padding
	}

	return
}

// rawConnEnablePacketInfo enables IPv4 and/or IPv6 packet info for the given raw network connection.
func rawConnEnablePacketInfo(c syscall.RawConn, v4Only, v6Only bool) error {
	var err4 error
	if !v6Only {
		err4 = errors.Join( // Either option alone leaves IPv4 packet info incomplete
			rawConnSetSockOptBool(c, cMsgIPv4DestAddrLevel, cMsgIPv4DestAddrOpt, true),
			rawConnSetSockOptBool(c, cMsgIPv4InterfaceLevel, cMsgIPv4InterfaceOpt, true),
		)
		if err4 != nil && v4Only {
			return err4
		}
	}

	var err6 error
	if !v4Only {
		err6 = rawConnSetSockOptBool(c, cMsgIPv6PacketInfoLevel, cMsgIPv6PacketInfoOpt, true)
		if err6 != nil && v6Only {
			return err6
		}
	}

	if err4 != nil && err6 != nil {
		return err4
	}

	return nil
}

// rawConnIsV6Only returns true if IPV6_V6ONLY socket option is enabled.
func rawConnIsV6Only(c syscall.RawConn) (bool, error) {
	return rawConnGetSockOptBool(c, cMsgIPv6PacketInfoLevel, unix.IPV6_V6ONLY)
}

// rawConnGetSockOptBool makes a platform-specific socket configuration call
// that gets an integer option value and returns the corresponding boolean value.
func rawConnGetSockOptBool(c syscall.RawConn, level int, opt int) (bool, error) {
	var sVal int
	var sErr error
	err := c.Control(func(fd uintptr) {
		sVal, sErr = unix.GetsockoptInt(int(fd), level, opt) //nolint:gosec // G115
	})
	if err != nil {
		return false, err
	}

	return sVal == 1, sErr
}

// rawConnSetSockOptBool makes a platform-specific socket configuration call
// that sets an integer option value from the given boolean value.
func rawConnSetSockOptBool(c syscall.RawConn, level int, opt int, value bool) error {
	var sErr error
	err := c.Control(func(fd uintptr) {
		sErr = unix.SetsockoptInt(int(fd), level, opt, boolInt(value)) //nolint:gosec // G115
	})
	if err != nil {
		return err
	}

	return sErr
}
