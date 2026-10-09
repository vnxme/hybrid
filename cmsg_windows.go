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

//go:build windows

package hybrid

import (
	"net"
	"net/netip"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

type (
	cMsgHeader = windows.WSACMSGHDR

	cMsgIPv4PacketInfo = windows.IN_PKTINFO
	cMsgIPv6PacketInfo = windows.IN6_PKTINFO
)

const (
	cMsgHeaderSize = int(unsafe.Sizeof(cMsgHeader{}))

	cMsgIPv4PacketInfoLevel = windows.IPPROTO_IP
	cMsgIPv6PacketInfoLevel = windows.IPPROTO_IPV6

	cMsgIPv4PacketInfoOpt = windows.IP_PKTINFO
	cMsgIPv6PacketInfoOpt = windows.IPV6_PKTINFO

	cMsgIPv4PacketInfoSize = int(unsafe.Sizeof(cMsgIPv4PacketInfo{}))
	cMsgIPv6PacketInfoSize = int(unsafe.Sizeof(cMsgIPv6PacketInfo{}))

	cMsgIPv4PacketInfoType = cMsgIPv4PacketInfoOpt
	cMsgIPv6PacketInfoType = cMsgIPv6PacketInfoOpt

	cMsgTruncated = windows.MSG_CTRUNC // Set in the flags of a read whose control messages are truncated
)

var (
	cMsgAlign = int(unsafe.Sizeof(windows.Handle(0)))

	cMsgBufferSize = max(128, cMsgIPv4PacketInfoPayload, cMsgIPv6PacketInfoPayload)

	cMsgHeaderSpace = cMsgAlignOf(cMsgHeaderSize)

	cMsgIPv4PacketInfoLen = uintptr(cMsgHeaderSpace + cMsgIPv4PacketInfoSize)
	cMsgIPv6PacketInfoLen = uintptr(cMsgHeaderSpace + cMsgIPv6PacketInfoSize)

	cMsgIPv4PacketInfoSpace = cMsgAlignOf(cMsgIPv4PacketInfoSize)
	cMsgIPv6PacketInfoSpace = cMsgAlignOf(cMsgIPv6PacketInfoSize)

	cMsgIPv4PacketInfoPayload = cMsgHeaderSpace + cMsgIPv4PacketInfoSpace
	cMsgIPv6PacketInfoPayload = cMsgHeaderSpace + cMsgIPv6PacketInfoSpace
)

// marshalPacketInfo4 writes a platform-specific IPv4 packet info control message into the given OOB buffer.
// It returns the number of bytes written/required, and true if the buffer has been large enough.
func marshalPacketInfo4(oob []byte, src netip.Addr, idx int, full bool) (int, bool) {
	if len(oob) < cMsgIPv4PacketInfoPayload {
		return cMsgIPv4PacketInfoPayload, false
	}

	if full {
		hdr := (*cMsgHeader)(unsafe.Pointer(&oob[0]))
		hdr.Len, hdr.Level, hdr.Type = cMsgIPv4PacketInfoLen, cMsgIPv4PacketInfoLevel, cMsgIPv4PacketInfoType
	}

	info := (*cMsgIPv4PacketInfo)(unsafe.Pointer(&oob[cMsgHeaderSpace]))
	if src.Is4() {
		copy(info.Addr[:], src.AsSlice())
	} else {
		copy(info.Addr[:], net.IPv4zero)
	}
	info.Ifindex = uint32(max(0, idx)) //nolint:gosec // G115

	return cMsgIPv4PacketInfoPayload, true
}

// marshalPacketInfo6 write a platform-specific IPv6 packet info control message into the given OOB buffer.
// It returns the number of bytes written/required, and true if the buffer has been large enough.
func marshalPacketInfo6(oob []byte, src netip.Addr, idx int, full bool) (int, bool) {
	if len(oob) < cMsgIPv6PacketInfoPayload {
		return cMsgIPv6PacketInfoPayload, false
	}

	if full {
		hdr := (*cMsgHeader)(unsafe.Pointer(&oob[0]))
		hdr.Len, hdr.Level, hdr.Type = cMsgIPv6PacketInfoLen, cMsgIPv6PacketInfoLevel, cMsgIPv6PacketInfoType
	}

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

// parsePacketInfo searches a platform-specific packet info control message in the given OOB buffer.
// It returns a destination address, a local interface index, and true if the message has been found.
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
		case hdr.Level == cMsgIPv4PacketInfoLevel && hdr.Type == cMsgIPv4PacketInfoType && len(data) >= cMsgIPv4PacketInfoSize:
			info := (*cMsgIPv4PacketInfo)(unsafe.Pointer(&data[0]))
			return netip.AddrFrom4(info.Addr), int(info.Ifindex), true
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
		err4 = rawConnSetSockOptBool(c, cMsgIPv4PacketInfoLevel, cMsgIPv4PacketInfoOpt, true)
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
	return rawConnGetSockOptBool(c, cMsgIPv6PacketInfoLevel, windows.IPV6_V6ONLY)
}

// rawConnGetSockOptBool makes a platform-specific socket configuration call
// that gets an integer option value and returns the corresponding boolean value.
func rawConnGetSockOptBool(c syscall.RawConn, level int, opt int) (bool, error) {
	var sVal int
	var sErr error
	err := c.Control(func(fd uintptr) {
		sVal, sErr = windows.GetsockoptInt(windows.Handle(fd), level, opt)
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
		sErr = windows.SetsockoptInt(windows.Handle(fd), level, opt, boolInt(value))
	})
	if err != nil {
		return err
	}

	return sErr
}
