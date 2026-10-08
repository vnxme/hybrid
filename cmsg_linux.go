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

//go:build linux

package hybrid

import (
	"net"
	"net/netip"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

type (
	cMsgHeader = unix.Cmsghdr

	cMsgIPv4PacketInfo = unix.Inet4Pktinfo
	cMsgIPv6PacketInfo = unix.Inet6Pktinfo
)

const (
	cMsgHeaderSize = unix.SizeofCmsghdr

	cMsgIPv4PacketInfoLevel = unix.IPPROTO_IP
	cMsgIPv6PacketInfoLevel = unix.IPPROTO_IPV6

	cMsgIPv4PacketInfoOpt = unix.IP_PKTINFO
	cMsgIPv6PacketInfoOpt = unix.IPV6_RECVPKTINFO

	cMsgIPv4PacketInfoSize = unix.SizeofInet4Pktinfo
	cMsgIPv6PacketInfoSize = unix.SizeofInet6Pktinfo

	cMsgIPv4PacketInfoType = cMsgIPv4PacketInfoOpt
	cMsgIPv6PacketInfoType = unix.IPV6_PKTINFO

	cMsgTruncated = unix.MSG_CTRUNC // Set in the flags of a read whose control messages are truncated
)

var (
	cMsgAlign = unix.CmsgSpace(1) - cMsgHeaderSpace

	cMsgBufferSize = max(128, cMsgIPv4PacketInfoPayload, cMsgIPv6PacketInfoPayload)

	cMsgHeaderSpace = unix.CmsgLen(0)

	cMsgIPv4PacketInfoLen = cMsgHeaderSpace + cMsgIPv4PacketInfoSize
	cMsgIPv6PacketInfoLen = cMsgHeaderSpace + cMsgIPv6PacketInfoSize

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

	info := (*cMsgIPv4PacketInfo)(unsafe.Pointer(&oob[cMsgHeaderSpace]))
	if src.Is4() {
		copy(info.Spec_dst[:], src.AsSlice())
	} else {
		copy(info.Spec_dst[:], net.IPv4zero)
	}
	info.Ifindex = int32(max(0, idx)) //nolint:gosec // G115

	if full {
		copy(info.Addr[:], net.IPv4zero)

		hdr := (*cMsgHeader)(unsafe.Pointer(&oob[0]))
		hdr.SetLen(cMsgIPv4PacketInfoLen)
		hdr.Level, hdr.Type = cMsgIPv4PacketInfoLevel, cMsgIPv4PacketInfoType
	}

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
		hdr.SetLen(cMsgIPv6PacketInfoLen)
		hdr.Level, hdr.Type = cMsgIPv6PacketInfoLevel, cMsgIPv6PacketInfoType
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
// It returns a destination address, a local interface index, and true it the message has been found.
func parsePacketInfo(oob []byte) (dst netip.Addr, idx int, ok bool) {
	var hdr *cMsgHeader
	var space int
	for len(oob) > cMsgHeaderSpace {
		hdr = (*cMsgHeader)(unsafe.Pointer(&oob[0]))
		space = cMsgAlignOf(int(hdr.Len)) //nolint:gosec // G115
		if space <= cMsgHeaderSpace || len(oob) < space {
			break
		}

		if hdr.Level == cMsgIPv4PacketInfoLevel && hdr.Type == cMsgIPv4PacketInfoType {
			info := (*cMsgIPv4PacketInfo)(unsafe.Pointer(&oob[cMsgHeaderSpace]))
			dst, idx, ok = netip.AddrFrom4(info.Addr), int(info.Ifindex), true
			break
		}

		if hdr.Level == cMsgIPv6PacketInfoLevel && hdr.Type == cMsgIPv6PacketInfoType {
			info := (*cMsgIPv6PacketInfo)(unsafe.Pointer(&oob[cMsgHeaderSpace]))
			dst, idx, ok = netip.AddrFrom16(info.Addr), int(info.Ifindex), true
			if info.Ifindex > 0 && isLocalIP6(dst) {
				dst = dst.WithZone(zoneCache.name(int(info.Ifindex)))
			}
			break
		}

		oob = oob[space:]
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
