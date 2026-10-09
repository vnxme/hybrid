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

package pktinfo

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// cMsgHeader is a control message header with SetLen, like [unix.Cmsghdr] has.
type cMsgHeader windows.WSACMSGHDR

// SetLen sets the length of the control message, including the header.
func (h *cMsgHeader) SetLen(n int) {
	h.Len = uintptr(n)
}

type (
	cMsgIPv4PacketInfo = windows.IN_PKTINFO
	cMsgIPv6PacketInfo = windows.IN6_PKTINFO
)

const (
	cMsgIPv4PacketInfoLevel = windows.IPPROTO_IP
	cMsgIPv4PacketInfoOpt   = windows.IP_PKTINFO
	cMsgIPv4PacketInfoType  = cMsgIPv4PacketInfoOpt

	cMsgIPv6PacketInfoLevel = windows.IPPROTO_IPV6
	cMsgIPv6PacketInfoOpt   = windows.IPV6_PKTINFO
	cMsgIPv6PacketInfoType  = cMsgIPv6PacketInfoOpt

	cMsgIPv4PacketInfoSize = int(unsafe.Sizeof(cMsgIPv4PacketInfo{}))
	cMsgIPv6PacketInfoSize = int(unsafe.Sizeof(cMsgIPv6PacketInfo{}))

	Truncated = windows.MSG_CTRUNC // Set in the flags of a read whose control messages are truncated
)

var (
	cMsgAlign       = int(unsafe.Sizeof(windows.Handle(0)))
	cMsgHeaderSpace = cMsgAlignOf(int(unsafe.Sizeof(cMsgHeader{})))
)

// cMsgLenOf returns the length of the control message with the given header, including the header.
func cMsgLenOf(hdr *cMsgHeader) int {
	return int(hdr.Len)
}

// setPacketInfo4 fills IPv4 packet info for sending from src through the interface idx.
func setPacketInfo4(info *cMsgIPv4PacketInfo, src [4]byte, idx int) {
	info.Addr = src
	info.Ifindex = uint32(idx) //nolint:gosec // G115
}

// IsV6Only returns true if IPV6_V6ONLY socket option is enabled.
func IsV6Only(c syscall.RawConn) (bool, error) {
	return rawConnGetSockOptBool(c, windows.IPPROTO_IPV6, windows.IPV6_V6ONLY)
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
