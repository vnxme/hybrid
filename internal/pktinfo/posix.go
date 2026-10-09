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

//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris || zos

package pktinfo

import (
	"syscall"

	"golang.org/x/sys/unix"
)

type cMsgHeader = unix.Cmsghdr

const (
	cMsgIPv6PacketInfoLevel = unix.IPPROTO_IPV6
	cMsgIPv6PacketInfoOpt   = unix.IPV6_RECVPKTINFO
	cMsgIPv6PacketInfoType  = unix.IPV6_PKTINFO

	Truncated = unix.MSG_CTRUNC // Set in the flags of a read whose control messages are truncated

	// ChoosesSource reports whether the system chooses the source address of a packet whose packet info
	// carries an unspecified one, as it does for a packet without packet info.
	ChoosesSource = true
)

var (
	cMsgAlign       = unix.CmsgSpace(1) - cMsgHeaderSpace
	cMsgHeaderSpace = unix.CmsgLen(0)
)

// cMsgLenOf returns the length of the control message with the given header, including the header.
func cMsgLenOf(hdr *cMsgHeader) int {
	return int(hdr.Len) //nolint:gosec // G115
}

// IsV6Only returns true if IPV6_V6ONLY socket option is enabled.
func IsV6Only(c syscall.RawConn) (bool, error) {
	return rawConnGetSockOptBool(c, unix.IPPROTO_IPV6, unix.IPV6_V6ONLY)
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

	return sVal != 0, sErr
}

// rawConnEnableSockOpt makes a platform-specific socket configuration call
// that enables an option, i.e. sets its integer value to 1.
func rawConnEnableSockOpt(c syscall.RawConn, level int, opt int) error {
	var sErr error
	err := c.Control(func(fd uintptr) {
		sErr = unix.SetsockoptInt(int(fd), level, opt, 1) //nolint:gosec // G115
	})
	if err != nil {
		return err
	}

	return sErr
}
