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

// Platforms with in_pktinfo and IP_RECVPKTINFO: darwin, illumos and solaris.

//go:build darwin || illumos || solaris

package pktinfo

import "golang.org/x/sys/unix"

type (
	cMsgIPv4PacketInfo = unix.Inet4Pktinfo
	cMsgIPv6PacketInfo = unix.Inet6Pktinfo
)

const (
	cMsgIPv4PacketInfoLevel = unix.IPPROTO_IP
	cMsgIPv4PacketInfoOpt   = unix.IP_RECVPKTINFO
	cMsgIPv4PacketInfoType  = cMsgIPv4PacketInfoOpt

	cMsgIPv4PacketInfoSize = unix.SizeofInet4Pktinfo
	cMsgIPv6PacketInfoSize = unix.SizeofInet6Pktinfo
)

// setPacketInfo4 fills IPv4 packet info for sending from src through the interface idx.
func setPacketInfo4(info *cMsgIPv4PacketInfo, src [4]byte, idx int) {
	info.Spec_dst = src
	// The kernel ignores it for sending, but leave no stale bytes from a pooled buffer
	info.Addr = [4]byte{}
	info.Ifindex = uint32(idx) //nolint:gosec // G115
}
