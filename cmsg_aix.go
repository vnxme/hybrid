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

//go:build aix

package hybrid

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

type inet6Pktinfo struct {
	Addr    [16]byte /* in6_addr */
	Ifindex uint32
}

type (
	cMsgIPv4Interface  = syscall.SockaddrDatalink
	cMsgIPv6PacketInfo = inet6Pktinfo
)

const (
	cMsgIPv4InterfaceOpt = unix.IP_RECVINTERFACE // unix.IP_RECVIF is defined, but it doesn't work

	cMsgIPv4InterfaceSize  = unix.SizeofSockaddrDatalink
	cMsgIPv6PacketInfoSize = int(unsafe.Sizeof(cMsgIPv6PacketInfo{}))
)
