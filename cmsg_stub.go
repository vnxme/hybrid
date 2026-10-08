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

//go:build !(aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris || windows || zos)

package hybrid

import (
	"errors"
	"fmt"
	"net/netip"
	"runtime"
	"syscall"
)

const (
	cMsgBufferSize = 0
	cMsgTruncated  = 0
)

var errPacketInfoUnsupported = fmt.Errorf("hybrid: packet info on %s/%s: %w", runtime.GOOS, runtime.GOARCH, errors.ErrUnsupported)

// marshalPacketInfo4 writes a platform-specific IPv4 packet info control message into the given OOB buffer.
// It returns the number of bytes written/required, and true if the buffer has been large enough.
func marshalPacketInfo4(oob []byte, src netip.Addr, idx int, full bool) (int, bool) {
	return 0, true
}

// marshalPacketInfo6 write a platform-specific IPv6 packet info control message into the given OOB buffer.
// It returns the number of bytes written/required, and true if the buffer has been large enough.
func marshalPacketInfo6(oob []byte, src netip.Addr, idx int, full bool) (int, bool) {
	return 0, true
}

// parsePacketInfo searches a platform-specific packet info control message in the given OOB buffer.
// It returns a destination address, a local interface index, and true it the message has been found.
func parsePacketInfo(oob []byte) (dst netip.Addr, idx int, ok bool) {
	return
}

// rawConnEnablePacketInfo enables IPv4 and/or IPv6 packet info for the given raw network connection.
func rawConnEnablePacketInfo(c syscall.RawConn, v4Only, v6Only bool) error {
	return errPacketInfoUnsupported
}

// rawConnIsV6Only returns true if IPV6_V6ONLY socket option is enabled.
func rawConnIsV6Only(c syscall.RawConn) (bool, error) {
	return false, errPacketInfoUnsupported
}
