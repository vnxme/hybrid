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

// A significant part of the code in this file was copied from
// https://github.com/golang/sys/tree/v0.43.0 in April 2026.
// Source: https://github.com/golang/sys/blob/v0.43.0/unix/sockcmsg_unix_other.go.
// License: https://github.com/golang/sys/blob/v0.43.0/LICENSE
// The original copyright is preserved below.

// Copyright 2017-2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris || windows || zos

package pktinfo

import (
	"errors"
	"iter"
	"net/netip"
	"syscall"
	"unsafe"
)

// Enable enables packet info on a socket for IPv4 unless v6Only is set, and for IPv6 unless v4Only is set.
// For a dual-stack socket, i.e. with neither set, it fails only if it fails for both IP versions,
// reporting both errors. It fails if both are set, as no IP version would be left.
func Enable(c syscall.RawConn, v4Only, v6Only bool) error {
	if v4Only && v6Only {
		return errors.New("hybrid: packet info for neither IP version")
	}

	var err4, err6 error
	if !v6Only {
		err4 = enable4(c)
	}
	if !v4Only {
		err6 = rawConnEnableSockOpt(c, cMsgIPv6PacketInfoLevel, cMsgIPv6PacketInfoOpt)
	}

	switch {
	case v4Only:
		return err4
	case v6Only:
		return err6
	case err4 != nil && err6 != nil:
		return errors.Join(err4, err6)
	}
	return nil
}

// Marshal writes a packet info control message for sending from src through the interface idx into oob,
// for IPv4 if v4 is set, or for IPv6 otherwise, and returns the number of bytes written. It clears
// the bytes it skips, e.g. padding, so that a pooled buffer passes no stale bytes on. Like
// binary.BigEndian.PutUint32, it panics if oob is too short, which BufferSize bytes never are.
func Marshal(oob []byte, v4 bool, src netip.Addr, idx int) int {
	if v4 {
		return marshal4(oob, src, idx)
	}
	return marshal6(oob, src, idx)
}

// cMsgAlignOf returns the number of bytes a structure of size n takes
// including padding bytes to match the platform-specific data alignment.
//
// Note: cMsgAlign must be a power of two: 1, 2, 4, 8, etc.
func cMsgAlignOf(n int) int {
	return (n + cMsgAlign - 1) & ^(cMsgAlign - 1)
}

// cMsgs returns an iterator over the control messages in the given OOB buffer, yielding the header
// and the data of each. Like [unix.ParseSocketControlMessage], it stops at a message longer than
// the rest of the buffer, and it accepts the last message without padding.
func cMsgs(oob []byte) iter.Seq2[*cMsgHeader, []byte] {
	return func(yield func(*cMsgHeader, []byte) bool) {
		for b := oob; len(b) >= cMsgHeaderSpace; {
			hdr := (*cMsgHeader)(unsafe.Pointer(&b[0]))
			n := cMsgLenOf(hdr)
			if n < cMsgHeaderSpace || n > len(b) || !yield(hdr, b[cMsgHeaderSpace:n]) {
				return
			}
			b = b[min(cMsgAlignOf(n), len(b)):]
		}
	}
}
