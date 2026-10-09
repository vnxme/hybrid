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

// cMsgAlignOf returns the number of bytes a structure of size n takes
// including padding bytes to match the platform-specific data alignment.
//
// Note: cMsgAlign must be a power of two: 1, 2, 4, 8, etc.
func cMsgAlignOf(n int) int {
	return (n + cMsgAlign - 1) & ^(cMsgAlign - 1)
}
