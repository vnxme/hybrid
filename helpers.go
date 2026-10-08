// Copyright 2025 VNXME
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

// The isClosedChan function in this file was copied from
// https://github.com/golang/go/tree/go1.25.5 in December 2025.
// Source: https://github.com/golang/go/blob/go1.25.5/src/net/pipe.go
// License: https://github.com/golang/go/blob/go1.25.5/LICENSE
// The original copyright is preserved below.

// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hybrid

import (
	"net"
	"slices"
)

// cloneAddr returns a copy of a standard address, so that they don't share memory.
// Any other address, including a nil pointer of a standard type, is returned as is.
func cloneAddr(addr net.Addr) net.Addr {
	switch a := addr.(type) {
	case *net.UDPAddr:
		if a != nil {
			c := *a
			c.IP = slices.Clone(a.IP)
			return &c
		}
	case *net.TCPAddr:
		if a != nil {
			c := *a
			c.IP = slices.Clone(a.IP)
			return &c
		}
	case *net.IPAddr:
		if a != nil {
			c := *a
			c.IP = slices.Clone(a.IP)
			return &c
		}
	case *net.UnixAddr:
		if a != nil {
			c := *a
			return &c
		}
	}
	return addr
}

// equalAddr compares two net.Addr values, handling nil.
func equalAddr(a, b net.Addr) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.String() == b.String() && a.Network() == b.Network()
}

// isClosedChan reports whether c is closed, without blocking.
func isClosedChan(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}
