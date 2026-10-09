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

//go:build aix || dragonfly || freebsd || netbsd || openbsd

package pktinfo

import (
	"errors"
	"slices"
	"syscall"
	"testing"
)

// failingRawConn is a syscall.RawConn whose Control fails for the given calls, counted from 0,
// and does nothing for the others, as if a socket option has been set.
type failingRawConn struct {
	syscall.RawConn
	calls int
	fail  []int
}

func (c *failingRawConn) Control(func(fd uintptr)) error {
	defer func() { c.calls++ }()
	if slices.Contains(c.fail, c.calls) {
		return errors.New("failed")
	}
	return nil
}

// TestEnableErrors ensures IPv4 packet info fails if either of its two options fails,
// i.e. IP_RECVDSTADDR (call 0) or IP_RECVIF (call 1), and that a dual-stack socket fails only
// if IPv6 packet info (call 2) fails too.
func TestEnableErrors(t *testing.T) {
	tests := []struct {
		name           string
		v4Only, v6Only bool
		fail           []int
		wantErr        bool
	}{
		{"DualStack", false, false, nil, false},
		{"DualStackNoIPv4", false, false, []int{0, 1}, false},
		{"DualStackNoIPv6", false, false, []int{2}, false},
		{"DualStackNoDestAddrNoIPv6", false, false, []int{0, 2}, true},
		{"DualStackNoInterfaceNoIPv6", false, false, []int{1, 2}, true},
		{"IPv4Only", true, false, nil, false},
		{"IPv4OnlyNoDestAddr", true, false, []int{0}, true},
		{"IPv4OnlyNoInterface", true, false, []int{1}, true},
		{"IPv6Only", false, true, nil, false},
		{"IPv6OnlyNoIPv6", false, true, []int{0}, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := &failingRawConn{fail: test.fail}
			if err := Enable(c, test.v4Only, test.v6Only); (err != nil) != test.wantErr {
				t.Errorf("Enable() = %v, want an error %v", err, test.wantErr)
			}
		})
	}
}
