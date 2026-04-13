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
// https://github.com/golang/net/tree/v0.53.0 and
// https://github.com/golang/sys/tree/v0.43.0 in April 2026.
// Sources: https://github.com/golang/net/blob/v0.53.0/ipv4/helper.go,
// https://github.com/golang/net/blob/v0.53.0/internal/socket/sys_posix.go,
// https://github.com/golang/sys/blob/v0.43.0/unix/sockcmsg_unix_other.go.
// Licenses: https://github.com/golang/net/blob/v0.53.0/LICENSE,
// https://github.com/golang/sys/blob/v0.43.0/LICENSE
// The original copyright is preserved below.

// Copyright 2017-2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris || windows || zos

package hybrid

import (
	"net"
	"net/netip"
	"strconv"
	"sync"
	"time"
)

// boolInt returns 1 for true and 0 for false.
func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// cMsgAlignOf returns the number of bytes a structure of size n takes
// including padding bytes to match the platform-specific data alignment.
//
// Note: cMsgAlign must be a power of two: 1, 2, 4, 8, etc.
func cMsgAlignOf(n int) int {
	return (n + cMsgAlign - 1) & ^(cMsgAlign - 1)
}

// isLocalIP6 checks if the given netip.Addr is a link-local unicast,
// interface-local multicast or link-local multicast IPv6 address.
//
// Note: x is assumed to be a valid IPv6, i.e. no extra checks are done.
func isLocalIP6(x netip.Addr) bool {
	a16 := x.As16()
	s := uint16(a16[0])<<8 | uint16(a16[1])
	if s&0xffc0 == 0xfe80 {
		return true
	}
	s = s & 0xff0f
	return s == 0xff01 || s == 0xff02
}

// An ipv6ZoneCache represents a cache holding partial network
// interface information. It is used for reducing the cost of IPv6
// addressing scope zone resolution.
//
// Multiple names sharing the index are managed by first-come
// first-served basis for consistency.
type ipv6ZoneCache struct {
	toIndex      map[string]int // interface name to its index
	toName       map[int]string // interface index to its name
	lastFetched  time.Time      // last time routing information was fetched
	sync.RWMutex                // guard the preceding
}

var zoneCache = ipv6ZoneCache{
	toIndex: make(map[string]int),
	toName:  make(map[int]string),
}

// update refreshes the network interface information if the cache was last
// updated more than 1 minute ago, or if force is set. It returns whether the
// cache was updated.
func (zc *ipv6ZoneCache) update(ift []net.Interface, force bool) (updated bool) { //nolint:unparam // keep the original
	zc.Lock()
	defer zc.Unlock()
	now := time.Now()
	if !force && zc.lastFetched.After(now.Add(-60*time.Second)) {
		return false
	}
	zc.lastFetched = now
	if len(ift) == 0 {
		var err error
		if ift, err = net.Interfaces(); err != nil {
			return false
		}
	}
	zc.toIndex = make(map[string]int, len(ift))
	zc.toName = make(map[int]string, len(ift))
	for _, ifi := range ift {
		zc.toIndex[ifi.Name] = ifi.Index
		if _, ok := zc.toName[ifi.Index]; !ok {
			zc.toName[ifi.Index] = ifi.Name
		}
	}
	return true
}

func (zc *ipv6ZoneCache) name(zone int) string {
	updated := zoneCache.update(nil, false)
	zoneCache.RLock()
	name, ok := zoneCache.toName[zone]
	zoneCache.RUnlock()
	if !ok && !updated {
		zoneCache.update(nil, true)
		zoneCache.RLock()
		name, ok = zoneCache.toName[zone]
		zoneCache.RUnlock()
	}
	if !ok { // last resort
		name = strconv.Itoa(zone)
	}
	return name
}

func (zc *ipv6ZoneCache) index(zone string) int {
	updated := zoneCache.update(nil, false)
	zoneCache.RLock()
	index, ok := zoneCache.toIndex[zone]
	zoneCache.RUnlock()
	if !ok && !updated {
		zoneCache.update(nil, true)
		zoneCache.RLock()
		index, ok = zoneCache.toIndex[zone]
		zoneCache.RUnlock()
	}
	if !ok { // last resort
		index, _ = strconv.Atoi(zone)
	}
	return index
}
