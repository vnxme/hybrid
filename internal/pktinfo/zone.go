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
// https://github.com/golang/net/tree/v0.53.0 in April 2026.
// Source: https://github.com/golang/net/blob/v0.53.0/internal/socket/sys_posix.go.
// License: https://github.com/golang/net/blob/v0.53.0/LICENSE
// The original copyright is preserved below.

// Copyright 2017-2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris || windows || zos

package pktinfo

import (
	"net"
	"strconv"
	"sync"
	"time"
)

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
