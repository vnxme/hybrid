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

// The code in this file is derived from https://github.com/golang/net/tree/v0.53.0 (April 2026).
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
	"sync/atomic"
	"time"
)

const (
	zoneMaxAge  = time.Minute // A list older than this is refreshed, as interfaces come and go
	zoneMissAge = time.Second // A list older than this is refreshed when an interface is missing from it
)

// An ipv6ZoneCache resolves IPv6 zones, i.e. the names and the indices of the network interfaces,
// without listing the interfaces each time. Its list is read without locking, and replaced by
// a single goroutine at a time once it's too old. Multiple names sharing an index are managed
// on a first-come, first-served basis.
type ipv6ZoneCache struct {
	current atomic.Pointer[zoneList]
	list    func() ([]net.Interface, error) // Lists the interfaces, e.g. net.Interfaces
	mu      sync.Mutex                      // Serializes listing the interfaces
}

// A zoneList is an immutable list of the network interfaces.
type zoneList struct {
	toIndex map[string]int // Interface names to their indices
	toName  map[int]string // Interface indices to their names
	fetched time.Time      // When the interfaces were listed, even if that failed
}

var zoneCache = ipv6ZoneCache{list: net.Interfaces}

// get returns a list of the interfaces no older than maxAge. If listing them fails,
// the previous list is kept, but its age is reset, so that it isn't retried too soon.
func (zc *ipv6ZoneCache) get(maxAge time.Duration) *zoneList {
	if l := zc.current.Load(); l != nil && time.Since(l.fetched) < maxAge {
		return l
	}

	zc.mu.Lock()
	defer zc.mu.Unlock()
	l := zc.current.Load()
	if l != nil && time.Since(l.fetched) < maxAge {
		return l // Another goroutine has just refreshed it
	}

	next := &zoneList{fetched: time.Now()}
	if ift, err := zc.list(); err == nil {
		next.toIndex = make(map[string]int, len(ift))
		next.toName = make(map[int]string, len(ift))
		for _, ifi := range ift {
			next.toIndex[ifi.Name] = ifi.Index
			if _, ok := next.toName[ifi.Index]; !ok {
				next.toName[ifi.Index] = ifi.Name
			}
		}
	} else if l != nil {
		next.toIndex, next.toName = l.toIndex, l.toName
	}
	zc.current.Store(next)
	return next
}

// name returns the name of the interface with the given index, or the index itself as a name.
func (zc *ipv6ZoneCache) name(index int) string {
	if name, ok := zc.get(zoneMaxAge).toName[index]; ok {
		return name
	}
	if name, ok := zc.get(zoneMissAge).toName[index]; ok {
		return name
	}
	return strconv.Itoa(index)
}

// index returns the index of the interface with the given name. A numeric name is taken as an index,
// unless an interface has this name. If no interface has the name, it returns 0.
func (zc *ipv6ZoneCache) index(name string) int {
	if index, ok := zc.get(zoneMaxAge).toIndex[name]; ok {
		return index
	}
	if index, err := strconv.Atoi(name); err == nil {
		return index
	}
	return zc.get(zoneMissAge).toIndex[name]
}
