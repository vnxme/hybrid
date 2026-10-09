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

//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris || windows || zos

package pktinfo

import (
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

func BenchmarkZoneCache(b *testing.B) {
	ifs, err := net.Interfaces()
	if err != nil || len(ifs) == 0 {
		b.Skipf("net.Interfaces() = %d, %v", len(ifs), err)
	}
	ifc := ifs[0]

	b.Run("Name", func(b *testing.B) {
		for b.Loop() {
			_ = zoneCache.name(ifc.Index)
		}
	})
	b.Run("NameParallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = zoneCache.name(ifc.Index)
			}
		})
	})
	b.Run("Index", func(b *testing.B) {
		for b.Loop() {
			_ = zoneCache.index(ifc.Name)
		}
	})
	b.Run("IndexParallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = zoneCache.index(ifc.Name)
			}
		})
	})
	b.Run("Missing", func(b *testing.B) { // An interface that doesn't exist, e.g. as it's just gone
		for b.Loop() {
			_ = zoneCache.name(1 << 30)
		}
	})
}

// fakeInterfaces lists the given interfaces, or fails if err is set, and counts the calls.
type fakeInterfaces struct {
	mu    sync.Mutex
	ift   []net.Interface
	err   error
	calls int
}

func (f *fakeInterfaces) list() ([]net.Interface, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.ift, f.err
}

func (f *fakeInterfaces) set(err error, ift ...net.Interface) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ift, f.err = ift, err
}

func (f *fakeInterfaces) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// TestZoneCache ensures names and indices are resolved from the list of interfaces, falling back
// to numeric zones, and that the list is refreshed once it's older than zoneMaxAge, or older than
// zoneMissAge when an interface is missing, but not more often.
func TestZoneCache(t *testing.T) {
	f := &fakeInterfaces{}
	f.set(nil, net.Interface{Index: 1, Name: "a"}, net.Interface{Index: 1, Name: "a2"})
	zc := &ipv6ZoneCache{list: f.list}
	age := func(d time.Duration) { // Makes the current list older
		l := *zc.current.Load()
		l.fetched = l.fetched.Add(-d)
		zc.current.Store(&l)
	}
	check := func(gotName, wantName string, gotIndex, wantIndex, wantCalls int) {
		t.Helper()
		if gotName != wantName || gotIndex != wantIndex || f.count() != wantCalls {
			t.Errorf("name, index, calls = %q, %d, %d, want %q, %d, %d",
				gotName, gotIndex, f.count(), wantName, wantIndex, wantCalls)
		}
	}

	// The first lookup lists the interfaces, and the first of the names sharing an index wins
	check(zc.name(1), "a", zc.index("a2"), 1, 1)
	check(zc.name(1), "a", zc.index("a"), 1, 1)

	// A missing interface doesn't refresh a list younger than zoneMissAge
	f.set(nil, net.Interface{Index: 1, Name: "a"}, net.Interface{Index: 2, Name: "b"})
	check(zc.name(2), "2", zc.index("b"), 0, 1)

	// It refreshes an older one, which then isn't refreshed again for another missing interface
	age(2 * zoneMissAge)
	check(zc.name(2), "b", zc.index("b"), 2, 2)
	check(zc.name(3), "3", zc.index("c"), 0, 2)

	// A numeric zone is an index without refreshing, even if the list could be refreshed
	age(2 * zoneMissAge)
	check(zc.name(1), "a", zc.index("7"), 7, 2)

	// Any list older than zoneMaxAge is refreshed, and if that fails, the old one is kept for a while
	f.set(errors.New("failed"))
	age(2 * zoneMaxAge)
	check(zc.name(1), "a", zc.index("b"), 2, 3)
	check(zc.name(9), "9", zc.index("b"), 2, 3)
}

// TestZoneCacheConcurrent ensures concurrent lookups list the interfaces once.
func TestZoneCacheConcurrent(t *testing.T) {
	f := &fakeInterfaces{}
	f.set(nil, net.Interface{Index: 1, Name: "a"})
	zc := &ipv6ZoneCache{list: f.list}
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for j := range 100 {
				if name, index := zc.name(1), zc.index("a"); name != "a" || index != 1 {
					t.Errorf("name(1), index(a) = %q, %d, want a, 1", name, index)
				}
				_ = zc.name(2 + (i+j)%4) // Missing, but the list is younger than zoneMissAge
				_ = zc.index(strconv.Itoa(i))
			}
		})
	}
	wg.Wait()
	if calls := f.count(); calls != 1 {
		t.Errorf("interfaces listed %d times, want 1", calls)
	}
}

// TestZoneCacheInterfaces ensures the real interfaces are resolved.
func TestZoneCacheInterfaces(t *testing.T) {
	ifs, err := net.Interfaces()
	if err != nil {
		t.Skipf("net.Interfaces() = %v", err)
	}
	names := make(map[int]string, len(ifs)) // The first of the names sharing an index
	for _, ifc := range ifs {
		if _, ok := names[ifc.Index]; !ok {
			names[ifc.Index] = ifc.Name
		}
	}
	for _, ifc := range ifs {
		if name, index := zoneCache.name(ifc.Index), zoneCache.index(ifc.Name); name != names[ifc.Index] || index != ifc.Index {
			t.Errorf("name(%d), index(%s) = %q, %d, want %q, %d", ifc.Index, ifc.Name, name, index, names[ifc.Index], ifc.Index)
		}
	}
}
