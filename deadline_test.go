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

package hybrid

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	deadlineFire    = 10 * time.Millisecond  // Time until a deadline set in the future fires
	deadlinePending = 50 * time.Millisecond  // Time until a deadline changed right after setting fires
	deadlineQuiet   = 100 * time.Millisecond // Time to make sure nothing happens
	deadlineLimit   = time.Second            // Time to fail instead of hanging
)

// expectOpen fails unless ch stays open for deadlineQuiet.
func expectOpen(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Errorf("channel is closed, want open")
	case <-time.After(deadlineQuiet):
	}
}

// expectClosed fails unless ch is closed within deadlineLimit.
func expectClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(deadlineLimit):
		t.Errorf("channel is open, want closed")
	}
}

func TestDeadline(t *testing.T) {
	tests := []struct {
		name string
		ctor func() Deadline
	}{
		{"Simple", SimpleDeadline},
		{"NilHook", func() Deadline { return HookDeadline(nil) }},
		{"NoopHook", func() Deadline { return HookDeadline(func() {}) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run("Initial", func(t *testing.T) {
				expectOpen(t, test.ctor().Wait())
			})

			t.Run("FirstUse", func(t *testing.T) {
				d := test.ctor()
				var chans [8]<-chan struct{}
				var wg sync.WaitGroup
				for i := range chans {
					wg.Go(func() { chans[i] = d.Wait() })
				}
				wg.Wait()
				for i, ch := range chans {
					if ch == nil || ch != chans[0] {
						t.Fatalf("Wait() #%d = %v, want the same non-nil channel as #0 = %v", i, ch, chans[0])
					}
				}
				expectOpen(t, chans[0])
			})

			t.Run("Zero", func(t *testing.T) {
				d := test.ctor()
				d.Set(time.Time{})
				expectOpen(t, d.Wait())
			})

			t.Run("Past", func(t *testing.T) {
				d := test.ctor()
				d.Set(time.Now().Add(-time.Second))
				if !isClosedChan(d.Wait()) {
					t.Errorf("channel is open, want closed immediately")
				}
			})

			t.Run("Future", func(t *testing.T) {
				d := test.ctor()
				d.Set(time.Now().Add(deadlinePending))
				ch := d.Wait()
				if isClosedChan(ch) {
					t.Errorf("channel is closed, want open until the deadline")
				}
				expectClosed(t, ch)
			})

			t.Run("Extend", func(t *testing.T) {
				d := test.ctor()
				d.Set(time.Now().Add(deadlinePending))
				ch := d.Wait()
				d.Set(time.Now().Add(time.Hour))
				if d.Wait() != ch {
					t.Errorf("channel is replaced, want kept until closed")
				}
				expectOpen(t, ch)
			})

			t.Run("Cancel", func(t *testing.T) {
				d := test.ctor()
				d.Set(time.Now().Add(deadlinePending))
				d.Set(time.Time{})
				expectOpen(t, d.Wait())
			})

			t.Run("Shorten", func(t *testing.T) {
				d := test.ctor()
				d.Set(time.Now().Add(time.Hour))
				d.Set(time.Now().Add(-time.Second))
				if !isClosedChan(d.Wait()) {
					t.Errorf("channel is open, want closed immediately")
				}
			})

			t.Run("Refresh", func(t *testing.T) {
				d := test.ctor()
				d.Set(time.Now().Add(-time.Second))
				expired := d.Wait()

				d.Set(time.Now().Add(deadlinePending))
				ch := d.Wait()
				if ch == expired || isClosedChan(ch) {
					t.Errorf("channel is closed, want a new open one until the deadline")
				}
				expectClosed(t, ch)

				d.Set(time.Time{})
				expectOpen(t, d.Wait())
			})

			t.Run("Concurrent", func(t *testing.T) {
				d := test.ctor()
				var wg sync.WaitGroup
				for i := range 8 {
					wg.Go(func() {
						for j := range 100 {
							switch (i + j) % 4 {
							case 0:
								d.Set(time.Time{})
							case 1:
								d.Set(time.Now().Add(-time.Second))
							case 2:
								d.Set(time.Now().Add(time.Duration(j) * time.Microsecond))
							default:
								_ = isClosedChan(d.Wait())
							}
						}
					})
				}
				wg.Wait()

				// The deadline remains usable
				d.Set(time.Now().Add(deadlineFire))
				expectClosed(t, d.Wait())
			})
		})
	}
}

func TestHookDeadline(t *testing.T) {
	t.Run("RunBeforeClose", func(t *testing.T) {
		var runs atomic.Int32
		d := HookDeadline(func() {
			time.Sleep(deadlineFire) // Let a premature close be observed
			runs.Add(1)
		})

		d.Set(time.Now().Add(deadlineFire))
		expectClosed(t, d.Wait())
		if n := runs.Load(); n != 1 {
			t.Errorf("hook ran %d times, want 1", n)
		}

		// The hook runs again once refreshed
		d.Set(time.Now().Add(deadlineFire))
		expectClosed(t, d.Wait())
		if n := runs.Load(); n != 2 {
			t.Errorf("hook ran %d times, want 2", n)
		}
	})

	t.Run("NoRun", func(t *testing.T) {
		var runs atomic.Int32
		d := HookDeadline(func() { runs.Add(1) })

		d.Set(time.Time{})
		d.Set(time.Now().Add(-time.Second)) // A time in the past closes the channel only

		d.Set(time.Now().Add(deadlinePending))
		d.Set(time.Time{}) // A cancelled timer doesn't run the hook
		d.Set(time.Now().Add(deadlinePending))
		d.Set(time.Now().Add(time.Hour)) // An extended timer doesn't run the hook before the time

		time.Sleep(deadlineQuiet)
		if n := runs.Load(); n != 0 {
			t.Errorf("hook ran %d times, want 0", n)
		}
	})

	t.Run("SetWaitsForHook", func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		d := HookDeadline(func() {
			close(entered)
			<-release
		})

		d.Set(time.Now().Add(deadlineFire))
		select {
		case <-entered:
		case <-time.After(deadlineLimit):
			t.Fatalf("hook isn't run")
		}

		set := make(chan struct{})
		go func() {
			d.Set(time.Time{})
			close(set)
		}()
		expectOpen(t, set) // Set is blocked while the hook is running

		close(release)
		expectClosed(t, set)
		expectOpen(t, d.Wait()) // Set has taken effect after the hook
	})
}
