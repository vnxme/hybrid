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

// A significant part of the code in this file was copied from
// https://github.com/golang/go/tree/go1.25.5 in December 2025.
// Source: https://github.com/golang/go/blob/go1.25.5/src/net/pipe.go
// License: https://github.com/golang/go/blob/go1.25.5/LICENSE
// The original copyright is preserved below.

// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hybrid

import (
	"sync"
	"sync/atomic"
	"time"
)

// deadlineHook is run by a deadline when its timer fires, before its channel is closed.
type deadlineHook interface {
	run()
}

// noHook is a deadline hook doing nothing. Being zero-sized, it adds no memory to a deadline.
type noHook struct{}

func (noHook) run() {}

// funcHook is a deadline hook calling a non-nil function.
type funcHook func()

func (f funcHook) run() { f() }

// deadline is an abstraction for handling timeouts.
// Once a timer set up by Set fires, it runs its hook, see [HookDeadline].
type deadline[H deadlineHook] struct {
	hook   H
	timer  *time.Timer
	cancel atomic.Value // Holds a chan struct{} created on first use, so that Wait needn't lock mu
	mu     sync.Mutex   // Guards timer and replacing cancel
}

// makeDeadline returns a deadline value to be embedded in a struct, avoiding a separate allocation.
func makeDeadline[H deadlineHook](hook H) deadline[H] {
	return deadline[H]{hook: hook}
}

// SimpleDeadline returns a pointer to the struct that satisfies the Deadline interface.
func SimpleDeadline() Deadline {
	d := makeDeadline(noHook{})
	return &d
}

// HookDeadline returns a pointer to the struct that satisfies the Deadline interface.
// Unless the hook argument is nil, it is run as follows:
//
//   - The hook is run on its own goroutine once the time passed to Set in the future
//     has come. It isn't run if Set is called with a zero time or a time in the past.
//   - The hook is run before the channel returned by Wait is closed, so its effects
//     are observed by any goroutine having received from the channel.
//   - Set waits for a running hook to return, so once Set returns, no hook started
//     for an earlier time is running, and none will be started.
//
// Consequently, the hook must not call Set on the same deadline, directly or indirectly,
// as it always deadlocks. The hook should also return promptly, as it blocks concurrent
// calls of Set. Wait never blocks, so the hook may call it.
func HookDeadline(hook func()) Deadline {
	if hook == nil {
		return SimpleDeadline()
	}
	d := makeDeadline(funcHook(hook))
	return &d
}

// load returns the current channel, creating it on first use. Unlike Set, it needn't lock mu:
// only the first channel is stored without mu, and a channel is stored in an interface without
// an allocation.
func (d *deadline[H]) load() chan struct{} {
	if cancel, ok := d.cancel.Load().(chan struct{}); ok {
		return cancel
	}
	d.cancel.CompareAndSwap(nil, make(chan struct{}))
	return d.cancel.Load().(chan struct{})
}

// Set sets the point in time when the deadline will time out.
// A timeout event is signaled by closing the channel returned by Wait.
// Once a timeout has occurred, the deadline can be refreshed by specifying
// a time value in the future. A zero time value prevents timeout.
func (d *deadline[H]) Set(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	cancel := d.load()
	if d.timer != nil && !d.timer.Stop() {
		<-cancel // Wait for the timer callback to finish and close cancel.
	}
	d.timer = nil

	// Time is zero, then there is no deadline.
	closed := isClosedChan(cancel)
	if t.IsZero() {
		if closed {
			d.cancel.Store(make(chan struct{}))
		}
		return
	}

	// Time in the future, set up a timer to cancel in the future.
	if dur := time.Until(t); dur > 0 {
		if closed {
			cancel = make(chan struct{})
			d.cancel.Store(cancel)
		}
		d.timer = time.AfterFunc(dur, func() {
			d.hook.run() // Run the hook before closing the channel.
			close(cancel)
		})
		return
	}

	// Time in the past, so close immediately.
	if !closed {
		close(cancel)
	}
}

// Wait returns a channel that is closed when the deadline is exceeded.
func (d *deadline[H]) Wait() <-chan struct{} {
	return d.load()
}

// Interface guards
var (
	_ Deadline = (*deadline[noHook])(nil)
	_ Deadline = (*deadline[funcHook])(nil)
)
