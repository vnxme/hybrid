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

// Major changes to the standard deadline implementation:
// - deadlines can run a hook when a timeout has occurred

package hybrid

import (
	"sync"
	"time"
)

// hookDeadline is an abstraction for handling timeouts.
// Once a timeout has occurred, it calls a hook, unless it is nil.
type hookDeadline struct {
	hook   func()
	timer  *time.Timer
	cancel chan struct{} // Must be non-nil
	mu     sync.Mutex    // Guards timer and cancel
}

// HookDeadline returns a pointer to the struct that satisfies the Deadline interface.
// Unless the hook argument is nil, it is run when the timer fires before the channel is closed.
func HookDeadline(hook func()) Deadline {
	return &hookDeadline{cancel: make(chan struct{}), hook: hook}
}

// Set sets the point in time when the deadline will time out.
// A timeout event is signaled by closing the channel returned by Wait.
// Once a timeout has occurred, the deadline can be refreshed by specifying
// a time value in the future. A zero time value prevents timeout.
func (d *hookDeadline) Set(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.timer != nil && !d.timer.Stop() {
		<-d.cancel // Wait for the timer callback to finish and close cancel.
	}
	d.timer = nil

	// Time is zero, then there is no deadline.
	closed := isClosedChan(d.cancel)
	if t.IsZero() {
		if closed {
			d.cancel = make(chan struct{})
		}
		return
	}

	// Time in the future, set up a timer to cancel in the future.
	if dur := time.Until(t); dur > 0 {
		if closed {
			d.cancel = make(chan struct{})
		}
		d.timer = time.AfterFunc(dur, func() {
			if d.hook != nil {
				d.hook() // Call the hook before closing the channel.
			}
			close(d.cancel)
		})
		return
	}

	// Time in the past, so close immediately.
	if !closed {
		close(d.cancel)
	}
}

// Wait returns a channel that is closed when the deadline is exceeded.
func (d *hookDeadline) Wait() chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cancel
}

// Interface guards
var _ Deadline = (*hookDeadline)(nil)
