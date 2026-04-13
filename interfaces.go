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

import "time"

// Deadline is an abstraction for handling timeouts.
type Deadline interface {
	// Set sets the point in time when the deadline will time out.
	// A timeout event is signaled by closing the channel returned by Wait.
	// Once a timeout has occurred, the deadline can be refreshed by specifying
	// a time value in the future. A zero time value prevents timeout.
	Set(t time.Time)
	// Wait returns a channel that is closed when the deadline is exceeded.
	Wait() chan struct{}
}
