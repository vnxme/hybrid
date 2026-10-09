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

//go:build !(aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris || windows || zos)

package pktinfo

import (
	"errors"
	"testing"
)

// TestUnsupported ensures enabling packet info and reading IPV6_V6ONLY fail with errors.ErrUnsupported
// on platforms without packet info.
func TestUnsupported(t *testing.T) {
	for _, only := range []struct{ v4, v6 bool }{{false, false}, {true, false}, {false, true}} {
		if err := Enable(nil, only.v4, only.v6); !errors.Is(err, errors.ErrUnsupported) {
			t.Errorf("Enable(nil, %v, %v) = %v, want errors.ErrUnsupported", only.v4, only.v6, err)
		}
	}
	if _, err := IsV6Only(nil); !errors.Is(err, errors.ErrUnsupported) {
		t.Errorf("IsV6Only(nil) = _, %v, want errors.ErrUnsupported", err)
	}
}
