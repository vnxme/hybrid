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
// Source: https://github.com/golang/go/blob/go1.25.5/src/net/pipe_test.go
// License: https://github.com/golang/go/blob/go1.25.5/LICENSE
// The original copyright is preserved below.

// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hybrid

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"golang.org/x/net/nettest"
)

type pipeCtor func() (net.Conn, net.Conn)

func TestPipe(t *testing.T) {
	t.Run("StandardUnbufferedSync", func(t *testing.T) {
		ctor := net.Pipe
		t.Run(testPipeBundleName, func(t *testing.T) { testPipeBundle(t, ctor) })
		t.Run(testPipeCloseErrorName, func(t *testing.T) { testPipeCloseError(t, ctor) })
		t.Run(testPipeEmptyAddrName, func(t *testing.T) { testPipeEmptyAddr(t, ctor) })
	})
	t.Run("HybridUnbufferedSync", func(t *testing.T) {
		ctor := Pipe
		t.Run(testPipeBundleName, func(t *testing.T) { testPipeBundle(t, ctor) })
		t.Run(testPipeCloseErrorName, func(t *testing.T) { testPipeCloseError(t, ctor) })
		t.Run(testPipeEmptyAddrName, func(t *testing.T) { testPipeEmptyAddr(t, ctor) })
	})
}

func testPipeBundle(t *testing.T, ctor pipeCtor) {
	t.Helper()
	nettest.TestConn(t, func() (c1, c2 net.Conn, stop func(), err error) {
		c1, c2 = ctor()
		stop = func() {
			_ = c1.Close()
			_ = c2.Close()
		}
		return
	})
}

func testPipeCloseError(t *testing.T, ctor pipeCtor) {
	t.Helper()
	c1, c2 := ctor()
	_ = c1.Close()

	if _, err := c1.Read(nil); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("c1.Read() = %v, want io.ErrClosedPipe", err)
	}
	if _, err := c1.Write(nil); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("c1.Write() = %v, want io.ErrClosedPipe", err)
	}
	if err := c1.SetDeadline(time.Time{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("c1.SetDeadline() = %v, want io.ErrClosedPipe", err)
	}
	if _, err := c2.Read(nil); !errors.Is(err, io.EOF) {
		t.Errorf("c2.Read() = %v, want io.EOF", err)
	}
	if _, err := c2.Write(nil); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("c2.Write() = %v, want io.ErrClosedPipe", err)
	}
	if err := c2.SetDeadline(time.Time{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("c2.SetDeadline() = %v, want io.ErrClosedPipe", err)
	}
}

func testPipeEmptyAddr(t *testing.T, ctor pipeCtor) {
	t.Helper()
	c1, c2 := ctor()

	l1 := c1.LocalAddr()
	if l1 == nil {
		t.Errorf("c1.LocalAddr() = nil, want non-nil")
	} else {
		if len(l1.Network()) == 0 {
			t.Errorf("c1.LocalAddr().Network() is empty, want non-empty")
		}
		if len(l1.String()) == 0 {
			t.Errorf("c1.LocalAddr().String() is empty, want non-empty")
		}
	}

	r1 := c1.RemoteAddr()
	if r1 == nil {
		t.Errorf("c1.RemoteAddr() = nil, want non-nil")
	} else {
		if len(r1.Network()) == 0 {
			t.Errorf("c1.RemoteAddr().Network() is empty, want non-empty")
		}
		if len(r1.String()) == 0 {
			t.Errorf("c1.RemoteAddr().String() is empty, want non-empty")
		}
	}

	l2 := c2.LocalAddr()
	if l2 == nil {
		t.Errorf("c2.LocalAddr() = nil, want non-nil")
	} else {
		if len(l2.Network()) == 0 {
			t.Errorf("c2.LocalAddr().Network() is empty, want non-empty")
		}
		if len(l2.String()) == 0 {
			t.Errorf("c2.LocalAddr().String() is empty, want non-empty")
		}
	}

	r2 := c2.RemoteAddr()
	if r2 == nil {
		t.Errorf("c2.RemoteAddr() = nil, want non-nil")
	} else {
		if len(r2.Network()) == 0 {
			t.Errorf("c2.RemoteAddr().Network() is empty, want non-empty")
		}
		if len(r2.String()) == 0 {
			t.Errorf("c2.RemoteAddr().String() is empty, want non-empty")
		}
	}

	_ = c1.Close()
	_ = c2.Close()
}

const (
	testPipeBundleName     = "Bundle"
	testPipeCloseErrorName = "CloseError"
	testPipeEmptyAddrName  = "EmptyAddr"
)
