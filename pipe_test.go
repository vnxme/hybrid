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

// Major changes to the standard pipe implementation:
// - read and write channels can be buffered or unbuffered
// - pipes can be sync, semi-sync or async

package hybrid

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/nettest"
)

type pipeCtor func() (StreamConn, StreamConn)

func BenchmarkPipe(b *testing.B) {
	benchmarks := []struct {
		name string
		ctor pipeCtor
	}{
		{"StandardUnbufferedSync", net.Pipe},
		{"HybridUnbufferedSync", func() (StreamConn, StreamConn) { return UnbufferedPipe(false) }},
		{"HybridUnbufferedSemi", func() (StreamConn, StreamConn) { return UnbufferedPipe(true) }},
		{"HybridBufferedAsync", func() (StreamConn, StreamConn) { return BufferedPipe(10) }},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.Run(benchmarkPipeChunkedReadName, func(b *testing.B) { benchmarkPipeChunkedRead(b, benchmark.ctor) })
			b.Run(benchmarkPipeChunkedWriteName, func(b *testing.B) { benchmarkPipeChunkedWrite(b, benchmark.ctor) })
		})
	}
}

func benchmarkPipeChunkedRead(b *testing.B, ctor pipeCtor) {
	c1, c2 := ctor()

	bWrite := make([]byte, 1<<10)
	rand.New(rand.NewSource(0)).Read(bWrite)

	bRead := make([]byte, 1<<5)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for {
			_, err := c1.Write(bWrite)
			if err != nil {
				break
			}
		}
		_ = c1.Close()
		wg.Done()
	}()

	go func() {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, err := c2.Read(bRead)
			if err != nil {
				break
			}
		}
		b.StopTimer()
		_ = c2.Close()
		wg.Done()
	}()

	wg.Wait()
}

func benchmarkPipeChunkedWrite(b *testing.B, ctor pipeCtor) {
	c1, c2 := ctor()

	bWrite := make([]byte, 1<<5)
	rand.New(rand.NewSource(0)).Read(bWrite)

	bRead := make([]byte, 1<<10)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			_, err := c1.Write(bWrite)
			if err != nil {
				break
			}
		}
		b.StopTimer()
		_ = c1.Close()
		wg.Done()
	}()

	go func() {
		for {
			_, err := c2.Read(bRead)
			if err != nil {
				break
			}
		}
		_ = c2.Close()
		wg.Done()
	}()

	wg.Wait()
}

func TestPipe(t *testing.T) {
	tests := []struct {
		name string
		ctor pipeCtor
	}{
		{"StandardUnbufferedSync", net.Pipe},
		{"HybridUnbufferedSync", func() (StreamConn, StreamConn) { return UnbufferedPipe(false) }},
		{"HybridUnbufferedSemi", func() (StreamConn, StreamConn) { return UnbufferedPipe(true) }},
		{"HybridBufferedAsync", func() (StreamConn, StreamConn) { return BufferedPipe(10) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run(testPipeBundleName, func(t *testing.T) { testPipeBundle(t, test.ctor) })
			t.Run(testPipeCloseErrorName, func(t *testing.T) { testPipeCloseError(t, test.ctor) })
			t.Run(testPipeConcurrentReadName, func(t *testing.T) { testPipeConcurrentRead(t, test.ctor) })
			t.Run(testPipeEmptyAddrName, func(t *testing.T) { testPipeEmptyAddr(t, test.ctor) })
			t.Run(testPipeSliceAliasName, func(t *testing.T) { testPipeSliceAlias(t, test.ctor) })
			t.Run(testPipeZeroLengthName, func(t *testing.T) { testPipeZeroLength(t, test.ctor) })
		})
	}
}

func TestBufferedPipePanic(t *testing.T) {
	for _, queueSize := range []int{0, -1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("BufferedPipe(%d) didn't panic", queueSize)
				}
			}()
			BufferedPipe(queueSize)
		}()
	}
}

func testPipeBundle(t *testing.T, ctor pipeCtor) {
	nettest.TestConn(t, func() (c1, c2 StreamConn, stop func(), err error) {
		c1, c2 = ctor()
		stop = func() {
			_ = c1.Close()
			_ = c2.Close()
		}
		return
	})
}

func testPipeCloseError(t *testing.T, ctor pipeCtor) {
	c1, c2 := ctor()
	_ = c1.Close()

	b := make([]byte, 1) // Zero-length reads differ, see testPipeZeroLength
	if _, err := c1.Read(b); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("c1.Read() = %v, want io.ErrClosedPipe", err)
	}
	if _, err := c1.Write(nil); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("c1.Write() = %v, want io.ErrClosedPipe", err)
	}
	if err := c1.SetDeadline(time.Time{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("c1.SetDeadline() = %v, want io.ErrClosedPipe", err)
	}
	if _, err := c2.Read(b); !errors.Is(err, io.EOF) {
		t.Errorf("c2.Read() = %v, want io.EOF", err)
	}
	if _, err := c2.Write(nil); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("c2.Write() = %v, want io.ErrClosedPipe", err)
	}
	if err := c2.SetWriteDeadline(time.Time{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("c2.SetWriteDeadline() = %v, want io.ErrClosedPipe", err)
	}

	// Unlike net.Pipe, a hybrid pipe allows setting read deadlines after remote closure
	var want error
	if _, ok := c2.(*pipe); !ok {
		want = io.ErrClosedPipe
	}
	if err := c2.SetReadDeadline(time.Time{}); !errors.Is(err, want) {
		t.Errorf("c2.SetReadDeadline() = %v, want %v", err, want)
	}
	if err := c2.SetDeadline(time.Time{}); !errors.Is(err, want) {
		t.Errorf("c2.SetDeadline() = %v, want %v", err, want)
	}

	_ = c2.Close()
	if err := c2.SetReadDeadline(time.Time{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("c2.SetReadDeadline() = %v, want io.ErrClosedPipe", err)
	}
}

// testPipeConcurrentRead ensures concurrent partial reads neither lose bytes nor race.
func testPipeConcurrentRead(t *testing.T, ctor pipeCtor) {
	c1, c2 := ctor()

	const writes, size = 1 << 10, 7
	go func() {
		for range writes {
			if _, err := c1.Write(make([]byte, size)); err != nil {
				t.Errorf("c1.Write() = %v, want nil", err)
				break
			}
		}
		_ = c1.Close()
	}()

	var total atomic.Int64
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			b := make([]byte, 3)
			for {
				n, err := c2.Read(b)
				total.Add(int64(n))
				if err != nil {
					if !errors.Is(err, io.EOF) {
						t.Errorf("c2.Read() = %v, want io.EOF", err)
					}
					return
				}
			}
		})
	}
	wg.Wait()
	_ = c2.Close()

	if got := total.Load(); got != writes*size {
		t.Errorf("read %d bytes, want %d", got, writes*size)
	}
}

func testPipeEmptyAddr(t *testing.T, ctor pipeCtor) {
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

func testPipeSliceAlias(t *testing.T, ctor pipeCtor) {
	c1, c2 := ctor()

	bSource := make([]byte, 1<<10)
	rand.New(rand.NewSource(0)).Read(bSource)

	bChange := make([]byte, 1<<5)
	rand.New(rand.NewSource(0)).Read(bChange)

	bWrite := bytes.Clone(bSource)
	bRead := make([]byte, len(bWrite))

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		nw, err := c1.Write(bWrite)
		if err != nil {
			t.Errorf("c1.Write() = %v, want nil", err)
		} else {
			if nw != len(bWrite) {
				t.Errorf("c1.Write() = %d, want %d", nw, len(bWrite))
			} else {
				copy(bWrite[len(bWrite)-len(bChange):], bChange)
			}
		}
		wg.Done()
	}()

	go func() {
		c := 1 << 3
		d := len(bRead) >> 1
		f := func(low, high, chunk int) {
			for i := low; i < high; i += chunk {
				nr, err := c2.Read(bRead[i : i+chunk])
				if err != nil {
					t.Errorf("c2.Read() = %v, want nil", err)
					break
				} else {
					if nr != chunk {
						t.Errorf("c2.Read() = %d, want %d", nr, chunk)
						break
					}
				}

			}
		}

		f(0, d, c)
		time.Sleep(50 * time.Millisecond)
		f(d, len(bRead), c)

		wg.Done()
	}()

	wg.Wait()
	err := c1.Close()
	if err != nil {
		t.Errorf("c1.Close() = %v, want nil", err)
	}
	err = c2.Close()
	if err != nil {
		t.Errorf("c2.Close() = %v, want nil", err)
	}

	if !bytes.Equal(bSource, bRead) {
		t.Errorf("transmitted data differs")
	}
}

// testPipeZeroLength ensures zero-length reads and writes behave like TCP ones:
// they return immediately, and no zero-length write is ever observed by a read.
func testPipeZeroLength(t *testing.T, ctor pipeCtor) {
	c1, c2 := ctor()
	if _, ok := c1.(*pipe); !ok {
		t.Skip("net.Pipe matches a zero-length write with a read")
	}

	// Fail instead of hanging, should a zero-length operation wait for the other end
	_ = c1.SetDeadline(time.Now().Add(time.Second))
	_ = c2.SetDeadline(time.Now().Add(time.Second))

	if n, err := c1.Write(nil); n != 0 || err != nil {
		t.Errorf("c1.Write() = %d, %v, want 0, nil", n, err)
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		if _, err := c1.Write([]byte{1}); err != nil {
			t.Errorf("c1.Write() = %v, want nil", err)
		}
	})
	if n, err := c2.Read(make([]byte, 2)); n != 1 || err != nil {
		t.Errorf("c2.Read() = %d, %v, want 1, nil", n, err)
	}
	wg.Wait()
	if n, err := c2.Read(nil); n != 0 || err != nil {
		t.Errorf("c2.Read() = %d, %v, want 0, nil", n, err)
	}

	// Like on Unix, a deadline fails a zero-length write but not a zero-length read
	_ = c1.SetDeadline(time.Now().Add(-time.Second))
	if _, err := c1.Write(nil); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("c1.Write() = %v, want os.ErrDeadlineExceeded", err)
	}
	if n, err := c1.Read(nil); n != 0 || err != nil {
		t.Errorf("c1.Read() = %d, %v, want 0, nil", n, err)
	}

	// Closure fails a zero-length write, but remote closure doesn't fail a zero-length read
	_ = c1.Close()
	if _, err := c1.Write(nil); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("c1.Write() = %v, want io.ErrClosedPipe", err)
	}
	if _, err := c1.Read(nil); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("c1.Read() = %v, want io.ErrClosedPipe", err)
	}
	if _, err := c2.Write(nil); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("c2.Write() = %v, want io.ErrClosedPipe", err)
	}
	if n, err := c2.Read(nil); n != 0 || err != nil {
		t.Errorf("c2.Read() = %d, %v, want 0, nil", n, err)
	}
	_ = c2.Close()
}

const (
	benchmarkPipeChunkedReadName  = "ChunkedRead"
	benchmarkPipeChunkedWriteName = "ChunkedWrite"

	testPipeBundleName         = "Bundle"
	testPipeCloseErrorName     = "CloseError"
	testPipeConcurrentReadName = "ConcurrentRead"
	testPipeEmptyAddrName      = "EmptyAddr"
	testPipeSliceAliasName     = "SliceAlias"
	testPipeZeroLengthName     = "ZeroLength"
)
