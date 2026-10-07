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

// pipeMode is the mode a pipe under test is expected to behave in.
type pipeMode int

const (
	pipeModeSync pipeMode = iota
	pipeModeSemiSync
	pipeModeAsync
)

const pipeQueueSize = 10

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
		mode pipeMode
	}{
		{"StandardUnbufferedSync", net.Pipe, pipeModeSync},
		{"HybridUnbufferedSync", func() (StreamConn, StreamConn) { return UnbufferedPipe(false) }, pipeModeSync},
		{"HybridUnbufferedSemi", func() (StreamConn, StreamConn) { return UnbufferedPipe(true) }, pipeModeSemiSync},
		{"HybridBufferedAsync", func() (StreamConn, StreamConn) { return BufferedPipe(pipeQueueSize) }, pipeModeAsync},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run(testPipeBundleName, func(t *testing.T) { testPipeBundle(t, test.ctor) })
			t.Run(testPipeCloseErrorName, func(t *testing.T) { testPipeCloseError(t, test.ctor) })
			t.Run(testPipeConcurrentReadName, func(t *testing.T) { testPipeConcurrentRead(t, test.ctor) })
			t.Run(testPipeDrainName, func(t *testing.T) { testPipeDrain(t, test.ctor, test.mode) })
			t.Run(testPipeEmptyAddrName, func(t *testing.T) { testPipeEmptyAddr(t, test.ctor) })
			t.Run(testPipeSliceAliasName, func(t *testing.T) { testPipeSliceAlias(t, test.ctor, test.mode) })
			t.Run(testPipeWriteReturnName, func(t *testing.T) { testPipeWriteReturn(t, test.ctor, test.mode) })
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

// testPipeDrain ensures all bytes written before remote closure are read before io.EOF.
// Semi-sync and async pipes are also drained after remote closure under a read deadline.
func testPipeDrain(t *testing.T, ctor pipeCtor, mode pipeMode) {
	c1, c2 := ctor()

	// A semi-sync pipe holds at most one write pending, an async one up to its queue size
	writes := 1
	if mode == pipeModeAsync {
		writes = 3
	}

	bWrite := make([]byte, 10)
	rand.New(rand.NewSource(0)).Read(bWrite)

	// Fail instead of hanging
	_ = c1.SetWriteDeadline(time.Now().Add(time.Second))
	_ = c2.SetReadDeadline(time.Now().Add(time.Second))

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range writes {
			if _, err := c1.Write(bWrite); err != nil {
				t.Errorf("c1.Write() = %v, want nil", err)
				return
			}
		}
		_ = c1.Close()
	}()

	var got []byte
	b := make([]byte, 3)
	for waited := false; ; {
		n, err := c2.Read(b)
		got = append(got, b[:n]...)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Errorf("c2.Read() = %v, want io.EOF", err)
			}
			break
		}
		if mode != pipeModeSync && !waited { // Read the rest after remote closure
			<-done
			waited = true
			if err := c2.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Errorf("c2.SetReadDeadline() = %v, want nil", err)
			}
		}
	}
	<-done

	if want := bytes.Repeat(bWrite, writes); !bytes.Equal(got, want) {
		t.Errorf("read %v, want %v", got, want)
	}
	if _, err := c2.Read(b); !errors.Is(err, io.EOF) {
		t.Errorf("c2.Read() = %v, want io.EOF", err)
	}
	_ = c2.Close()
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

// testPipeSliceAlias ensures a pipe doesn't refer to a write buffer once the write has returned,
// so that the caller may modify it. A sync pipe trivially satisfies that, as its write returns
// once all the bytes have been read, while the others must have copied the bytes left unread.
func testPipeSliceAlias(t *testing.T, ctor pipeCtor, mode pipeMode) {
	c1, c2 := ctor()

	// Fail instead of hanging
	_ = c1.SetWriteDeadline(time.Now().Add(time.Second))
	_ = c2.SetReadDeadline(time.Now().Add(time.Second))

	bSource := make([]byte, 1<<10)
	rand.New(rand.NewSource(0)).Read(bSource)

	bWrite := bytes.Clone(bSource)
	bRead := make([]byte, len(bWrite))

	modified := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(modified)
		if nw, err := c1.Write(bWrite); nw != len(bWrite) || err != nil {
			t.Errorf("c1.Write() = %d, %v, want %d, nil", nw, err, len(bWrite))
			return
		}
		for i := range bWrite {
			bWrite[i] ^= 0xff
		}
	})

	// Read the first half in chunks, then the second half once the write buffer has been modified
	const chunk = 1 << 3
	for i := 0; i < len(bRead); i += chunk {
		if i == len(bRead)/2 && mode != pipeModeSync {
			<-modified
		}
		if nr, err := c2.Read(bRead[i : i+chunk]); nr != chunk || err != nil {
			t.Errorf("c2.Read() = %d, %v, want %d, nil", nr, err, chunk)
			break
		}
	}
	wg.Wait()

	if err := c1.Close(); err != nil {
		t.Errorf("c1.Close() = %v, want nil", err)
	}
	if err := c2.Close(); err != nil {
		t.Errorf("c2.Close() = %v, want nil", err)
	}

	if !bytes.Equal(bSource, bRead) {
		t.Errorf("transmitted data differs")
	}
}

// testPipeWriteReturn ensures a write returns once all its bytes have been read in a sync pipe,
// once it has been received by a read in a semi-sync pipe, or once it has been queued in an async pipe.
// A write expected to block is detected by exceeding a short deadline.
func testPipeWriteReturn(t *testing.T, ctor pipeCtor, mode pipeMode) {
	c1, c2 := ctor()
	defer func() {
		_ = c1.Close()
		_ = c2.Close()
	}()
	const block = 100 * time.Millisecond
	_ = c2.SetReadDeadline(time.Now().Add(time.Second)) // Fail instead of hanging

	// read reads up to n bytes from c2 in a separate goroutine
	read := func(n int) <-chan []byte {
		ch := make(chan []byte, 1)
		go func() {
			b := make([]byte, n)
			nr, err := c2.Read(b)
			if err != nil {
				t.Errorf("c2.Read() = %v, want nil", err)
			}
			ch <- b[:nr]
		}()
		return ch
	}

	switch mode {
	case pipeModeSync:
		ch := read(3)
		_ = c1.SetWriteDeadline(time.Now().Add(block))
		if n, err := c1.Write(make([]byte, 10)); n != 3 || !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("c1.Write() = %d, %v, want 3, os.ErrDeadlineExceeded", n, err)
		}
		<-ch
	case pipeModeSemiSync:
		ch := read(3)
		_ = c1.SetWriteDeadline(time.Now().Add(time.Second))
		if n, err := c1.Write([]byte("0123456789")); n != 10 || err != nil {
			t.Errorf("c1.Write() = %d, %v, want 10, nil", n, err)
		}
		<-ch
		_ = c1.SetWriteDeadline(time.Now().Add(block))
		if n, err := c1.Write([]byte("x")); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("c1.Write() = %d, %v, want 0, os.ErrDeadlineExceeded", n, err)
		}
		if b := <-read(16); string(b) != "3456789" {
			t.Errorf("c2.Read() = %q, want %q", b, "3456789")
		}
	case pipeModeAsync:
		_ = c1.SetWriteDeadline(time.Now().Add(time.Second))
		for i := range pipeQueueSize {
			if n, err := c1.Write([]byte{byte(i)}); n != 1 || err != nil {
				t.Errorf("c1.Write() = %d, %v, want 1, nil", n, err)
			}
		}
		_ = c1.SetWriteDeadline(time.Now().Add(block))
		if n, err := c1.Write([]byte{0xff}); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("c1.Write() = %d, %v, want 0, os.ErrDeadlineExceeded", n, err)
		}
		for i := range pipeQueueSize {
			if b := <-read(16); !bytes.Equal(b, []byte{byte(i)}) {
				t.Errorf("c2.Read() = %v, want %v", b, []byte{byte(i)})
			}
		}
	}

	// Nothing else may be read, including the bytes of a write that has timed out
	_ = c2.SetReadDeadline(time.Now().Add(block))
	if n, err := c2.Read(make([]byte, 16)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("c2.Read() = %d, %v, want 0, os.ErrDeadlineExceeded", n, err)
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
	testPipeDrainName          = "Drain"
	testPipeEmptyAddrName      = "EmptyAddr"
	testPipeSliceAliasName     = "SliceAlias"
	testPipeWriteReturnName    = "WriteReturn"
	testPipeZeroLengthName     = "ZeroLength"
)
