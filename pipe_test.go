// Copyright 2025-2026 VNXME
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
		{"HybridBufferedAsync", func() (StreamConn, StreamConn) { return BufferedPipe(pipeQueueSize) }},
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
	defer func() {
		_ = c2.Close() // Make c1.Write fail
		wg.Wait()
	}()
	wg.Go(func() {
		for {
			if _, err := c1.Write(bWrite); err != nil {
				break
			}
		}
		_ = c1.Close()
	})

	b.ReportAllocs()
	for b.Loop() {
		if _, err := c2.Read(bRead); err != nil {
			b.Fatalf("c2.Read() = %v, want nil", err)
		}
	}
}

func benchmarkPipeChunkedWrite(b *testing.B, ctor pipeCtor) {
	c1, c2 := ctor()

	bWrite := make([]byte, 1<<5)
	rand.New(rand.NewSource(0)).Read(bWrite)

	bRead := make([]byte, 1<<10)

	var wg sync.WaitGroup
	defer func() {
		_ = c1.Close() // Make c2.Read fail
		wg.Wait()
	}()
	wg.Go(func() {
		for {
			if _, err := c2.Read(bRead); err != nil {
				break
			}
		}
		_ = c2.Close()
	})

	b.ReportAllocs()
	for b.Loop() {
		if _, err := c1.Write(bWrite); err != nil {
			b.Fatalf("c1.Write() = %v, want nil", err)
		}
	}
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
			t.Run(testPipeCloseName, func(t *testing.T) { testPipeClose(t, test.ctor) })
			t.Run(testPipeCloseErrorName, func(t *testing.T) { testPipeCloseError(t, test.ctor) })
			t.Run(testPipeConcurrentReadName, func(t *testing.T) { testPipeConcurrentRead(t, test.ctor) })
			t.Run(testPipeConcurrentWriteName, func(t *testing.T) { testPipeConcurrentWrite(t, test.ctor) })
			t.Run(testPipeDeadlineName, func(t *testing.T) { testPipeDeadline(t, test.ctor, test.mode) })
			t.Run(testPipeDrainName, func(t *testing.T) { testPipeDrain(t, test.ctor, test.mode) })
			t.Run(testPipeEmptyAddrName, func(t *testing.T) { testPipeEmptyAddr(t, test.ctor) })
			t.Run(testPipeLockWaitName, func(t *testing.T) { testPipeLockWait(t, test.ctor) })
			t.Run(testPipeSliceAliasName, func(t *testing.T) { testPipeSliceAlias(t, test.ctor, test.mode) })
			t.Run(testPipeUnblockName, func(t *testing.T) { testPipeUnblock(t, test.ctor, test.mode) })
			t.Run(testPipeWriteReturnName, func(t *testing.T) { testPipeWriteReturn(t, test.ctor, test.mode) })
			t.Run(testPipeWriteSizesName, func(t *testing.T) { testPipeWriteSizes(t, test.ctor) })
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

// testPipeClose ensures only the first closing of a hybrid pipe end succeeds, even if concurrent,
// while closing a net.Pipe end always succeeds.
func testPipeClose(t *testing.T, ctor pipeCtor) {
	c1, c2 := ctor()
	_, hybrid := c1.(*pipe)
	defer func() { _ = c2.Close() }()

	const closers = 20
	want := closers
	if hybrid {
		want = 1
	}
	if n := closeConcurrently(t, c1, closers); n != want {
		t.Errorf("c1.Close() succeeded %d times, want %d", n, want)
	}
}

// testPipeCloseError ensures closure errors are reported the same way as by net.Pipe, or
// by TCP for a hybrid pipe: net.ErrClosed once the local end is closed, a bare io.EOF on read
// and a broken pipe on write after remote closure, and any other error wrapped in net.OpError.
func testPipeCloseError(t *testing.T, ctor pipeCtor) {
	c1, c2 := ctor()
	_, hybrid := c1.(*pipe)

	// Errors expected after local closure, after remote closure on write and on setting a deadline,
	// and on closing a closed end, where nil means no error is expected
	closed, broken, deadline, reclose := io.ErrClosedPipe, io.ErrClosedPipe, io.ErrClosedPipe, error(nil)
	if hybrid {
		closed, broken, deadline, reclose = net.ErrClosed, errPipeBroken, nil, net.ErrClosed
	}

	// expect fails unless err is target, wrapped in net.OpError for op by a hybrid pipe
	expect := func(name string, err error, op string, target error) {
		t.Helper()
		var opErr *net.OpError
		switch {
		case target == nil:
			if err != nil {
				t.Errorf("%s = %v, want nil", name, err)
			}
		case target == io.EOF:
			if err != io.EOF { // io.EOF must not be wrapped, as callers compare it with ==
				t.Errorf("%s = %#v, want a bare io.EOF", name, err)
			}
		case !errors.Is(err, target):
			t.Errorf("%s = %v, want %v", name, err, target)
		case hybrid && (!errors.As(err, &opErr) || opErr.Op != op):
			t.Errorf("%s = %#v, want net.OpError for %q", name, err, op)
		case hybrid && (op == "set") != (opErr.Source == nil):
			t.Errorf("%s = net.OpError from %v, want from %s", name, opErr.Source, map[bool]string{true: "nil", false: "the local address"}[op == "set"])
		}
	}

	b := make([]byte, 1) // Zero-length reads and writes differ, see testPipeZeroLength
	_ = c1.Close()
	_, err := c1.Read(b)
	expect("c1.Read()", err, "read", closed)
	_, err = c1.Write(b)
	expect("c1.Write()", err, "write", closed)
	expect("c1.SetDeadline()", c1.SetDeadline(time.Time{}), "set", closed)
	expect("c1.SetReadDeadline()", c1.SetReadDeadline(time.Time{}), "set", closed)
	expect("c1.SetWriteDeadline()", c1.SetWriteDeadline(time.Time{}), "set", closed)
	expect("c1.Close()", c1.Close(), "close", reclose)

	_, err = c2.Read(b)
	expect("c2.Read()", err, "read", io.EOF)
	_, err = c2.Write(b)
	expect("c2.Write()", err, "write", broken)
	expect("c2.SetDeadline()", c2.SetDeadline(time.Time{}), "set", deadline)
	expect("c2.SetReadDeadline()", c2.SetReadDeadline(time.Time{}), "set", deadline)
	expect("c2.SetWriteDeadline()", c2.SetWriteDeadline(time.Time{}), "set", deadline)
	expect("c2.Close()", c2.Close(), "close", nil)
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

// testPipeConcurrentWrite ensures concurrent writes aren't interleaved, i.e. each of them is read
// as a contiguous run of bytes, even if reads are shorter than writes.
func testPipeConcurrentWrite(t *testing.T, ctor pipeCtor) {
	c1, c2 := ctor()
	defer func() { _ = c2.Close() }()
	_ = c1.SetWriteDeadline(time.Now().Add(5 * time.Second)) // Fail instead of hanging
	_ = c2.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Every write consists of a byte unique to it
	const writers, writes, size = 4, 64, 1 << 10
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			b := make([]byte, size)
			for i := range writes {
				for j := range b {
					b[j] = byte(w*writes + i)
				}
				if _, err := c1.Write(b); err != nil {
					t.Errorf("c1.Write() = %v, want nil", err)
					return
				}
			}
		})
	}
	go func() {
		wg.Wait()
		_ = c1.Close()
	}()

	var stream []byte
	b := make([]byte, 100)
	for {
		n, err := c2.Read(b)
		stream = append(stream, b[:n]...)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Errorf("c2.Read() = %v, want io.EOF", err)
			}
			break
		}
	}

	if len(stream) != writers*writes*size {
		t.Fatalf("read %d bytes, want %d", len(stream), writers*writes*size)
	}
	seen := make(map[byte]bool)
	for off := 0; off < len(stream); off += size {
		w := stream[off : off+size]
		if bytes.Count(w, w[:1]) != size {
			t.Fatalf("write read at offset %d is interleaved with another one", off)
		}
		if seen[w[0]] {
			t.Fatalf("write %d is read twice", w[0])
		}
		seen[w[0]] = true
	}
}

// testPipeDeadline ensures an exceeded read deadline prevails over pending bytes, even if
// the remote end is closed, while local closure prevails over pending bytes too, and remote
// closure prevails over an exceeded write deadline.
func testPipeDeadline(t *testing.T, ctor pipeCtor, mode pipeMode) {
	c1, c2 := ctor()
	_, hybrid := c1.(*pipe)
	_ = c1.SetWriteDeadline(time.Now().Add(time.Second)) // Fail instead of hanging
	_ = c2.SetReadDeadline(time.Now().Add(time.Second))

	// Bytes are retained or queued by a semi-sync or async pipe, while a sync pipe write is blocked
	done := make(chan error, 1)
	go func() {
		_, err := c1.Write([]byte("abcd"))
		done <- err
	}()
	b := make([]byte, 16)
	if n, err := c2.Read(b[:1]); err != nil || string(b[:n]) != "a" {
		t.Errorf("c2.Read() = %q, %v, want %q, nil", b[:n], err, "a")
	}

	_ = c2.SetReadDeadline(time.Now().Add(-time.Second))
	if _, err := c2.Read(b); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("c2.Read() with bytes pending = %v, want %v", err, os.ErrDeadlineExceeded)
	}
	_ = c2.SetReadDeadline(time.Now().Add(time.Second))
	if n, err := c2.Read(b); err != nil || string(b[:n]) != "bcd" {
		t.Errorf("c2.Read() = %q, %v, want %q, nil", b[:n], err, "bcd")
	}
	if err := <-done; err != nil {
		t.Errorf("c1.Write() = %v, want nil", err)
	}

	if mode != pipeModeSync { // Only a semi-sync or async pipe may have bytes pending after remote closure
		go func() {
			_, err := c1.Write([]byte("efgh"))
			done <- err
		}()
		if n, err := c2.Read(b[:1]); err != nil || string(b[:n]) != "e" {
			t.Errorf("c2.Read() = %q, %v, want %q, nil", b[:n], err, "e")
		}
		if err := <-done; err != nil {
			t.Errorf("c1.Write() = %v, want nil", err)
		}
		_ = c1.Close()

		_ = c2.SetReadDeadline(time.Now().Add(-time.Second))
		if _, err := c2.Read(b); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("c2.Read() with bytes pending after remote closure = %v, want %v", err, os.ErrDeadlineExceeded)
		}
		_ = c2.Close()
		if _, err := c2.Read(b); !errors.Is(err, net.ErrClosed) {
			t.Errorf("c2.Read() with bytes pending after local closure = %v, want %v", err, net.ErrClosed)
		}
	}
	_ = c1.Close()
	_ = c2.Close()

	broken := io.ErrClosedPipe
	if hybrid {
		broken = errPipeBroken
	}
	c1, c2 = ctor()
	_ = c1.SetWriteDeadline(time.Now().Add(-time.Second))
	if _, err := c1.Write(b); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("c1.Write() = %v, want %v", err, os.ErrDeadlineExceeded)
	}
	_ = c2.Close()
	if _, err := c1.Write(b); !errors.Is(err, broken) {
		t.Errorf("c1.Write() after remote closure = %v, want %v", err, broken)
	}
	_ = c1.Close()
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

	for name, addr := range map[string]net.Addr{
		"c1.LocalAddr()":  c1.LocalAddr(),
		"c1.RemoteAddr()": c1.RemoteAddr(),
		"c2.LocalAddr()":  c2.LocalAddr(),
		"c2.RemoteAddr()": c2.RemoteAddr(),
	} {
		switch {
		case addr == nil:
			t.Errorf("%s = nil, want non-nil", name)
		case addr.Network() == "":
			t.Errorf("%s.Network() is empty, want non-empty", name)
		case addr.String() == "":
			t.Errorf("%s.String() is empty, want non-empty", name)
		}
	}

	_ = c1.Close()
	_ = c2.Close()
}

// testPipeLockWait ensures an operation fails if its pipe end is closed or its deadline is exceeded
// while the operation is waiting for another one to finish, even though it is ready to proceed afterwards.
// As select chooses randomly between ready cases, every scenario is repeated to catch a success.
func testPipeLockWait(t *testing.T, ctor pipeCtor) {
	c1, c2 := ctor()
	_, hybrid := c1.(*pipe)
	_ = c1.Close()
	_ = c2.Close()
	if !hybrid {
		t.Skip("relies on the internals of a hybrid pipe")
	}

	scenarios := []struct {
		name  string
		read  bool
		event func(c1, c2 StreamConn)
		want  error
	}{
		{"ReadLocalClose", true, func(c1, _ StreamConn) { _ = c1.Close() }, net.ErrClosed},
		{"ReadDeadline", true, func(c1, _ StreamConn) { _ = c1.SetReadDeadline(time.Now()) }, os.ErrDeadlineExceeded},
		{"ReadDeadlineAfterRemoteClose", true, func(c1, c2 StreamConn) { // Like TCP, an exceeded deadline prevails over io.EOF
			_ = c2.Close()
			_ = c1.SetReadDeadline(time.Now())
		}, os.ErrDeadlineExceeded},
		{"WriteLocalClose", false, func(c1, _ StreamConn) { _ = c1.Close() }, net.ErrClosed},
		{"WriteRemoteClose", false, func(_, c2 StreamConn) { _ = c2.Close() }, errPipeBroken},
		{"WriteDeadline", false, func(c1, _ StreamConn) { _ = c1.SetWriteDeadline(time.Now()) }, os.ErrDeadlineExceeded},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			for range 20 {
				c1, c2 := ctor()
				mu := &c1.(*pipe).wrMu
				if sc.read {
					mu = &c1.(*pipe).rdMu
				}
				mu.Lock() // Another operation is in progress

				done := make(chan error, 1)
				go func() {
					var err error
					if sc.read { // Pass the checks before the lock, then wait for it
						_, err = c1.Read(make([]byte, 1))
					} else {
						_, err = c1.Write([]byte("x"))
					}
					done <- err
				}()
				go func() { // Make the operation ready to proceed in any mode
					if sc.read {
						_, _ = c2.Write([]byte("x"))
					} else {
						_, _ = c2.Read(make([]byte, 1))
					}
				}()
				time.Sleep(time.Millisecond) // Let the operation wait, though its outcome doesn't depend on it
				sc.event(c1, c2)
				mu.Unlock()

				select {
				case err := <-done:
					if !errors.Is(err, sc.want) {
						t.Errorf("c1 operation = %v, want %v", err, sc.want)
					}
				case <-time.After(time.Second):
					t.Errorf("c1 operation doesn't return")
				}
				_ = c1.Close()
				_ = c2.Close()
			}
		})
	}
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

// testPipeUnblock ensures a blocked read or write returns once either end is closed
// or the deadline is exceeded, with the same errors as by net.Pipe, or by TCP for a hybrid pipe.
func testPipeUnblock(t *testing.T, ctor pipeCtor, mode pipeMode) {
	c1, c2 := ctor()
	_, hybrid := c1.(*pipe)
	_ = c1.Close()
	_ = c2.Close()

	closed, broken := io.ErrClosedPipe, io.ErrClosedPipe
	if hybrid {
		closed, broken = net.ErrClosed, errPipeBroken
	}
	unblockers := []struct {
		name      string
		unblock   func(c1, c2 StreamConn)
		wantRead  error
		wantWrite error
	}{
		{"LocalClose", func(c1, _ StreamConn) { _ = c1.Close() }, closed, closed},
		{"RemoteClose", func(_, c2 StreamConn) { _ = c2.Close() }, io.EOF, broken},
		{"Deadline", func(c1, _ StreamConn) { _ = c1.SetDeadline(time.Now()) }, os.ErrDeadlineExceeded, os.ErrDeadlineExceeded},
	}

	for _, read := range []bool{true, false} {
		for _, u := range unblockers {
			name, want := "Read"+u.name, u.wantRead
			if !read {
				name, want = "Write"+u.name, u.wantWrite
			}
			t.Run(name, func(t *testing.T) {
				c1, c2 := ctor()
				defer func() {
					_ = c1.Close()
					_ = c2.Close()
				}()
				b := make([]byte, 16)
				if !read && mode == pipeModeAsync { // Fill the queue for the write to block
					for range pipeQueueSize {
						_, _ = c1.Write(b)
					}
				}

				done := make(chan error, 1)
				go func() {
					var err error
					if read {
						_, err = c1.Read(b)
					} else {
						_, err = c1.Write(b)
					}
					done <- err
				}()
				time.Sleep(100 * time.Millisecond) // Let the operation block, though its outcome doesn't depend on it
				u.unblock(c1, c2)

				select {
				case err := <-done:
					if !errors.Is(err, want) {
						t.Errorf("c1 operation = %v, want %v", err, want)
					}
				case <-time.After(time.Second):
					t.Errorf("c1 operation doesn't return")
				}
			})
		}
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

// testPipeWriteSizes ensures writes of various sizes are read intact in parts, so that bytes are
// retained between reads, including sizes around buffer size classes, longer writes followed by
// shorter ones of the same class, and sizes beyond the pooled ones, so that no bytes of a reused
// buffer leak into a write, and no buffer is reused while its bytes are retained.
func testPipeWriteSizes(t *testing.T, ctor pipeCtor) {
	poisonBufs(t)
	c1, c2 := ctor()
	defer func() {
		_ = c1.Close()
		_ = c2.Close()
	}()

	b := make([]byte, 1000)
	sizes := []int{1, 2, 3, 4, 6, 5, 1536, 1025, 1472, 1537, 2048, 1 << bufPoolMaxBits, 49153, 1<<bufPoolMaxBits + 1}
	for i, size := range sizes {
		want := bytes.Repeat([]byte{byte(i + 1)}, size)
		done := make(chan error, 1)
		go func() {
			_, err := c1.Write(want)
			done <- err
		}()

		_ = c2.SetReadDeadline(time.Now().Add(time.Second))
		got := make([]byte, 0, size)
		for len(got) < size {
			n, err := c2.Read(b[:min(len(b), size-len(got))])
			if err != nil {
				t.Fatalf("c2.Read() after %d of %d bytes = %v, want nil", len(got), size, err)
			}
			got = append(got, b[:n]...)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("c2.Read() of %d bytes = %d bytes not all equal to %d", size, len(got), i+1)
		}

		select {
		case err := <-done:
			if err != nil {
				t.Errorf("c1.Write() of %d bytes = %v, want nil", size, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("c1.Write() of %d bytes doesn't return", size)
		}
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

	// As on Unix, a deadline fails a zero-length write but not a zero-length read
	_ = c1.SetDeadline(time.Now().Add(-time.Second))
	if _, err := c1.Write(nil); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("c1.Write() = %v, want os.ErrDeadlineExceeded", err)
	}
	if n, err := c1.Read(nil); n != 0 || err != nil {
		t.Errorf("c1.Read() = %d, %v, want 0, nil", n, err)
	}

	// Closure fails a zero-length write, but remote closure doesn't fail a zero-length read
	_ = c1.Close()
	if _, err := c1.Write(nil); !errors.Is(err, net.ErrClosed) {
		t.Errorf("c1.Write() = %v, want %v", err, net.ErrClosed)
	}
	if _, err := c1.Read(nil); !errors.Is(err, net.ErrClosed) {
		t.Errorf("c1.Read() = %v, want %v", err, net.ErrClosed)
	}
	if _, err := c2.Write(nil); !errors.Is(err, errPipeBroken) {
		t.Errorf("c2.Write() = %v, want %v", err, errPipeBroken)
	}
	if n, err := c2.Read(nil); n != 0 || err != nil {
		t.Errorf("c2.Read() = %d, %v, want 0, nil", n, err)
	}
	_ = c2.Close()
}

const (
	benchmarkPipeChunkedReadName  = "ChunkedRead"
	benchmarkPipeChunkedWriteName = "ChunkedWrite"

	testPipeBundleName          = "Bundle"
	testPipeCloseName           = "Close"
	testPipeCloseErrorName      = "CloseError"
	testPipeConcurrentReadName  = "ConcurrentRead"
	testPipeConcurrentWriteName = "ConcurrentWrite"
	testPipeDeadlineName        = "Deadline"
	testPipeDrainName           = "Drain"
	testPipeEmptyAddrName       = "EmptyAddr"
	testPipeLockWaitName        = "LockWait"
	testPipeSliceAliasName      = "SliceAlias"
	testPipeUnblockName         = "Unblock"
	testPipeWriteReturnName     = "WriteReturn"
	testPipeWriteSizesName      = "WriteSizes"
	testPipeZeroLengthName      = "ZeroLength"
)
