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
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type trunkCtor func(addr1, addr2 net.Addr) (SelectivePacketConn, SelectivePacketConn)

const (
	trunkQueueSize = 10

	trunkBlock = 100 * time.Millisecond // Time to detect an operation blocks
	trunkLimit = time.Second            // Time to fail instead of hanging
)

// trunkWrite writes a packet on c in a separate goroutine, so that an unbuffered trunk
// doesn't block until the packet is read. The returned function waits for the write
// to return and checks it has succeeded.
func trunkWrite(t *testing.T, c SelectivePacketConn, payload string, dst, src net.Addr, idx int) (wait func()) {
	done := make(chan error, 1)
	go func() {
		_, err := c.WriteToFrom([]byte(payload), dst, src, idx)
		done <- err
	}()
	return func() {
		t.Helper()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("WriteToFrom(%q) = %v, want nil", payload, err)
			}
		case <-time.After(trunkLimit):
			t.Errorf("WriteToFrom(%q) doesn't return", payload)
		}
	}
}

func TestTrunk(t *testing.T) {
	tests := []struct {
		name     string
		ctor     trunkCtor
		buffered bool
	}{
		{"Unbuffered", UnbufferedTrunk, false},
		{"Buffered", func(addr1, addr2 net.Addr) (SelectivePacketConn, SelectivePacketConn) {
			return BufferedTrunk(trunkQueueSize, addr1, addr2)
		}, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run(testTrunkAddrsName, func(t *testing.T) { testTrunkAddrs(t, test.ctor) })
			t.Run(testTrunkCloseName, func(t *testing.T) { testTrunkClose(t, test.ctor) })
			t.Run(testTrunkConcurrentName, func(t *testing.T) { testTrunkConcurrent(t, test.ctor) })
			t.Run(testTrunkDeadlineName, func(t *testing.T) { testTrunkDeadline(t, test.ctor, test.buffered) })
			t.Run(testTrunkDrainName, func(t *testing.T) { testTrunkDrain(t, test.ctor, test.buffered) })
			t.Run(testTrunkEmptyPacketName, func(t *testing.T) { testTrunkEmptyPacket(t, test.ctor) })
			t.Run(testTrunkErrorsName, func(t *testing.T) { testTrunkErrors(t, test.ctor) })
			t.Run(testTrunkShortBufferName, func(t *testing.T) { testTrunkShortBuffer(t, test.ctor) })
			t.Run(testTrunkSliceAliasName, func(t *testing.T) { testTrunkSliceAlias(t, test.ctor) })
			t.Run(testTrunkUnblockName, func(t *testing.T) { testTrunkUnblock(t, test.ctor, test.buffered) })
			t.Run(testTrunkWriteReturnName, func(t *testing.T) { testTrunkWriteReturn(t, test.ctor, test.buffered) })
		})
	}
}

func TestBufferedTrunkPanic(t *testing.T) {
	for _, queueSize := range []int{0, -1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("BufferedTrunk(%d) didn't panic", queueSize)
				}
			}()
			BufferedTrunk(queueSize, nil, nil)
		}()
	}
}

func TestCloneAddr(t *testing.T) {
	ip := func() net.IP { return net.IPv4(192, 0, 2, 1).To4() }
	for _, addr := range []net.Addr{
		&net.UDPAddr{IP: ip(), Port: 1, Zone: "z"},
		&net.TCPAddr{IP: ip(), Port: 2, Zone: "z"},
		&net.IPAddr{IP: ip(), Zone: "z"},
		&net.UnixAddr{Name: "/tmp/sock", Net: "unixgram"},
	} {
		c := cloneAddr(addr)
		if c == addr || !equalNetAddr(c, addr) {
			t.Errorf("cloneAddr(%v) = %v, want an equal copy", addr, c)
		}
		want := addr.String()
		switch a := c.(type) {
		case *net.UDPAddr:
			a.IP[3] = 99
		case *net.TCPAddr:
			a.IP[3] = 99
		case *net.IPAddr:
			a.IP[3] = 99
		case *net.UnixAddr:
			a.Name = "/tmp/other"
		}
		if addr.String() != want {
			t.Errorf("modifying a copy changed the original to %v, want %v", addr, want)
		}
	}

	// Other addresses, including nil ones, are returned as is
	for _, addr := range []net.Addr{nil, (*net.UDPAddr)(nil), oneTrunkAddr, pipeAddr{}} {
		if c := cloneAddr(addr); c != addr {
			t.Errorf("cloneAddr(%#v) = %#v, want it as is", addr, c)
		}
	}
}

// testTrunkAddrs ensures each end of a trunk has its own local address used by default,
// multiple packet flows sharing a trunk are told apart by addresses, and addresses
// aren't shared by both ends.
func testTrunkAddrs(t *testing.T, ctor trunkCtor) {
	addr1 := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 53}
	addr2 := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 53}
	flow1 := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1001}
	flow2 := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 1002}

	c1, c2 := ctor(nil, nil)
	for name, addr := range map[string]net.Addr{"c1": c1.LocalAddr(), "c2": c2.LocalAddr()} {
		if addr == nil || addr.Network() != trunkNetwork {
			t.Errorf("%s.LocalAddr() = %v, want a %s address", name, addr, trunkNetwork)
		}
	}
	_ = c1.Close()
	_ = c2.Close()

	c1, c2 = ctor(addr1, addr2)
	defer func() {
		_ = c1.Close()
		_ = c2.Close()
	}()
	if got := c1.LocalAddr(); got != addr1 {
		t.Errorf("c1.LocalAddr() = %v, want %v", got, addr1)
	}
	if got := c2.LocalAddr(); got != addr2 {
		t.Errorf("c2.LocalAddr() = %v, want %v", got, addr2)
	}

	// transfer writes a packet on w, reads it on r and waits for the write to return
	transfer := func(w, r SelectivePacketConn, payload string, dst, src net.Addr, idx int) (net.Addr, net.Addr) {
		t.Helper()
		wait := trunkWrite(t, w, payload, dst, src, idx)
		_ = r.SetReadDeadline(time.Now().Add(trunkLimit))
		b := make([]byte, 16)
		n, gotSrc, gotDst, gotIdx, err := r.ReadFromTo(b)
		if err != nil || string(b[:n]) != payload || gotIdx != idx {
			t.Errorf("ReadFromTo() = %q, %d, %v, want %q, %d, nil", b[:n], gotIdx, err, payload, idx)
		}
		wait()
		return gotSrc, gotDst
	}

	// expect fails unless the addresses got are equal to the ones wanted
	expect := func(src, dst, wantSrc, wantDst net.Addr) {
		t.Helper()
		if !equalNetAddr(src, wantSrc) || !equalNetAddr(dst, wantDst) {
			t.Errorf("ReadFromTo() = %v, %v, want %v, %v", src, dst, wantSrc, wantDst)
		}
	}

	// Without addresses, a packet goes from one local address to the other
	src, dst := transfer(c1, c2, "a", nil, nil, 0)
	expect(src, dst, addr1, addr2)

	// Interleaved flows keep their addresses and interface indices
	src, dst = transfer(c1, c2, "b", addr2, flow1, 1)
	expect(src, dst, flow1, addr2)
	src, dst = transfer(c1, c2, "c", addr2, flow2, 2)
	expect(src, dst, flow2, addr2)
	src, dst = transfer(c1, c2, "d", addr2, flow1, 1)
	expect(src, dst, flow1, addr2)

	// A reply by means of PacketConn reaches the flow it is addressed to
	src, dst = transfer(c2, c1, "e", flow2, nil, 0)
	expect(src, dst, addr2, flow2)

	// A writer modifying addresses once a write has returned doesn't affect the packet read
	from := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 3).To4(), Port: 1003}
	src, _ = transfer(c1, c2, "f", addr2, from, 0)
	from.IP[3], from.Port = 99, 9999
	if src.String() != "192.0.2.3:1003" {
		t.Errorf("ReadFromTo() src = %v, want 192.0.2.3:1003", src)
	}

	// A reader modifying addresses doesn't affect local addresses
	src, dst = transfer(c1, c2, "g", nil, nil, 0)
	srcUDP, ok1 := src.(*net.UDPAddr)
	dstUDP, ok2 := dst.(*net.UDPAddr)
	if !ok1 || !ok2 || src == c1.LocalAddr() || dst == c2.LocalAddr() {
		t.Errorf("ReadFromTo() = %#v, %#v, want copies of local addresses", src, dst)
	} else {
		srcUDP.Port, dstUDP.Port = 1, 2
		expect(c1.LocalAddr(), c2.LocalAddr(), addr1, addr2)
	}

	// No addresses are returned on errors
	_ = c2.SetReadDeadline(time.Now().Add(-time.Second))
	if _, src, dst, _, err := c2.ReadFromTo(make([]byte, 16)); err == nil || src != nil || dst != nil {
		t.Errorf("ReadFromTo() = %v, %v, %v, want nil, nil, an error", src, dst, err)
	}
}

// testTrunkClose ensures only the first closing of a trunk end succeeds, even if concurrent.
func testTrunkClose(t *testing.T, ctor trunkCtor) {
	c1, c2 := ctor(nil, nil)
	if err := c2.Close(); err != nil {
		t.Errorf("c2.Close() = %v, want nil", err)
	}
	if err := c1.Close(); err != nil {
		t.Errorf("c1.Close() after remote closure = %v, want nil", err)
	}
	if err := c1.Close(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("c1.Close() after local closure = %v, want %v", err, net.ErrClosed)
	}

	c1, c2 = ctor(nil, nil)
	defer func() { _ = c2.Close() }()
	const closers = 20
	var succeeded atomic.Int32
	var wg sync.WaitGroup
	for range closers {
		wg.Go(func() {
			switch err := c1.Close(); {
			case err == nil:
				succeeded.Add(1)
			case !errors.Is(err, net.ErrClosed):
				t.Errorf("c1.Close() = %v, want nil or %v", err, net.ErrClosed)
			}
		})
	}
	wg.Wait()
	if n := succeeded.Load(); n != 1 {
		t.Errorf("c1.Close() succeeded %d times, want 1", n)
	}
}

// testTrunkConcurrent ensures concurrent writers and readers neither lose, duplicate nor corrupt
// packets, while every writer reuses its buffer right after a write has returned.
func testTrunkConcurrent(t *testing.T, ctor trunkCtor) {
	c1, c2 := ctor(nil, nil)
	const writers, readers, packets, size = 4, 4, 1 << 8, 1 << 12

	// A packet starts with its writer and sequence numbers followed by the latter repeated
	var wwg sync.WaitGroup
	for w := range writers {
		wwg.Go(func() {
			b := make([]byte, size)
			for seq := range packets {
				b[0] = byte(w)
				for i := 1; i < size; i++ {
					b[i] = byte(seq)
				}
				if _, err := c1.WriteTo(b, nil); err != nil {
					if !errors.Is(err, net.ErrClosed) { // Closed once readers have failed
						t.Errorf("c1.WriteTo() = %v, want nil", err)
					}
					return
				}
				clear(b)
			}
		})
	}

	var mu sync.Mutex
	seen := make(map[[2]byte]bool)
	var rwg sync.WaitGroup
	for range readers {
		rwg.Go(func() {
			b := make([]byte, size)
			for {
				_ = c2.SetReadDeadline(time.Now().Add(trunkBlock))
				n, _, err := c2.ReadFrom(b)
				if errors.Is(err, os.ErrDeadlineExceeded) {
					return // All packets have been read
				}
				if err != nil || n != size || bytes.Count(b[1:n], b[1:2]) != n-1 {
					t.Errorf("c2.ReadFrom() = %d, %v, want an intact packet of %d bytes", n, err, size)
					return
				}
				key := [2]byte{b[0], b[1]}
				mu.Lock()
				if seen[key] {
					t.Errorf("packet %v is read twice", key)
				}
				seen[key] = true
				mu.Unlock()
			}
		})
	}

	rwg.Wait()
	_ = c2.Close() // Unblock writers, should readers have failed
	wwg.Wait()
	_ = c1.Close()
	if len(seen) != writers*packets {
		t.Errorf("read %d packets, want %d", len(seen), writers*packets)
	}
}

// testTrunkDeadline ensures an exceeded read deadline prevails over a pending packet, even if
// the remote end is closed, while remote closure prevails over an exceeded write deadline.
func testTrunkDeadline(t *testing.T, ctor trunkCtor, buffered bool) {
	b := make([]byte, 16)

	c1, c2 := ctor(nil, nil)
	_ = c2.SetReadDeadline(time.Now().Add(-time.Second))
	if _, _, err := c2.ReadFrom(b); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("c2.ReadFrom() = %v, want %v", err, os.ErrDeadlineExceeded)
	}

	// A packet is queued in a buffered trunk, while its write is blocked in an unbuffered one
	wait := trunkWrite(t, c1, "a", nil, nil, 0)
	if buffered {
		wait()
	} else {
		time.Sleep(trunkBlock) // Let the write block, though the read outcome doesn't depend on it
	}
	if _, _, err := c2.ReadFrom(b); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("c2.ReadFrom() with a packet pending = %v, want %v", err, os.ErrDeadlineExceeded)
	}
	_ = c2.SetReadDeadline(time.Now().Add(trunkLimit))
	if n, _, err := c2.ReadFrom(b); err != nil || string(b[:n]) != "a" {
		t.Errorf("c2.ReadFrom() = %q, %v, want %q, nil", b[:n], err, "a")
	}
	if !buffered {
		wait()
	}

	if buffered { // Only a buffered trunk may have a packet pending after remote closure
		wait = trunkWrite(t, c1, "b", nil, nil, 0)
		wait()
		_ = c1.Close()
		_ = c2.SetReadDeadline(time.Now().Add(-time.Second))
		if _, _, err := c2.ReadFrom(b); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("c2.ReadFrom() with a packet pending after remote closure = %v, want %v", err, os.ErrDeadlineExceeded)
		}
	}
	_ = c1.Close()
	_ = c2.Close()

	c1, c2 = ctor(nil, nil)
	_ = c1.SetWriteDeadline(time.Now().Add(-time.Second))
	if _, err := c1.WriteTo(b, nil); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("c1.WriteTo() = %v, want %v", err, os.ErrDeadlineExceeded)
	}
	_ = c2.Close()
	if _, err := c1.WriteTo(b, nil); !errors.Is(err, net.ErrClosed) {
		t.Errorf("c1.WriteTo() after remote closure = %v, want %v", err, net.ErrClosed)
	}
	_ = c1.Close()
}

// testTrunkDrain ensures all packets written before remote closure are read before net.ErrClosed.
// A buffered trunk is also drained after remote closure under a read deadline.
func testTrunkDrain(t *testing.T, ctor trunkCtor, buffered bool) {
	c1, c2 := ctor(nil, nil)
	_ = c1.SetWriteDeadline(time.Now().Add(trunkLimit))
	_ = c2.SetReadDeadline(time.Now().Add(trunkLimit))

	payloads := []string{"a", "b", "c"}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, payload := range payloads {
			if _, err := c1.WriteTo([]byte(payload), nil); err != nil {
				t.Errorf("c1.WriteTo() = %v, want nil", err)
				return
			}
		}
		_ = c1.Close()
	}()

	if buffered { // Read after remote closure
		<-done
		if err := c2.SetWriteDeadline(time.Time{}); !errors.Is(err, net.ErrClosed) {
			t.Errorf("c2.SetWriteDeadline() = %v, want %v", err, net.ErrClosed)
		}
		if err := c2.SetDeadline(time.Time{}); err != nil {
			t.Errorf("c2.SetDeadline() = %v, want nil", err)
		}
		if err := c2.SetReadDeadline(time.Now().Add(trunkLimit)); err != nil {
			t.Errorf("c2.SetReadDeadline() = %v, want nil", err)
		}
	}

	b := make([]byte, 16)
	for _, want := range payloads {
		if n, _, err := c2.ReadFrom(b); err != nil || string(b[:n]) != want {
			t.Errorf("c2.ReadFrom() = %q, %v, want %q, nil", b[:n], err, want)
		}
	}
	<-done
	if _, _, err := c2.ReadFrom(b); !errors.Is(err, net.ErrClosed) {
		t.Errorf("c2.ReadFrom() = %v, want %v", err, net.ErrClosed)
	}

	_ = c2.Close()
	if err := c2.SetReadDeadline(time.Time{}); !errors.Is(err, net.ErrClosed) {
		t.Errorf("c2.SetReadDeadline() = %v, want %v", err, net.ErrClosed)
	}
}

// testTrunkEmptyPacket ensures a trunk transfers empty packets, the same way as UDP does.
func testTrunkEmptyPacket(t *testing.T, ctor trunkCtor) {
	c1, c2 := ctor(nil, nil)
	defer func() {
		_ = c1.Close()
		_ = c2.Close()
	}()
	_ = c2.SetReadDeadline(time.Now().Add(trunkLimit))

	for _, b := range [][]byte{nil, make([]byte, 16)} {
		wait := trunkWrite(t, c1, "", nil, nil, 0)
		if n, src, err := c2.ReadFrom(b); n != 0 || src == nil || err != nil {
			t.Errorf("c2.ReadFrom() into %d bytes = %d, %v, %v, want 0, an address, nil", len(b), n, src, err)
		}
		wait()
	}
}

// testTrunkErrors ensures a trunk reports errors the same way as a UDP connection does:
// wrapped in net.OpError, with net.ErrClosed after closure of either end instead of io.EOF.
func testTrunkErrors(t *testing.T, ctor trunkCtor) {
	// expect fails unless err is a net.OpError for op wrapping target
	expect := func(name string, err error, op string, target error) {
		t.Helper()
		var opErr *net.OpError
		if !errors.As(err, &opErr) || opErr.Op != op || !errors.Is(err, target) || errors.Is(err, io.EOF) {
			t.Errorf("%s = %v, want net.OpError for %q wrapping %v", name, err, op, target)
		}
	}
	b := make([]byte, 16)

	c1, c2 := ctor(nil, nil)
	wait := trunkWrite(t, c1, "long payload", nil, nil, 0)
	_, _, err := c2.ReadFrom(b[:4])
	expect("c2.ReadFrom() with a short buffer", err, "read", io.ErrShortBuffer)
	wait()

	_ = c2.SetReadDeadline(time.Now().Add(-time.Second))
	_, _, err = c2.ReadFrom(b)
	expect("c2.ReadFrom() after the deadline", err, "read", os.ErrDeadlineExceeded)
	if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Errorf("c2.ReadFrom() = %v, want a net.Error timing out", err)
	}
	_ = c2.SetReadDeadline(time.Time{})

	_ = c1.Close()
	_, _, err = c2.ReadFrom(b)
	expect("c2.ReadFrom() after remote closure", err, "read", net.ErrClosed)
	_, err = c2.WriteTo(b, nil)
	expect("c2.WriteTo() after remote closure", err, "write", net.ErrClosed)

	_, _, err = c1.ReadFrom(b)
	expect("c1.ReadFrom() after local closure", err, "read", net.ErrClosed)
	_, err = c1.WriteTo(b, nil)
	expect("c1.WriteTo() after local closure", err, "write", net.ErrClosed)
	expect("c1.SetDeadline() after local closure", c1.SetDeadline(time.Time{}), "set", net.ErrClosed)
	expect("c1.SetReadDeadline() after local closure", c1.SetReadDeadline(time.Time{}), "set", net.ErrClosed)
	expect("c1.SetWriteDeadline() after local closure", c1.SetWriteDeadline(time.Time{}), "set", net.ErrClosed)
	expect("c1.Close() after local closure", c1.Close(), "close", net.ErrClosed)
	_ = c2.Close()
}

// testTrunkShortBuffer ensures a packet longer than the read buffer is truncated,
// its remaining bytes are dropped, and its addresses are still returned.
func testTrunkShortBuffer(t *testing.T, ctor trunkCtor) {
	addr1 := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 53}
	c1, c2 := ctor(addr1, nil)
	defer func() {
		_ = c1.Close()
		_ = c2.Close()
	}()
	_ = c2.SetReadDeadline(time.Now().Add(trunkLimit))

	for _, size := range []int{0, 4} {
		wait := trunkWrite(t, c1, "long payload", nil, nil, 0)
		b := make([]byte, size)
		if n, src, err := c2.ReadFrom(b); n != size || !equalNetAddr(src, addr1) || !errors.Is(err, io.ErrShortBuffer) {
			t.Errorf("c2.ReadFrom() into %d bytes = %d, %v, %v, want %d, %v, %v", size, n, src, err, size, addr1, io.ErrShortBuffer)
		}
		wait()
	}

	// The next read returns the next packet, not the remaining bytes of the previous one
	wait := trunkWrite(t, c1, "next", nil, nil, 0)
	b := make([]byte, 16)
	if n, _, err := c2.ReadFrom(b); err != nil || string(b[:n]) != "next" {
		t.Errorf("c2.ReadFrom() = %q, %v, want %q, nil", b[:n], err, "next")
	}
	wait()
}

// testTrunkSliceAlias ensures a trunk doesn't refer to a write buffer once the write has returned,
// so that the caller may reuse it right away, as UDP code commonly does. A regression is likely,
// though not certain, to be detected, as it depends on how reads and writes are scheduled.
func testTrunkSliceAlias(t *testing.T, ctor trunkCtor) {
	c1, c2 := ctor(nil, nil)
	const packets, size = 1 << 10, 1 << 16

	var wg sync.WaitGroup
	wg.Go(func() {
		b := make([]byte, size)
		for range packets {
			clear(b)
			if _, err := c1.WriteTo(b, nil); err != nil {
				if !errors.Is(err, net.ErrClosed) { // Closed once the reader has failed
					t.Errorf("c1.WriteTo() = %v, want nil", err)
				}
				return
			}
			for i := range b {
				b[i] = 0xff
			}
		}
	})

	b := make([]byte, size)
	for i := range packets {
		_ = c2.SetReadDeadline(time.Now().Add(trunkLimit))
		n, _, err := c2.ReadFrom(b)
		if err != nil {
			t.Errorf("c2.ReadFrom() = %v, want nil", err)
			break
		}
		if bytes.IndexByte(b[:n], 0xff) >= 0 {
			t.Errorf("packet %d is modified after its write has returned", i)
			break
		}
	}

	_ = c2.Close()
	_ = c1.Close()
	wg.Wait()
}

// testTrunkUnblock ensures a blocked read or write returns once either end is closed
// or the deadline is exceeded.
func testTrunkUnblock(t *testing.T, ctor trunkCtor, buffered bool) {
	unblockers := []struct {
		name    string
		unblock func(c1, c2 SelectivePacketConn)
		want    error
	}{
		{"LocalClose", func(c1, _ SelectivePacketConn) { _ = c1.Close() }, net.ErrClosed},
		{"RemoteClose", func(_, c2 SelectivePacketConn) { _ = c2.Close() }, net.ErrClosed},
		{"Deadline", func(c1, _ SelectivePacketConn) { _ = c1.SetDeadline(time.Now()) }, os.ErrDeadlineExceeded},
	}

	for _, read := range []bool{true, false} {
		for _, u := range unblockers {
			name := map[bool]string{true: "Read", false: "Write"}[read] + u.name
			t.Run(name, func(t *testing.T) {
				c1, c2 := ctor(nil, nil)
				defer func() {
					_ = c1.Close()
					_ = c2.Close()
				}()
				b := make([]byte, 16)
				if !read && buffered { // Fill the queue for the write to block
					for range trunkQueueSize {
						_, _ = c1.WriteTo(b, nil)
					}
				}

				done := make(chan error, 1)
				go func() {
					var err error
					if read {
						_, _, err = c1.ReadFrom(b)
					} else {
						_, err = c1.WriteTo(b, nil)
					}
					done <- err
				}()
				time.Sleep(trunkBlock) // Let the operation block, though its outcome doesn't depend on it
				u.unblock(c1, c2)

				select {
				case err := <-done:
					if !errors.Is(err, u.want) {
						t.Errorf("c1 operation = %v, want %v", err, u.want)
					}
				case <-time.After(trunkLimit):
					t.Errorf("c1 operation doesn't return")
				}
			})
		}
	}
}

// testTrunkWriteReturn ensures a write returns once its packet has been read in an unbuffered
// trunk, or once it has been queued in a buffered one. A write expected to block is detected
// by exceeding a short deadline.
func testTrunkWriteReturn(t *testing.T, ctor trunkCtor, buffered bool) {
	c1, c2 := ctor(nil, nil)
	defer func() {
		_ = c1.Close()
		_ = c2.Close()
	}()
	_ = c2.SetReadDeadline(time.Now().Add(trunkLimit))

	queued := 0
	if buffered {
		queued = trunkQueueSize
	}
	_ = c1.SetWriteDeadline(time.Now().Add(trunkLimit))
	for i := range queued {
		if n, err := c1.WriteTo([]byte{byte(i)}, nil); n != 1 || err != nil {
			t.Errorf("c1.WriteTo() = %d, %v, want 1, nil", n, err)
		}
	}
	_ = c1.SetWriteDeadline(time.Now().Add(trunkBlock))
	if n, err := c1.WriteTo([]byte{0xff}, nil); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("c1.WriteTo() = %d, %v, want 0, %v", n, err, os.ErrDeadlineExceeded)
	}

	b := make([]byte, 16)
	for i := range queued {
		if n, _, err := c2.ReadFrom(b); err != nil || !bytes.Equal(b[:n], []byte{byte(i)}) {
			t.Errorf("c2.ReadFrom() = %v, %v, want %v, nil", b[:n], err, []byte{byte(i)})
		}
	}

	// Nothing else may be read, including the packet of a write that has timed out
	_ = c2.SetReadDeadline(time.Now().Add(trunkBlock))
	if n, _, err := c2.ReadFrom(b); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("c2.ReadFrom() = %d, %v, want 0, %v", n, err, os.ErrDeadlineExceeded)
	}
}

const (
	testTrunkAddrsName       = "Addrs"
	testTrunkCloseName       = "Close"
	testTrunkConcurrentName  = "Concurrent"
	testTrunkDeadlineName    = "Deadline"
	testTrunkDrainName       = "Drain"
	testTrunkEmptyPacketName = "EmptyPacket"
	testTrunkErrorsName      = "Errors"
	testTrunkShortBufferName = "ShortBuffer"
	testTrunkSliceAliasName  = "SliceAlias"
	testTrunkUnblockName     = "Unblock"
	testTrunkWriteReturnName = "WriteReturn"
)
