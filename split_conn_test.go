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

package hybrid

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// testSplitRelease ensures releasing a split connection hands the packets given, copied, the packets queued
// and the later packets of its flow over to the fallback connection, in order, while the other flows go on
// as they were.
func testSplitRelease(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	client, ln := splitTrunk(t, &SplitConfig{FallbackConn: true})
	writeFrom(t, client, a, "a1")
	writeFrom(t, client, a, "a2")
	writeFrom(t, client, a, "a3")
	conn := acceptSplitConn(t, ln)
	read := make([][]byte, 2) // As a handler matching the flow would
	for i := range read {
		read[i] = make([]byte, 8)
		n, err := conn.Read(read[i])
		if err != nil {
			t.Fatalf("Read() = %v, want nil", err)
		}
		read[i] = read[i][:n]
	}
	if err := conn.Release(read...); err != nil {
		t.Fatalf("Release() = %v, want nil", err)
	}
	copy(read[0], "xx") // Released packets are copied
	writeFrom(t, client, a, "a4")
	writeFrom(t, client, b, "b1")

	want := strings.Join([]string{a.String() + ":a1", a.String() + ":a2", a.String() + ":a3", a.String() + ":a4"}, " ")
	if got := strings.Join(readFallback(t, ln.Fallback()), " "); got != want {
		t.Errorf("packets of the fallback connection = %q, want %q", got, want)
	}
	if other := acceptSplitConn(t, ln); other.RemoteAddr().String() != b.String() {
		t.Errorf("RemoteAddr() = %s, want %s", other.RemoteAddr(), b)
	}
	if _, _, err := conn.ReadFrom(make([]byte, 8)); err == nil {
		t.Errorf("Read() after Release() = nil, want an error")
	}
	if err := conn.Release(); err == nil {
		t.Errorf("Release() again = nil, want an error")
	}
	if err := conn.Close(); err == nil {
		t.Errorf("Close() after Release() = nil, want an error")
	}
	waitSplitStats(t, ln, SplitStats{})
}

// testSplitReleaseWithoutFallback ensures releasing a split connection without a fallback connection drops
// and counts the packets given and those of its flow, without starting a new flow.
func testSplitReleaseWithoutFallback(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	client, ln := splitTrunk(t, nil)
	if ln.Fallback() != nil {
		t.Fatalf("Fallback() = %v, want nil", ln.Fallback())
	}
	writeFrom(t, client, a, "a1")
	writeFrom(t, client, a, "a2")
	conn := acceptSplitConn(t, ln)
	checkSplitPacket(t, conn, "a1")
	if err := conn.Release([]byte("a1")); err != nil {
		t.Fatalf("Release() = %v, want nil", err)
	}
	writeFrom(t, client, a, "a3")
	writeFrom(t, client, b, "b1")
	waitSplitStats(t, ln, SplitStats{NoFallback: 3})
	if conn := acceptSplitConn(t, ln); conn.RemoteAddr().String() != b.String() { // Not a new flow of a
		t.Errorf("RemoteAddr() = %s, want %s", conn.RemoteAddr(), b)
	}
}

// testSplitFallbackDrops ensures the packets of a released flow are dropped if the queue of the fallback
// connection is full, or if the fallback connection is closed, which ends the released flows, so that
// the next packet of each is offered to Accept again.
func testSplitFallbackDrops(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}

	t.Run("QueueFull", func(t *testing.T) {
		client, ln := splitTrunk(t, &SplitConfig{FallbackConn: true, FallbackQueueSize: 1})
		writeFrom(t, client, a, "a1")
		releaseSplitConn(t, ln)
		writeFrom(t, client, a, "a2")
		writeFrom(t, client, a, "a3")
		waitSplitStats(t, ln, SplitStats{FallbackQueueFull: 2})
		if got, want := strings.Join(readFallback(t, ln.Fallback()), " "), a.String()+":a1"; got != want {
			t.Errorf("packets of the fallback connection = %q, want %q", got, want)
		}
	})

	t.Run("Closed", func(t *testing.T) {
		client, ln := splitTrunk(t, &SplitConfig{FallbackConn: true})
		writeFrom(t, client, a, "a1")
		writeFrom(t, client, b, "b1") // Read after a1, as there is a single read loop
		releaseSplitConn(t, ln)
		_ = acceptSplitConn(t, ln) // So a1 has reached the fallback connection
		if err := ln.Fallback().Close(); err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}
		writeFrom(t, client, a, "a2") // A new flow, as the released one has ended
		conn := acceptSplitConn(t, ln)
		if got := readSplitConn(t, conn); conn.RemoteAddr().String() != a.String() || strings.Join(got, " ") != "a2" {
			t.Errorf("RemoteAddr(), packets read = %s, %q, want %s, the latest one only", conn.RemoteAddr(), got, a)
		}
		waitSplitStats(t, ln, SplitStats{FallbackClosed: 1}) // a1 left unread
	})

	t.Run("ReleasedOnceClosed", func(t *testing.T) {
		client, ln := splitTrunk(t, &SplitConfig{FallbackConn: true})
		writeFrom(t, client, a, "a1")
		writeFrom(t, client, b, "b1") // Read after a1, as there is a single read loop
		conn := acceptSplitConn(t, ln)
		_ = acceptSplitConn(t, ln) // So a1 is queued on conn
		if err := ln.Fallback().Close(); err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}
		if err := conn.Release(); err != nil {
			t.Fatalf("Release() = %v, want nil", err)
		}
		waitSplitStats(t, ln, SplitStats{NoFallback: 1}) // a1, moved to the closed fallback connection

		writeFrom(t, client, a, "a2") // A new flow, as the flow released has ended straight away
		if got := readSplitConn(t, acceptSplitConn(t, ln)); strings.Join(got, " ") != "a2" {
			t.Errorf("packets read = %q, want the latest one only", got)
		}
	})
}

// testSplitConnCloseDrops ensures the packets left unread on a split connection are dropped and counted
// once it's closed by its handler or expires, including when the splitter stops before the read loop deletes it,
// or before Accept returns it.
func testSplitConnCloseDrops(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}

	// queue writes packets of a and accepts the connection of a, then of b, once the packets of a are queued
	queue := func(t *testing.T, client FromToPacketConn, ln SplitListener) SplitConn {
		for _, data := range []string{"a1", "a2", "a3"} {
			writeFrom(t, client, a, data)
		}
		writeFrom(t, client, b, "b1") // Read after those of a, as there is a single read loop
		conn := acceptSplitConn(t, ln)
		_ = acceptSplitConn(t, ln)
		return conn
	}

	t.Run("Closed", func(t *testing.T) {
		client, ln := splitTrunk(t, nil)
		conn := queue(t, client, ln)
		_ = conn.Close()
		writeFrom(t, client, b, "b2") // The read loop deletes closed connections after reading
		waitSplitStats(t, ln, SplitStats{ConnClosed: 3})
	})

	t.Run("Expired", func(t *testing.T) {
		const idle = 50 * time.Millisecond
		client, ln := splitTrunk(t, &SplitConfig{IdleTimeout: idle})
		conn := queue(t, client, ln)
		time.Sleep(2 * idle)
		if !isClosedSplitConn(conn) {
			t.Fatalf("connection open after the idle timeout, want closed")
		}
		writeFrom(t, client, b, "b2")
		waitSplitStats(t, ln, SplitStats{ConnClosed: 4}) // b1 too, as the connection of b has expired as well
	})

	t.Run("ClosedBeforeStop", func(t *testing.T) {
		client, ln := splitTrunk(t, nil)
		conn := queue(t, client, ln)
		_ = conn.Close() // Left to the read loop, which reads no packet to delete it after
		_ = ln.Close()
		if got, want := ln.Stats(), (SplitStats{ConnClosed: 3}); got != want {
			t.Errorf("Stats() = %+v, want %+v", got, want)
		}
	})

	t.Run("UnacceptedAtStop", func(t *testing.T) {
		client, ln := splitTrunk(t, nil)
		writeFrom(t, client, a, "a1")
		writeFrom(t, client, b, "b1")
		writeFrom(t, client, b, "b2")
		writeFrom(t, client, a, "a2") // Read after those of b, as there is a single read loop
		conn := acceptSplitConn(t, ln)
		checkSplitPacket(t, conn, "a1")
		checkSplitPacket(t, conn, "a2")
		_ = ln.Close() // Closes the connection of b, which Accept no longer returns
		if got, want := ln.Stats(), (SplitStats{ConnClosed: 2}); got != want {
			t.Errorf("Stats() = %+v, want %+v", got, want)
		}
	})
}

// testSplitClosedErrors ensures a split connection and the fallback connection report closing as a trunk does,
// and so a UDP connection: as net.ErrClosed, wrapped in net.OpError with the operation and the addresses.
func testSplitClosedErrors(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := splitTrunk(t, &SplitConfig{FallbackConn: true})
	writeFrom(t, client, a, "a1")
	conn, fb := acceptSplitConn(t, ln), ln.Fallback()
	if err := conn.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
	if err := fb.Close(); err != nil {
		t.Fatalf("Close() of the fallback connection = %v, want nil", err)
	}

	str := func(a net.Addr) string { return fmt.Sprint(a) } // Tells a nil address from any other
	b := make([]byte, 8)
	for _, test := range []struct {
		name         string
		err          error
		op           string
		source, addr net.Addr
	}{
		{"Read", func() error { _, err := conn.Read(b); return err }(), "read", conn.LocalAddr(), a},
		{"ReadFrom", func() error { _, _, err := conn.ReadFrom(b); return err }(), "read", conn.LocalAddr(), a},
		{"Write", func() error { _, err := conn.Write(b); return err }(), "write", conn.LocalAddr(), a},
		{"WriteTo", func() error { _, err := conn.WriteTo(b, a); return err }(), "write", conn.LocalAddr(), a},
		{"Close", conn.Close(), "close", conn.LocalAddr(), a},
		{"Release", conn.Release(), "release", conn.LocalAddr(), a},
		{"SetDeadline", conn.SetDeadline(time.Time{}), "set", nil, conn.LocalAddr()},
		{"FallbackReadFrom", func() error { _, _, err := fb.ReadFrom(b); return err }(), "read", fb.LocalAddr(), nil},
		{"FallbackWriteTo", func() error { _, err := fb.WriteTo(b, a); return err }(), "write", fb.LocalAddr(), a},
		{"FallbackClose", fb.Close(), "close", fb.LocalAddr(), nil},
		{"FallbackSetDeadline", fb.SetDeadline(time.Time{}), "set", nil, fb.LocalAddr()},
		{"SetReadDeadline", conn.SetReadDeadline(time.Time{}), "set", nil, conn.LocalAddr()},
		{"SetWriteDeadline", conn.SetWriteDeadline(time.Time{}), "set", nil, conn.LocalAddr()},
		{"FallbackSetReadDeadline", fb.SetReadDeadline(time.Time{}), "set", nil, fb.LocalAddr()},
		{"FallbackSetWriteDeadline", fb.SetWriteDeadline(time.Time{}), "set", nil, fb.LocalAddr()},
	} {
		var oe *net.OpError
		if !errors.As(test.err, &oe) || !errors.Is(test.err, net.ErrClosed) || oe.Op != test.op ||
			oe.Net != ln.Addr().Network() || str(oe.Source) != str(test.source) || str(oe.Addr) != str(test.addr) {
			t.Errorf("%s() = %#v, want a %q net.OpError from %v to %v of net.ErrClosed", test.name, test.err, test.op, test.source, test.addr)
		}
	}
}

// testSplitDeadlines ensures the read and write deadlines of split connections and of the fallback connection
// make reads and writes fail once exceeded, and let a waiting read time out.
func testSplitDeadlines(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := splitTrunk(t, &SplitConfig{FallbackConn: true})
	writeFrom(t, client, a, "a1")
	conn := acceptSplitConn(t, ln)
	_ = readSplitConn(t, conn)

	past := time.Now().Add(-time.Second)
	b := make([]byte, 8)
	for name, err := range map[string]error{
		"ReadFrom":         func() error { _ = conn.SetDeadline(past); _, _, err := conn.ReadFrom(b); return err }(),
		"WriteTo":          func() error { _, err := conn.WriteTo(b, conn.RemoteAddr()); return err }(),
		"Read":             func() error { _, err := conn.Read(b); return err }(),
		"Write":            func() error { _, err := conn.Write(b); return err }(),
		"FallbackReadFrom": func() error { _ = ln.Fallback().SetDeadline(past); _, _, err := ln.Fallback().ReadFrom(b); return err }(),
		"FallbackWriteTo": func() error {
			_ = ln.Fallback().SetWriteDeadline(past)
			_, err := ln.Fallback().WriteTo(b, a)
			return err
		}(),
	} {
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("%s() after the deadline = %v, want os.ErrDeadlineExceeded", name, err)
		}
	}

	// A deadline in the future lets a waiting read time out
	_ = ln.Fallback().SetDeadline(time.Now().Add(20 * time.Millisecond))
	if _, _, err := ln.Fallback().ReadFrom(b); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("ReadFrom() of the fallback connection = %v, want os.ErrDeadlineExceeded", err)
	}
	if err := ln.Fallback().Close(); err != nil {
		t.Fatal(err)
	}
	if err := ln.Fallback().Close(); err == nil {
		t.Errorf("Close() of the fallback connection again = nil, want an error")
	}
	if _, _, err := ln.Fallback().ReadFrom(b); err == nil {
		t.Errorf("ReadFrom() of the closed fallback connection = nil, want an error")
	}
}

// testSplitConcurrentReads ensures reads of a split connection and of the fallback connection needn't be
// serialized: each packet is read once.
func testSplitConcurrentReads(t *testing.T) {
	const count = 1000
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	write := func(client FromToPacketConn, from int) {
		for i := from; i < count; i++ {
			if _, err := client.WriteToFrom(fmt.Appendf(nil, "p%d", i), nil, a, 0); err != nil {
				return
			}
		}
	}

	t.Run("Conn", func(t *testing.T) {
		client, ln := splitTrunk(t, &SplitConfig{ReadQueueSize: count})
		writeFrom(t, client, a, "p0")
		conn := acceptSplitConn(t, ln)
		go write(client, 1)
		readConcurrently(t, conn, 8, count)
		waitSplitStats(t, ln, SplitStats{})
	})

	t.Run("Fallback", func(t *testing.T) {
		client, ln := splitTrunk(t, &SplitConfig{FallbackConn: true, FallbackQueueSize: count})
		writeFrom(t, client, a, "p0")
		releaseSplitConn(t, ln)
		go write(client, 1)
		readConcurrently(t, ln.Fallback(), 8, count)
		waitSplitStats(t, ln, SplitStats{})
	})
}

// testSplitShortBuffer ensures a packet longer than the buffer of a read is read in part, with
// io.ErrShortBuffer as a temporary net.Error, so that a server, e.g. of QUIC, goes on reading,
// and the rest of it dropped, from a split connection and the fallback connection.
func testSplitShortBuffer(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	client, ln := splitTrunk(t, &SplitConfig{FallbackConn: true})
	writeFrom(t, client, a, "abcdef")
	writeFrom(t, client, a, "g")
	conn := acceptSplitConn(t, ln)
	writeFrom(t, client, b, "hijklm")
	writeFrom(t, client, b, "n")
	releaseSplitConn(t, ln)

	for _, test := range []struct {
		name     string
		pc       FromToPacketConn
		src      net.Addr
		out, nxt string
	}{
		{"Conn", conn, a, "abc", "g"},
		{"Fallback", ln.Fallback(), b, "hij", "n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_ = test.pc.SetReadDeadline(time.Now().Add(time.Second))
			buf := make([]byte, 3)
			n, src, _, _, err := test.pc.ReadFromTo(buf)
			var oe *net.OpError
			if string(buf[:n]) != test.out || src.String() != test.src.String() || !errors.Is(err, io.ErrShortBuffer) ||
				!errors.As(err, &oe) || oe.Op != "read" || oe.Addr.String() != test.src.String() {
				t.Errorf("ReadFromTo() = %q from %v, %v, want %q from %s, a read net.OpError of io.ErrShortBuffer", buf[:n], src, err, test.out, test.src)
			}
			if ne, ok := err.(net.Error); !ok || !ne.Temporary() || ne.Timeout() { // Checked as quic-go does
				t.Errorf("ReadFromTo() = %v, want a temporary net.Error, but no timeout", err)
			}
			if n, _, err := test.pc.ReadFrom(buf); err != nil || string(buf[:n]) != test.nxt {
				t.Errorf("ReadFrom() = %q, %v, want %q, nil, the rest of the packet before dropped", buf[:n], err, test.nxt)
			}
		})
	}
}

// testSplitShortBufferReadsOn ensures a server reading as quic-go does, into a buffer of 1452 bytes, and stopping
// on a read error unless it is temporary, skips a longer packet of a split connection or of the fallback connection,
// and reads the next one, so that a remote address can't stop it with a single long packet.
func testSplitShortBufferReadsOn(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	client, ln := splitTrunk(t, &SplitConfig{FallbackConn: true})
	long := strings.Repeat("x", 2000)
	writeFrom(t, client, a, "first")
	conn := acceptSplitConn(t, ln)
	writeFrom(t, client, a, long)
	writeFrom(t, client, a, "last")
	for _, data := range []string{"first", long, "last"} {
		writeFrom(t, client, b, data)
	}
	releaseSplitConn(t, ln)

	for _, test := range []struct {
		name string
		pc   PacketConn
	}{
		{"Conn", conn},
		{"Fallback", ln.Fallback()},
	} {
		t.Run(test.name, func(t *testing.T) {
			_ = test.pc.SetReadDeadline(time.Now().Add(time.Second))
			var pkts []string
			for skipped := 0; len(pkts) < 2; {
				buf := make([]byte, 1452)
				n, _, err := test.pc.ReadFrom(buf)
				if ne, ok := err.(net.Error); ok && ne.Temporary() {
					skipped++
					continue
				}
				if err != nil || skipped > 1 {
					t.Fatalf("ReadFrom() = %v after %d packets skipped, want nil after 1 at most", err, skipped)
				}
				pkts = append(pkts, string(buf[:n]))
			}
			if !slices.Equal(pkts, []string{"first", "last"}) {
				t.Errorf("packets read = %q, want %q", pkts, []string{"first", "last"})
			}
		})
	}
}

// testSplitReadCopiesAddrs ensures reads of a split connection and of the fallback connection return
// addresses the caller may change, as trunks and UDP do, without changing those the flow is written to.
func testSplitReadCopiesAddrs(t *testing.T) {
	a, c := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 3), Port: 3}
	local := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 53}
	client, ln := splitTrunk(t, &SplitConfig{FallbackConn: true})
	for _, src := range []net.Addr{a, c} {
		if _, err := client.WriteToFrom([]byte("x"), local, src, 3); err != nil {
			t.Fatal(err)
		}
	}
	conn := acceptSplitConn(t, ln)
	releaseSplitConn(t, ln) // The flow of c, read by the fallback connection

	change := func(addrs ...net.Addr) {
		for _, addr := range addrs {
			u := addr.(*net.UDPAddr)
			u.IP[len(u.IP)-1], u.Port = 99, 99
		}
	}
	b := make([]byte, 8)
	for _, test := range []struct {
		name string
		pc   FromToPacketConn
		src  *net.UDPAddr
	}{
		{"Conn", conn, a},
		{"Fallback", ln.Fallback(), c},
	} {
		t.Run(test.name, func(t *testing.T) {
			_ = test.pc.SetReadDeadline(time.Now().Add(time.Second))
			_, src, dst, _, err := test.pc.ReadFromTo(b)
			if err != nil || src.String() != test.src.String() || dst.String() != local.String() {
				t.Fatalf("ReadFromTo() = from %v to %v, %v, want from %s to %s, nil", src, dst, err, test.src, local)
			}
			change(src, dst)
			if _, err := test.pc.WriteTo([]byte("y"), &net.UDPAddr{IP: test.src.IP, Port: test.src.Port}); err != nil {
				t.Fatalf("WriteTo() = %v, want nil", err)
			}
			_ = client.SetReadDeadline(time.Now().Add(time.Second))
			if _, from, to, _, err := client.ReadFromTo(b); err != nil || to.String() != test.src.String() || from.String() != local.String() {
				t.Errorf("ReadFromTo() = from %v to %v, %v, want from %s to %s, nil", from, to, err, local, test.src)
			}
		})
	}
	if conn.RemoteAddr().String() != a.String() || conn.LocalAddr().String() != local.String() {
		t.Errorf("RemoteAddr(), LocalAddr() = %v, %v, want %s, %s", conn.RemoteAddr(), conn.LocalAddr(), a, local)
	}
}

// testSplitConnWriteToConnected ensures a split connection writes to the remote address of its flow only,
// from its local address, failing with net.ErrWriteToConnected otherwise, as a connected UDP socket does,
// and that an IPv4 address matches whether it's written in 4 bytes or 16.
func testSplitConnWriteToConnected(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	local := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 53}
	other := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	client, ln := splitTrunk(t, nil)
	if _, err := client.WriteToFrom([]byte("x"), local, a, 0); err != nil {
		t.Fatal(err)
	}
	conn := acceptSplitConn(t, ln)
	a4, local4 := &net.UDPAddr{IP: a.IP.To4(), Port: a.Port}, &net.UDPAddr{IP: local.IP.To4(), Port: local.Port}

	for _, test := range []struct {
		name     string
		dst, src net.Addr
		ok       bool
	}{
		{"None", nil, nil, true},
		{"Remote", a, nil, true},
		{"Remote4", a4, nil, true},
		{"Local4", nil, local4, true},
		{"OtherRemote", other, nil, false},
		{"OtherLocal", nil, other, false},
	} {
		_, err := conn.WriteToFrom([]byte("y"), test.dst, test.src, 5)
		var oe *net.OpError
		if test.ok && err != nil || !test.ok && (!errors.Is(err, net.ErrWriteToConnected) || !errors.As(err, &oe) || oe.Op != "write") {
			t.Errorf("%s: WriteToFrom() = %v, want %v", test.name, err, map[bool]string{true: "nil", false: "a write net.OpError of net.ErrWriteToConnected"}[test.ok])
		}
	}
}

// testSplitConnCloseAfterStop ensures closing a split connection once the splitter has stopped drops and counts
// the packets left unread, though there is no read loop to report the connection to.
func testSplitConnCloseAfterStop(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	client, ln := splitTrunk(t, nil)
	writeFrom(t, client, a, "a1")
	writeFrom(t, client, a, "a2")
	writeFrom(t, client, b, "b1") // Read after those of a, as there is a single read loop
	conn := acceptSplitConn(t, ln)
	_ = acceptSplitConn(t, ln)

	if err := ln.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close() of the connection = %v, want nil", err)
	}
	if got, want := ln.Stats(), (SplitStats{ConnClosed: 2}); got != want {
		t.Errorf("Stats() = %+v, want %+v", got, want)
	}
}
