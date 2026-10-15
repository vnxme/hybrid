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
	"strings"
	"testing"
	"time"
)

// testInletRelease ensures releasing an inlet connection hands the packets queued and the later packets
// of its flow over to the fallback connection, in order, while the other flows go on as they were.
func testInletRelease(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	client, ln := inletTrunk(t, &InletConfig{FallbackConn: true})
	writeFrom(t, client, a, "a1")
	writeFrom(t, client, a, "a2")
	conn := acceptInletConn(t, ln)
	if err := conn.Release(); err != nil {
		t.Fatalf("Release() = %v, want nil", err)
	}
	writeFrom(t, client, a, "a3")
	writeFrom(t, client, b, "b1")

	if got, want := strings.Join(readFallback(t, ln.Fallback()), " "), a.String()+":a1 "+a.String()+":a2 "+a.String()+":a3"; got != want {
		t.Errorf("packets of the fallback connection = %q, want %q", got, want)
	}
	if other := acceptInletConn(t, ln); other.RemoteAddr().String() != b.String() {
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
	waitInletStats(t, ln, InletStats{})
}

// testInletReleaseWithoutFallback ensures releasing an inlet connection without a fallback connection drops
// the packets of its flow, without starting a new flow.
func testInletReleaseWithoutFallback(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	client, ln := inletTrunk(t, nil)
	if ln.Fallback() != nil {
		t.Fatalf("Fallback() = %v, want nil", ln.Fallback())
	}
	writeFrom(t, client, a, "a1")
	releaseInletConn(t, ln)
	writeFrom(t, client, a, "a2")
	writeFrom(t, client, b, "b1")
	waitInletStats(t, ln, InletStats{NoFallback: 2})
	if conn := acceptInletConn(t, ln); conn.RemoteAddr().String() != b.String() { // Not a new flow of a
		t.Errorf("RemoteAddr() = %s, want %s", conn.RemoteAddr(), b)
	}
}

// testInletFallbackDrops ensures the packets of a released flow are dropped if the queue of the fallback
// connection is full, or if the fallback connection is closed, which ends the released flows, so that
// the next packet of each is offered to Accept again.
func testInletFallbackDrops(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}

	t.Run("QueueFull", func(t *testing.T) {
		client, ln := inletTrunk(t, &InletConfig{FallbackConn: true, FallbackQueueSize: 1})
		writeFrom(t, client, a, "a1")
		releaseInletConn(t, ln)
		writeFrom(t, client, a, "a2")
		writeFrom(t, client, a, "a3")
		waitInletStats(t, ln, InletStats{FallbackQueueFull: 2})
		if got, want := strings.Join(readFallback(t, ln.Fallback()), " "), a.String()+":a1"; got != want {
			t.Errorf("packets of the fallback connection = %q, want %q", got, want)
		}
	})

	t.Run("Closed", func(t *testing.T) {
		client, ln := inletTrunk(t, &InletConfig{FallbackConn: true})
		writeFrom(t, client, a, "a1")
		writeFrom(t, client, b, "b1") // Read after a1, as there is a single read loop
		releaseInletConn(t, ln)
		_ = acceptInletConn(t, ln) // So a1 has reached the fallback connection
		if err := ln.Fallback().Close(); err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}
		writeFrom(t, client, a, "a2") // A new flow, as the released one has ended
		conn := acceptInletConn(t, ln)
		if got := readInletConn(t, conn); conn.RemoteAddr().String() != a.String() || strings.Join(got, " ") != "a2" {
			t.Errorf("RemoteAddr(), packets read = %s, %q, want %s, the latest one only", conn.RemoteAddr(), got, a)
		}
		waitInletStats(t, ln, InletStats{FallbackClosed: 1}) // a1 left unread
	})

	t.Run("ReleasedOnceClosed", func(t *testing.T) {
		client, ln := inletTrunk(t, &InletConfig{FallbackConn: true})
		writeFrom(t, client, a, "a1")
		writeFrom(t, client, b, "b1") // Read after a1, as there is a single read loop
		conn := acceptInletConn(t, ln)
		_ = acceptInletConn(t, ln) // So a1 is queued on conn
		if err := ln.Fallback().Close(); err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}
		if err := conn.Release(); err != nil {
			t.Fatalf("Release() = %v, want nil", err)
		}
		waitInletStats(t, ln, InletStats{NoFallback: 1}) // a1, moved to the closed fallback connection

		writeFrom(t, client, a, "a2") // A new flow, as the flow released has ended straight away
		if got := readInletConn(t, acceptInletConn(t, ln)); strings.Join(got, " ") != "a2" {
			t.Errorf("packets read = %q, want the latest one only", got)
		}
	})
}

// testInletConnCloseDrops ensures the packets left unread on an inlet connection are dropped and counted
// once it's closed by its handler or expires.
func testInletConnCloseDrops(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}

	// queue writes packets of a and accepts the connection of a, then of b, once the packets of a are queued
	queue := func(t *testing.T, client FromToPacketConn, ln InletListener) InletConn {
		for _, data := range []string{"a1", "a2", "a3"} {
			writeFrom(t, client, a, data)
		}
		writeFrom(t, client, b, "b1") // Read after those of a, as there is a single read loop
		conn := acceptInletConn(t, ln)
		_ = acceptInletConn(t, ln)
		return conn
	}

	t.Run("Closed", func(t *testing.T) {
		client, ln := inletTrunk(t, nil)
		conn := queue(t, client, ln)
		_ = conn.Close()
		writeFrom(t, client, b, "b2") // The read loop deletes closed connections after reading
		waitInletStats(t, ln, InletStats{ConnClosed: 3})
	})

	t.Run("Expired", func(t *testing.T) {
		const idle = 50 * time.Millisecond
		client, ln := inletTrunk(t, &InletConfig{IdleTimeout: idle})
		conn := queue(t, client, ln)
		time.Sleep(2 * idle)
		if !isClosedInletConn(conn) {
			t.Fatalf("connection open after the idle timeout, want closed")
		}
		writeFrom(t, client, b, "b2")
		waitInletStats(t, ln, InletStats{ConnClosed: 4}) // b1 too, as the connection of b has expired as well
	})
}

// testInletClosedErrors ensures an inlet connection and the fallback connection report closing as a trunk does,
// and so a UDP connection: as net.ErrClosed, wrapped in net.OpError with the operation and the addresses.
func testInletClosedErrors(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := inletTrunk(t, &InletConfig{FallbackConn: true})
	writeFrom(t, client, a, "a1")
	conn, fb := acceptInletConn(t, ln), ln.Fallback()
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
		{"ReadFrom", func() error { _, _, err := conn.ReadFrom(b); return err }(), "read", conn.LocalAddr(), a},
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

// testInletDeadlines ensures the read and write deadlines of inlet connections and of the fallback connection.
func testInletDeadlines(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := inletTrunk(t, &InletConfig{FallbackConn: true})
	writeFrom(t, client, a, "a1")
	conn := acceptInletConn(t, ln)
	_ = readInletConn(t, conn)

	past := time.Now().Add(-time.Second)
	b := make([]byte, 8)
	for name, err := range map[string]error{
		"ReadFrom":         func() error { _ = conn.SetDeadline(past); _, _, err := conn.ReadFrom(b); return err }(),
		"WriteTo":          func() error { _, err := conn.WriteTo(b, conn.RemoteAddr()); return err }(),
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

// testInletConcurrentReads ensures reads of an inlet connection and of the fallback connection needn't be
// serialized: each packet is read once.
func testInletConcurrentReads(t *testing.T) {
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
		client, ln := inletTrunk(t, &InletConfig{ReadQueueSize: count})
		writeFrom(t, client, a, "p0")
		conn := acceptInletConn(t, ln)
		go write(client, 1)
		readConcurrently(t, conn, 8, count)
		waitInletStats(t, ln, InletStats{})
	})

	t.Run("Fallback", func(t *testing.T) {
		client, ln := inletTrunk(t, &InletConfig{FallbackConn: true, FallbackQueueSize: count})
		writeFrom(t, client, a, "p0")
		releaseInletConn(t, ln)
		go write(client, 1)
		readConcurrently(t, ln.Fallback(), 8, count)
		waitInletStats(t, ln, InletStats{})
	})
}

// testInletShortBuffer ensures a packet longer than the buffer of a read is read in part, with
// io.ErrShortBuffer, and the rest of it dropped, from an inlet connection and the fallback connection.
func testInletShortBuffer(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	client, ln := inletTrunk(t, &InletConfig{FallbackConn: true})
	writeFrom(t, client, a, "abcdef")
	writeFrom(t, client, a, "g")
	conn := acceptInletConn(t, ln)
	writeFrom(t, client, b, "hijklm")
	writeFrom(t, client, b, "n")
	releaseInletConn(t, ln)

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
			if n, _, err := test.pc.ReadFrom(buf); err != nil || string(buf[:n]) != test.nxt {
				t.Errorf("ReadFrom() = %q, %v, want %q, nil, the rest of the packet before dropped", buf[:n], err, test.nxt)
			}
		})
	}
}

// testInletReadCopiesAddrs ensures reads of an inlet connection and of the fallback connection return
// addresses the caller may change, as trunks and UDP do, without changing those the flow is written to.
func testInletReadCopiesAddrs(t *testing.T) {
	a, c := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 3), Port: 3}
	local := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 53}
	client, ln := inletTrunk(t, &InletConfig{FallbackConn: true})
	for _, src := range []net.Addr{a, c} {
		if _, err := client.WriteToFrom([]byte("x"), local, src, 3); err != nil {
			t.Fatal(err)
		}
	}
	conn := acceptInletConn(t, ln)
	releaseInletConn(t, ln) // The flow of c, read by the fallback connection

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

// testInletConnWriteToConnected ensures an inlet connection writes to the remote address of its flow only,
// from its local address, failing with net.ErrWriteToConnected otherwise, as a connected UDP socket does,
// and that an IPv4 address matches whether it's written in 4 bytes or 16.
func testInletConnWriteToConnected(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	local := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 53}
	other := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	client, ln := inletTrunk(t, nil)
	if _, err := client.WriteToFrom([]byte("x"), local, a, 0); err != nil {
		t.Fatal(err)
	}
	conn := acceptInletConn(t, ln)
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

// testInletConnCloseAfterStop ensures closing an inlet connection once the inlet has stopped drops and counts
// the packets left unread, though there is no read loop to report the connection to.
func testInletConnCloseAfterStop(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	client, ln := inletTrunk(t, nil)
	writeFrom(t, client, a, "a1")
	writeFrom(t, client, a, "a2")
	writeFrom(t, client, b, "b1") // Read after those of a, as there is a single read loop
	conn := acceptInletConn(t, ln)
	_ = acceptInletConn(t, ln)

	if err := ln.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close() of the connection = %v, want nil", err)
	}
	if got, want := ln.Stats(), (InletStats{ConnClosed: 2}); got != want {
		t.Errorf("Stats() = %+v, want %+v", got, want)
	}
}
