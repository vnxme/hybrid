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
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// TestInletRelease ensures releasing an inlet connection hands the packets queued and the later packets
// of its flow over to the fallback connection, in order, while the other flows go on as they were.
func TestInletRelease(t *testing.T) {
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

// TestInletReleaseWithoutFallback ensures releasing an inlet connection without a fallback connection drops
// the packets of its flow, without starting a new flow.
func TestInletReleaseWithoutFallback(t *testing.T) {
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

// TestInletReleaseDrops ensures the packets of a released flow are dropped if the queue of the fallback
// connection is full, or if the fallback connection is closed.
func TestInletReleaseDrops(t *testing.T) {
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
		writeFrom(t, client, a, "a2")
		waitInletStats(t, ln, InletStats{NoFallback: 1, FallbackClosed: 1}) // a1 left unread, and a2
	})
}

// TestInletConnClosed ensures the packets left unread on an inlet connection are dropped and counted
// once it's closed by its handler or expires.
func TestInletConnClosed(t *testing.T) {
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

// TestInletConnErrors ensures an inlet connection and the fallback connection report closing as a trunk does,
// and so a UDP connection: as net.ErrClosed, wrapped in net.OpError with the operation and the addresses.
func TestInletConnErrors(t *testing.T) {
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
	} {
		var oe *net.OpError
		if !errors.As(test.err, &oe) || !errors.Is(test.err, net.ErrClosed) || oe.Op != test.op ||
			oe.Net != ln.Addr().Network() || str(oe.Source) != str(test.source) || str(oe.Addr) != str(test.addr) {
			t.Errorf("%s() = %#v, want a %q net.OpError from %v to %v of net.ErrClosed", test.name, test.err, test.op, test.source, test.addr)
		}
	}
}

// TestInletDeadlines ensures the read and write deadlines of inlet connections and of the fallback connection.
func TestInletDeadlines(t *testing.T) {
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
