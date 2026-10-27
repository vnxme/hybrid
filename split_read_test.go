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
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testSplitTrunkFlows ensures packets over a trunk make a flow per source and destination address, whatever
// their types are, that a flow keeps the latest interface index and replies from its destination address,
// and that a trunk wrapped so as to hide destination addresses makes a flow per source address alone.
func testSplitTrunkFlows(t *testing.T) {
	a, b, c := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}, testAddr("c")
	dst1, dst2, dst3 := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 53}, &net.UDPAddr{IP: net.IPv4(198, 51, 100, 2), Port: 53}, testAddr("d")
	writes := []struct {
		src, dst net.Addr
		idx      int
		data     string
	}{
		{a, dst1, 1, "a1"},
		{b, dst1, 1, "b1"},
		{c, dst1, 1, "c1"},
		{a, dst2, 2, "a2"}, // Another flow of a, to another destination address
		{a, dst3, 3, "a3"}, // And to a destination address of another type
		{a, dst1, 4, "a4"}, // The flow of a1, through another interface
	}

	type flow struct {
		data string
		idx  int
	}
	check := func(t *testing.T, wrap func(PacketConn) PacketConn, key func(SplitConn) string, want map[string]flow) {
		client, server := BufferedTrunk(16, nil, nil)
		defer func() { _ = client.Close() }()
		ln := Split(wrap(server), nil)
		defer func() { _ = ln.Close() }()
		for _, w := range writes {
			if _, err := client.WriteToFrom([]byte(w.data), w.dst, w.src, w.idx); err != nil {
				t.Fatal(err)
			}
		}

		for range want {
			conn := acceptSplitConn(t, ln)
			k := key(conn)
			if got := strings.Join(readSplitConn(t, conn), " "); got != want[k].data {
				t.Errorf("packets of the flow of %s = %q, want %q", k, got, want[k].data)
			}
			if got := conn.(interface{ LocalIfIdx() int }).LocalIfIdx(); got != want[k].idx {
				t.Errorf("LocalIfIdx() of the flow of %s = %d, want %d", k, got, want[k].idx)
			}
			_ = conn.Close()
		}
		waitSplitStats(t, ln, SplitStats{})
	}

	t.Run("BySourceAndDestination", func(t *testing.T) {
		key := func(c SplitConn) string { return c.RemoteAddr().String() + ">" + c.LocalAddr().String() }
		check(t, func(pc PacketConn) PacketConn { return pc }, key, map[string]flow{
			a.String() + ">" + dst1.String(): {"a1 a4", 4},
			a.String() + ">" + dst2.String(): {"a2", 2},
			a.String() + ">" + dst3.String(): {"a3", 3},
			b.String() + ">" + dst1.String(): {"b1", 1},
			c.String() + ">" + dst1.String(): {"c1", 1},
		})
	})

	t.Run("BySource", func(t *testing.T) {
		key := func(c SplitConn) string { return c.RemoteAddr().String() }
		check(t, func(pc PacketConn) PacketConn { return FakeAddressPacketConn(pc, nil, nil) }, key, map[string]flow{
			a.String(): {"a1 a2 a3 a4", 0}, // Hidden by the wrapper, as are destination addresses
			b.String(): {"b1", 0},
			c.String(): {"c1", 0},
		})
	})

	t.Run("Replies", func(t *testing.T) {
		client, server := BufferedTrunk(16, nil, nil)
		defer func() { _ = client.Close() }()
		ln := Split(server, nil)
		defer func() { _ = ln.Close() }()
		for _, dst := range []net.Addr{dst1, dst2} {
			if _, err := client.WriteToFrom([]byte("q"), dst, a, 0); err != nil {
				t.Fatal(err)
			}
			if _, err := acceptSplitConn(t, ln).Write([]byte("r")); err != nil {
				t.Fatal(err)
			}
			_ = client.SetReadDeadline(time.Now().Add(time.Second))
			if _, src, to, _, err := client.ReadFromTo(make([]byte, 8)); err != nil || src.String() != dst.String() || to.String() != a.String() {
				t.Errorf("ReadFromTo() = %v, %v, %v, want %v, %v, nil", src, to, err, dst, a)
			}
		}
	})
}

// scriptedUDPConn is a FromToUDPConn whose reads return the given packets, then wait until it's closed.
// Calling a method it doesn't implement panics.
type scriptedUDPConn struct {
	FromToUDPConn
	pkts chan scriptedPacket
	done chan struct{}
	once sync.Once
}

// scriptedPacket is a packet a scriptedUDPConn reads, with its source and destination addresses.
type scriptedPacket struct {
	src, dst netip.AddrPort
	data     string
}

// newScriptedUDPConn returns a scriptedUDPConn that reads the given packets in order.
func newScriptedUDPConn(pkts ...scriptedPacket) *scriptedUDPConn {
	c := &scriptedUDPConn{pkts: make(chan scriptedPacket, len(pkts)), done: make(chan struct{})}
	for _, p := range pkts {
		c.pkts <- p
	}
	return c
}

// ReadFromToUDPAddrPort returns the next packet, through interface 1, or waits until the connection is closed.
func (c *scriptedUDPConn) ReadFromToUDPAddrPort(b []byte) (int, netip.AddrPort, netip.AddrPort, int, error) {
	select {
	case p := <-c.pkts:
		return copy(b, p.data), p.src, p.dst, 1, nil
	case <-c.done:
		return 0, netip.AddrPort{}, netip.AddrPort{}, 0, net.ErrClosed
	}
}

// Close closes the connection, so that reads fail with net.ErrClosed once the packets are read.
func (c *scriptedUDPConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}

// SetDeadline and SetWriteDeadline do nothing, as reads wait until the connection is closed.
func (c *scriptedUDPConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedUDPConn) SetWriteDeadline(time.Time) error { return nil }

// LocalAddr returns an unspecified address, as of a socket that needs packet info.
func (c *scriptedUDPConn) LocalAddr() net.Addr { return &net.UDPAddr{IP: net.IPv4zero, Port: 53} }

// testSplitPacketInfoFlows ensures a UDP connection that reports destination addresses makes a flow per source
// and destination address, and that a packet without a source or a destination address is dropped and counted.
func testSplitPacketInfoFlows(t *testing.T) {
	s1, s2 := netip.MustParseAddrPort("192.0.2.1:1"), netip.MustParseAddrPort("192.0.2.2:2")
	d1, d2 := netip.MustParseAddrPort("198.51.100.1:53"), netip.MustParseAddrPort("198.51.100.2:53")
	ln := Split(newScriptedUDPConn(
		scriptedPacket{s1, d1, "x"},
		scriptedPacket{s1, d2, "y"}, // Another flow, as it's to another destination address
		scriptedPacket{s1, netip.AddrPort{}, "z"},
		scriptedPacket{netip.AddrPort{}, d1, "u"},
		scriptedPacket{netip.AddrPort{}, netip.AddrPort{}, "t"}, // Counted once, as of no source address
		scriptedPacket{s2, d1, "w"},
		scriptedPacket{s1, d1, "v"},
	), nil)
	defer func() { _ = ln.Close() }()

	want := map[string]string{s1.String() + ">" + d1.String(): "x v", s1.String() + ">" + d2.String(): "y", s2.String() + ">" + d1.String(): "w"}
	for range want {
		conn := acceptSplitConn(t, ln)
		flow := conn.RemoteAddr().String() + ">" + conn.LocalAddr().String()
		if got := strings.Join(readSplitConn(t, conn), " "); got != want[flow] {
			t.Errorf("packets of flow %s = %q, want %q", flow, got, want[flow])
		}
	}
	waitSplitStats(t, ln, SplitStats{NoSource: 2, NoDestination: 1})
}

// testSplitReadDrops ensures the splitter drops and counts a packet rather than wait for a full queue
// or a full table, and that a dropped first packet doesn't create a flow.
func testSplitReadDrops(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	setup := func(t *testing.T, config *SplitConfig) (write func(src net.Addr, data string), ln SplitListener) {
		client, server := BufferedTrunk(16, nil, nil)
		t.Cleanup(func() { _ = client.Close() })
		ln = Split(server, config)
		t.Cleanup(func() { _ = ln.Close() })
		return func(src net.Addr, data string) {
			t.Helper()
			if _, err := client.WriteToFrom([]byte(data), nil, src, 0); err != nil {
				t.Fatal(err)
			}
		}, ln
	}

	t.Run("Truncated", func(t *testing.T) {
		const size = minSplitReadBufferSize
		write, ln := setup(t, &SplitConfig{ReadBufferSize: size})
		write(a, strings.Repeat("x", size+1)) // Fits the read buffer, but is larger than the maximum
		write(a, strings.Repeat("y", 2*size))
		write(a, strings.Repeat("z", size))
		waitSplitStats(t, ln, SplitStats{Truncated: 2})
		checkSplitPacket(t, acceptSplitConn(t, ln), strings.Repeat("z", size)) // The first flow, as truncated packets make none
	})

	t.Run("ConnQueueFull", func(t *testing.T) {
		write, ln := setup(t, &SplitConfig{ReadQueueSize: 1})
		for _, data := range []string{"a1", "a2", "a3"} {
			write(a, data)
		}
		waitSplitStats(t, ln, SplitStats{ConnQueueFull: 2})
		if got := readSplitConn(t, acceptSplitConn(t, ln)); strings.Join(got, " ") != "a1" {
			t.Errorf("packets read = %q, want the first one only", got)
		}
	})

	t.Run("AcceptQueueFull", func(t *testing.T) {
		write, ln := setup(t, &SplitConfig{AcceptQueueSize: 1})
		write(a, "a1")
		write(b, "b1")
		write(b, "b2") // Still a new flow, as the first packet of b hasn't created one
		waitSplitStats(t, ln, SplitStats{AcceptQueueFull: 2})
		if conn := acceptSplitConn(t, ln); conn.RemoteAddr().String() != a.String() {
			t.Fatalf("RemoteAddr() = %s, want %s", conn.RemoteAddr(), a)
		}

		write(b, "b3") // Now there is room in the accept queue
		conn := acceptSplitConn(t, ln)
		if got := readSplitConn(t, conn); conn.RemoteAddr().String() != b.String() || strings.Join(got, " ") != "b3" {
			t.Errorf("RemoteAddr(), packets read = %s, %q, want %s, the latest one only", conn.RemoteAddr(), got, b)
		}
	})

	t.Run("TableFull", func(t *testing.T) {
		write, ln := setup(t, &SplitConfig{MaxFlows: 1})
		write(a, "a1")
		write(b, "b1")
		waitSplitStats(t, ln, SplitStats{TableFull: 1})

		_ = acceptSplitConn(t, ln).Close() // Frees a place in the table
		write(b, "b2")
		conn := acceptSplitConn(t, ln)
		if got := readSplitConn(t, conn); conn.RemoteAddr().String() != b.String() || strings.Join(got, " ") != "b2" {
			t.Errorf("RemoteAddr(), packets read = %s, %q, want %s, the latest one only", conn.RemoteAddr(), got, b)
		}
	})

	t.Run("TableFullEnded", func(t *testing.T) {
		// A flow released to a fallback connection closed since has ended, but nothing has reported it,
		// so the full table is swept for ended flows before a new flow is dropped
		write, ln := setup(t, &SplitConfig{MaxFlows: 1, FallbackConn: true})
		write(a, "a1")
		releaseSplitConn(t, ln)
		_ = readFallback(t, ln.Fallback())
		if err := ln.Fallback().Close(); err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}
		write(b, "b1")
		conn := acceptSplitConn(t, ln)
		if got := readSplitConn(t, conn); conn.RemoteAddr().String() != b.String() || strings.Join(got, " ") != "b1" {
			t.Errorf("RemoteAddr(), packets read = %s, %q, want %s, b1", conn.RemoteAddr(), got, b)
		}
		waitSplitStats(t, ln, SplitStats{})
	})
}

// isClosedSplitConn reports whether a split connection has been closed, e.g. as its flow has expired.
func isClosedSplitConn(c SplitConn) bool { return c.SetDeadline(time.Time{}) != nil }

// keepFlow starts a flow with start, which returns its connection and a function that keeps the flow active,
// calls this function every quarter of idle for three idle timeouts, and returns the connection, or fails if it
// has been closed. The flow may still expire if the goroutines of the test and of the splitter stall, e.g. on
// a busy machine, as the splitter's clock ticks every quarter of idle too, so keepFlow starts the flow anew,
// up to 3 times, if it has expired after a call came over half a period late. A flow that never stays active
// fails every time, whatever the stalls.
func keepFlow(t *testing.T, idle time.Duration, start func() (SplitConn, func())) SplitConn {
	t.Helper()
	period := idle / 4
	for attempt := 1; ; attempt++ {
		last := time.Now() // Before the first packet, which start writes
		conn, touch := start()
		var stalled bool
		for range 12 {
			time.Sleep(period)
			touch()
			now := time.Now()
			stalled = stalled || now.Sub(last) > period+period/2
			last = now
		}
		switch {
		case !isClosedSplitConn(conn):
			return conn
		case stalled && attempt < 3:
			t.Logf("attempt %d stalled, so the flow may have expired; retrying", attempt)
		default:
			t.Fatalf("the connection of an active flow is closed, want open")
		}
	}
}

// testSplitIdleTimeout ensures a flow ends once it's been idle for the idle timeout, released or not,
// so that its next packet starts a new flow, while packets read or written keep it going.
func testSplitIdleTimeout(t *testing.T) {
	const idle = 200 * time.Millisecond // Long enough for the scheduling stalls of a busy machine
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}

	t.Run("Expired", func(t *testing.T) {
		client, ln := splitTrunk(t, &SplitConfig{IdleTimeout: idle})
		writeFrom(t, client, a, "a1")
		conn := acceptSplitConn(t, ln)
		time.Sleep(2 * idle) // Over one and a quarter idle timeouts
		if !isClosedSplitConn(conn) {
			t.Fatalf("the connection of an idle flow is open, want closed")
		}

		writeFrom(t, client, a, "a2")
		if got := readSplitConn(t, acceptSplitConn(t, ln)); strings.Join(got, " ") != "a2" {
			t.Errorf("packets of the new flow = %q, want %q", got, "a2")
		}
	})

	t.Run("KeptByReads", func(t *testing.T) {
		conn := keepFlow(t, idle, func() (SplitConn, func()) {
			client, ln := splitTrunk(t, &SplitConfig{IdleTimeout: idle})
			writeFrom(t, client, a, "a0")
			i := 0
			return acceptSplitConn(t, ln), func() {
				i++
				writeFrom(t, client, a, fmt.Sprintf("a%d", i))
			}
		})
		if got := readSplitConn(t, conn); len(got) != 13 {
			t.Errorf("packets of the flow = %q, want 13 of them", got)
		}
	})

	t.Run("KeptByWrites", func(t *testing.T) {
		keepFlow(t, idle, func() (SplitConn, func()) {
			client, ln := splitTrunk(t, &SplitConfig{IdleTimeout: idle})
			writeFrom(t, client, a, "a0")
			conn := acceptSplitConn(t, ln)
			go func() { // Drain the trunk, so that writes never wait
				b := make([]byte, 8)
				for {
					if _, _, err := client.ReadFrom(b); err != nil {
						return
					}
				}
			}()
			return conn, func() {
				if _, err := conn.WriteTo([]byte("w"), conn.RemoteAddr()); err != nil && !isClosedSplitConn(conn) {
					t.Fatalf("Write() = %v, want nil", err)
				}
			}
		})
	})

	t.Run("Released", func(t *testing.T) {
		client, ln := splitTrunk(t, &SplitConfig{IdleTimeout: idle, FallbackConn: true})
		writeFrom(t, client, a, "a1")
		if err := acceptSplitConn(t, ln).Release(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * idle)

		writeFrom(t, client, a, "a2") // A new flow, offered to Accept rather than to the fallback connection
		if got := readSplitConn(t, acceptSplitConn(t, ln)); strings.Join(got, " ") != "a2" {
			t.Errorf("packets of the new flow = %q, want %q", got, "a2")
		}
		if got := readFallback(t, ln.Fallback()); strings.Join(got, " ") != a.String()+":a1" {
			t.Errorf("packets of the fallback connection = %q, want the released one only", got)
		}
	})

	t.Run("Disabled", func(t *testing.T) {
		client, ln := splitTrunk(t, &SplitConfig{IdleTimeout: -1})
		writeFrom(t, client, a, "a1")
		conn := acceptSplitConn(t, ln)
		time.Sleep(2 * idle)
		if isClosedSplitConn(conn) {
			t.Errorf("the connection of a flow is closed without an idle timeout, want open")
		}
	})
}

// negativeReads is a FromToPacketConn whose failed reads report a negative count, as Linux does
// for a socket closed while reading.
type negativeReads struct {
	FromToPacketConn
}

// ReadFromTo reads as the connection embedded does, but reports a negative count for a failed read.
func (c negativeReads) ReadFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
	n, src, dst, idx, err := c.FromToPacketConn.ReadFromTo(b)
	if err != nil {
		return -1, src, dst, idx, err
	}
	return n, src, dst, idx, nil
}

// testSplitNegativeRead ensures the read loop takes a failed read with a negative count for an error,
// rather than for a packet, so that the splitter closes once its underlying connection does.
func testSplitNegativeRead(t *testing.T) {
	client, server := BufferedTrunk(16, nil, nil)
	defer func() { _ = client.Close() }()
	ln := Split(negativeReads{server}, nil)
	defer func() { _ = ln.Close() }()

	_ = server.Close() // Under the splitter, which notices it by reading
	accepted := make(chan error, 1)
	go func() {
		_, err := ln.Accept()
		accepted <- err
	}()
	select {
	case err := <-accepted:
		if !errors.Is(err, net.ErrClosed) {
			t.Errorf("Accept() = %v, want net.ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Accept() hasn't returned, as the splitter hasn't closed")
	}
	waitSplitStats(t, ln, SplitStats{})
}

// sourcelessRead is a FromToPacketConn whose first read reports no packet, no address and no error, as Caddy's
// fakeClosePacketConn does once it resets the read deadline it closes itself with, and whose later reads
// are those of the connection embedded.
type sourcelessRead struct {
	FromToPacketConn
	done atomic.Bool
}

// ReadFrom reads as ReadFromTo does, but returns no destination address.
func (c *sourcelessRead) ReadFrom(b []byte) (int, net.Addr, error) {
	n, src, _, _, err := c.ReadFromTo(b)
	return n, src, err
}

// ReadFromTo reports nothing at all the first time, then reads as the connection embedded does.
func (c *sourcelessRead) ReadFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
	if c.done.CompareAndSwap(false, true) {
		return 0, nil, nil, 0, nil
	}
	return c.FromToPacketConn.ReadFromTo(b)
}

// testSplitNoSource ensures a read with no source address, which can't make a flow, is dropped and counted,
// rather than accepted, and that the next packet makes a flow as usual, whether the splitter reads source
// addresses only, or destination addresses too, which are missing then as well.
func testSplitNoSource(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	for _, test := range []struct {
		name string
		wrap func(*sourcelessRead) PacketConn
	}{
		{"Packet", func(c *sourcelessRead) PacketConn { return struct{ PacketConn }{c} }},
		{"FromTo", func(c *sourcelessRead) PacketConn { return c }},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, server := BufferedTrunk(16, nil, nil)
			defer func() { _ = client.Close() }()
			ln := Split(test.wrap(&sourcelessRead{FromToPacketConn: server}), nil)
			defer func() { _ = ln.Close() }()

			writeFrom(t, client, a, "x")
			conn := acceptSplitConn(t, ln)
			if src := fmt.Sprint(conn.RemoteAddr()); src != a.String() {
				t.Errorf("RemoteAddr() = %s, want %s", src, a)
			}
			if got := readSplitConn(t, conn); !slices.Equal(got, []string{"x"}) {
				t.Errorf("packets read = %q, want %q", got, []string{"x"})
			}
			waitSplitStats(t, ln, SplitStats{NoSource: 1})
		})
	}
}

// checkSplitPacket reads a packet from a split connection, and fails unless it's the one wanted.
func checkSplitPacket(t *testing.T, c SplitConn, want string) {
	t.Helper()
	b := make([]byte, len(want)+1)
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	if n, _, err := c.ReadFrom(b); err != nil || string(b[:n]) != want {
		t.Errorf("Read() = %d bytes, %v, want %d bytes, nil", n, err, len(want))
	}
}

// testSplitUDPMaxDatagram ensures a splitter with the default read buffer reads the largest UDP datagrams
// in full, over IPv4 and IPv6 alike.
func testSplitUDPMaxDatagram(t *testing.T) {
	for _, test := range []struct {
		network string
		size    int
	}{
		{"udp4", udp4PayloadSizeMax},
		{"udp6", udp6PayloadSizeMax},
	} {
		t.Run(test.network, func(t *testing.T) {
			client, server := newLocalPacketListeners(t, test.network)
			ln := Split(server, nil)
			defer func() { _ = ln.Close() }()

			data := strings.Repeat("x", test.size)
			if _, err := client.WriteTo([]byte(data), server.LocalAddr()); err != nil {
				t.Fatal(err)
			}
			checkSplitPacket(t, acceptSplitConn(t, ln), data)
		})
	}
}

// testSplitUDPTruncated ensures a splitter drops and counts a UDP datagram larger than its read buffer,
// whether the system flags truncation, as Linux does, or reports an error, as Windows does.
func testSplitUDPTruncated(t *testing.T) {
	skipIfNoUDP(t)

	const size = minSplitReadBufferSize
	for _, info := range []bool{true, false} {
		t.Run(map[bool]string{true: "PacketInfo", false: "NoPacketInfo"}[info], func(t *testing.T) {
			pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			ln := Split(pc, &SplitConfig{ReadBufferSize: size, PacketInfo: info})
			defer func() { _ = ln.Close() }()
			client, err := net.DialUDP("udp4", nil, pc.LocalAddr().(*net.UDPAddr))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()

			for _, data := range []string{strings.Repeat("x", size+1), strings.Repeat("y", 2*size), strings.Repeat("z", size)} {
				if _, err := client.Write([]byte(data)); err != nil {
					t.Fatal(err)
				}
			}
			waitSplitStats(t, ln, SplitStats{Truncated: 2})
			checkSplitPacket(t, acceptSplitConn(t, ln), strings.Repeat("z", size))
		})
	}
}

// sharedUDPConn is a user of a UDP socket shared with other users, closed as Caddy closes one on a reload, where
// the socket can't be reused: with a read deadline passed, which interrupts the read in progress, and which its
// reader clears once it takes the timeout, so that the other users read on.
type sharedUDPConn struct {
	net.PacketConn
	closed atomic.Bool
}

// ReadFrom reads a packet, or fails with net.ErrClosed once the connection is closed, dropping the packet read,
// if any, as the reader of a closed connection has left.
func (c *sharedUDPConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(b)
	if c.closed.Load() {
		if isTimeout(err) {
			_ = c.PacketConn.SetReadDeadline(time.Time{})
		}
		return 0, nil, net.ErrClosed
	}
	return n, addr, err
}

// Close closes the connection, leaving the socket open, with a read deadline passed.
func (c *sharedUDPConn) Close() error {
	c.closed.Store(true)
	return c.PacketConn.SetReadDeadline(time.Now())
}

// testSplitSharedReload ensures a splitter replacing another one over a shared socket, as on a reload, neither
// keeps the other from stopping without traffic, by clearing the read deadline that stops it, nor loses the next
// packet to the other, whatever the order of starting one and closing the other is.
func testSplitSharedReload(t *testing.T) {
	skipIfNoUDP(t)
	pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	client, err := net.DialUDP("udp4", nil, pc.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	ln := Split(&sharedUDPConn{PacketConn: pc}, nil)
	defer func() { _ = ln.Close() }()
	for i := range 20 {
		started := make(chan SplitListener, 1)
		go func() { started <- Split(&sharedUDPConn{PacketConn: pc}, nil) }()
		closed := make(chan error, 1)
		go func() { closed <- ln.Close() }()
		select {
		case err := <-closed:
			if err != nil {
				t.Fatalf("reload %d: Close() = %v, want nil", i, err)
			}
		case <-time.After(time.Second):
			_ = pc.Close() // So that Close returns
			t.Fatalf("reload %d: Close() hasn't returned with no traffic", i)
		}
		ln = <-started

		data := fmt.Sprint("reload ", i)
		if _, err := client.Write([]byte(data)); err != nil {
			t.Fatal(err)
		}
		checkSplitPacket(t, acceptSplitConn(t, ln), data)
	}
}

// timeoutCountingConn is a PacketConn that counts the read timeouts it reports.
type timeoutCountingConn struct {
	net.PacketConn
	timeouts atomic.Int64
}

// ReadFrom reads a packet, counting a timeout.
func (c *timeoutCountingConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(b)
	if isTimeout(err) {
		c.timeouts.Add(1)
	}
	return n, addr, err
}

// testSplitForeignDeadline ensures a read deadline passed that the splitter hasn't set, and that no one clears,
// e.g. as another reader of a shared socket has left without reading again, neither makes the read loop spin
// on timeouts, nor keeps it from reading for long.
func testSplitForeignDeadline(t *testing.T) {
	skipIfNoUDP(t)
	pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	client, err := net.DialUDP("udp4", nil, pc.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	c := &timeoutCountingConn{PacketConn: pc}
	ln := Split(c, nil)
	defer func() { _ = ln.Close() }()

	if _, err := client.Write([]byte("before")); err != nil {
		t.Fatal(err)
	}
	conn := acceptSplitConn(t, ln)
	checkSplitPacket(t, conn, "before")

	_ = pc.SetReadDeadline(time.Unix(1, 0)) // By another reader
	for deadline := time.Now().Add(time.Second); c.timeouts.Load() == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("no read has timed out")
		}
	}
	if _, err := client.Write([]byte("after")); err != nil {
		t.Fatal(err)
	}
	checkSplitPacket(t, conn, "after")
	time.Sleep(10 * time.Millisecond) // Let the read loop spin, if it did
	if n := c.timeouts.Load(); n != splitForeignTimeouts {
		t.Errorf("read timeouts = %d, want %d, after which the read deadline is cleared", n, splitForeignTimeouts)
	}
}
