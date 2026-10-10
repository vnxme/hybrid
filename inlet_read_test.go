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
	"strings"
	"sync"
	"testing"
	"time"
)

// TestInletTrunkFlows ensures packets over a trunk make a flow per source address, whatever its type,
// destination address and interface index are, and that a flow keeps the latest interface index.
func TestInletTrunkFlows(t *testing.T) {
	client, server := BufferedTrunk(16, nil, nil)
	defer func() { _ = client.Close() }()
	ln := Inlet(server, nil)
	defer func() { _ = ln.Close() }()

	a, b, c := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}, testAddr("c")
	dst1, dst2 := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 53}, &net.UDPAddr{IP: net.IPv4(198, 51, 100, 2), Port: 53}
	for _, w := range []struct {
		src, dst net.Addr
		idx      int
		data     string
	}{
		{a, dst1, 1, "a1"},
		{b, dst1, 1, "b1"},
		{c, dst1, 1, "c1"},
		{a, dst2, 2, "a2"}, // The flow of a, though to another destination address and through another interface
	} {
		if _, err := client.WriteToFrom([]byte(w.data), w.dst, w.src, w.idx); err != nil {
			t.Fatal(err)
		}
	}

	want := map[string]string{a.String(): "a1 a2", b.String(): "b1", c.String(): "c1"}
	for range want {
		conn := acceptInletConn(t, ln)
		remote := conn.RemoteAddr().String()
		if got := strings.Join(readInletConn(t, conn), " "); got != want[remote] {
			t.Errorf("packets of the flow of %s = %q, want %q", remote, got, want[remote])
		}
		if wantIdx := map[bool]int{true: 2, false: 1}[remote == a.String()]; conn.(interface{ LocalIfIdx() int }).LocalIfIdx() != wantIdx {
			t.Errorf("LocalIfIdx() of the flow of %s = %d, want %d", remote, conn.(interface{ LocalIfIdx() int }).LocalIfIdx(), wantIdx)
		}
		_ = conn.Close()
	}
	waitInletStats(t, ln, InletStats{})
}

// scriptedUDPConn is a FromToUDPConn whose reads return the given packets, then wait until it's closed.
// Calling a method it doesn't implement panics.
type scriptedUDPConn struct {
	FromToUDPConn
	pkts chan scriptedPacket
	done chan struct{}
	once sync.Once
}

type scriptedPacket struct {
	src, dst netip.AddrPort
	data     string
}

func newScriptedUDPConn(pkts ...scriptedPacket) *scriptedUDPConn {
	c := &scriptedUDPConn{pkts: make(chan scriptedPacket, len(pkts)), done: make(chan struct{})}
	for _, p := range pkts {
		c.pkts <- p
	}
	return c
}

func (c *scriptedUDPConn) ReadFromToUDPAddrPort(b []byte) (int, netip.AddrPort, netip.AddrPort, int, error) {
	select {
	case p := <-c.pkts:
		return copy(b, p.data), p.src, p.dst, 1, nil
	case <-c.done:
		return 0, netip.AddrPort{}, netip.AddrPort{}, 0, net.ErrClosed
	}
}

func (c *scriptedUDPConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}

func (c *scriptedUDPConn) LocalAddr() net.Addr { return &net.UDPAddr{IP: net.IPv4zero, Port: 53} }

// TestInletUDPFlows ensures a UDP connection that reports destination addresses makes a flow per source
// and destination address, and that a packet without a destination address is dropped and counted.
func TestInletUDPFlows(t *testing.T) {
	s1, s2 := netip.MustParseAddrPort("192.0.2.1:1"), netip.MustParseAddrPort("192.0.2.2:2")
	d1, d2 := netip.MustParseAddrPort("198.51.100.1:53"), netip.MustParseAddrPort("198.51.100.2:53")
	ln := Inlet(newScriptedUDPConn(
		scriptedPacket{s1, d1, "x"},
		scriptedPacket{s1, d2, "y"}, // Another flow, as it's to another destination address
		scriptedPacket{s1, netip.AddrPort{}, "z"},
		scriptedPacket{s2, d1, "w"},
		scriptedPacket{s1, d1, "v"},
	), nil)
	defer func() { _ = ln.Close() }()

	want := map[string]string{s1.String() + ">" + d1.String(): "x v", s1.String() + ">" + d2.String(): "y", s2.String() + ">" + d1.String(): "w"}
	for range want {
		conn := acceptInletConn(t, ln)
		flow := conn.RemoteAddr().String() + ">" + conn.LocalAddr().String()
		if got := strings.Join(readInletConn(t, conn), " "); got != want[flow] {
			t.Errorf("packets of flow %s = %q, want %q", flow, got, want[flow])
		}
	}
	waitInletStats(t, ln, InletStats{NoDestination: 1})
}

// TestInletDrops ensures the inlet drops and counts a packet rather than wait for a full queue
// or a full table, and that a dropped first packet doesn't create a flow.
func TestInletDrops(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	setup := func(t *testing.T, config *InletConfig) (write func(src net.Addr, data string), ln PacketListener) {
		client, server := BufferedTrunk(16, nil, nil)
		t.Cleanup(func() { _ = client.Close() })
		ln = Inlet(server, config)
		t.Cleanup(func() { _ = ln.Close() })
		return func(src net.Addr, data string) {
			t.Helper()
			if _, err := client.WriteToFrom([]byte(data), nil, src, 0); err != nil {
				t.Fatal(err)
			}
		}, ln
	}

	t.Run("ConnQueueFull", func(t *testing.T) {
		write, ln := setup(t, &InletConfig{ReadQueueSize: 1})
		for _, data := range []string{"a1", "a2", "a3"} {
			write(a, data)
		}
		waitInletStats(t, ln, InletStats{ConnQueueFull: 2})
		if got := readInletConn(t, acceptInletConn(t, ln)); strings.Join(got, " ") != "a1" {
			t.Errorf("packets read = %q, want the first one only", got)
		}
	})

	t.Run("AcceptQueueFull", func(t *testing.T) {
		write, ln := setup(t, &InletConfig{AcceptQueueSize: 1})
		write(a, "a1")
		write(b, "b1")
		write(b, "b2") // Still a new flow, as the first packet of b hasn't created one
		waitInletStats(t, ln, InletStats{AcceptQueueFull: 2})
		if conn := acceptInletConn(t, ln); conn.RemoteAddr().String() != a.String() {
			t.Fatalf("RemoteAddr() = %s, want %s", conn.RemoteAddr(), a)
		}

		write(b, "b3") // Now there is room in the accept queue
		conn := acceptInletConn(t, ln)
		if got := readInletConn(t, conn); conn.RemoteAddr().String() != b.String() || strings.Join(got, " ") != "b3" {
			t.Errorf("RemoteAddr(), packets read = %s, %q, want %s, the latest one only", conn.RemoteAddr(), got, b)
		}
	})

	t.Run("TableFull", func(t *testing.T) {
		write, ln := setup(t, &InletConfig{MaxFlows: 1})
		write(a, "a1")
		write(b, "b1")
		waitInletStats(t, ln, InletStats{TableFull: 1})

		_ = acceptInletConn(t, ln).Close() // Frees a place in the table
		write(b, "b2")
		conn := acceptInletConn(t, ln)
		if got := readInletConn(t, conn); conn.RemoteAddr().String() != b.String() || strings.Join(got, " ") != "b2" {
			t.Errorf("RemoteAddr(), packets read = %s, %q, want %s, the latest one only", conn.RemoteAddr(), got, b)
		}
	})
}

// TestInletFallbackOnly ensures every flow goes to the fallback connection straight away with FallbackOnly,
// that a full queue of the fallback connection drops packets, and that a closed one does too.
func TestInletFallbackOnly(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}

	t.Run("Flows", func(t *testing.T) {
		client, ln := inletTrunk(t, &InletConfig{FallbackOnly: true})
		accepted := make(chan error, 1)
		go func() {
			_, err := ln.Accept()
			accepted <- err
		}()
		writeFrom(t, client, a, "a1")
		writeFrom(t, client, b, "b1")
		writeFrom(t, client, a, "a2")
		if got, want := strings.Join(readFallback(t, ln.Fallback()), " "), a.String()+":a1 "+b.String()+":b1 "+a.String()+":a2"; got != want {
			t.Errorf("packets of the fallback connection = %q, want %q", got, want)
		}
		_ = ln.Close()
		if err := <-accepted; err == nil {
			t.Errorf("Accept() = nil, want an error once the inlet is closed, as no flow is accepted")
		}
	})

	t.Run("QueueFull", func(t *testing.T) {
		client, ln := inletTrunk(t, &InletConfig{FallbackOnly: true, FallbackQueueSize: 1})
		writeFrom(t, client, a, "a1")
		writeFrom(t, client, a, "a2")
		writeFrom(t, client, b, "b1")
		waitInletStats(t, ln, InletStats{FallbackQueueFull: 2})
		if got, want := strings.Join(readFallback(t, ln.Fallback()), " "), a.String()+":a1"; got != want {
			t.Errorf("packets of the fallback connection = %q, want %q", got, want)
		}
	})

	t.Run("Closed", func(t *testing.T) {
		client, ln := inletTrunk(t, &InletConfig{FallbackOnly: true})
		if err := ln.Fallback().Close(); err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}
		writeFrom(t, client, a, "a1")
		waitInletStats(t, ln, InletStats{NoFallback: 1})
	})
}

// isClosedInletConn reports whether an inlet connection has been closed, e.g. as its flow has expired.
func isClosedInletConn(c MixedConn) bool { return c.SetDeadline(time.Time{}) != nil }

// TestInletIdleTimeout ensures a flow ends once it's been idle for the idle timeout, released or not,
// so that its next packet starts a new flow, while packets read or written keep it going.
func TestInletIdleTimeout(t *testing.T) {
	const idle = 200 * time.Millisecond // Long enough for the scheduling stalls of a busy machine
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}

	t.Run("Expired", func(t *testing.T) {
		client, ln := inletTrunk(t, &InletConfig{IdleTimeout: idle})
		writeFrom(t, client, a, "a1")
		conn := acceptInletConn(t, ln)
		time.Sleep(2 * idle) // Over one and a quarter idle timeouts
		if !isClosedInletConn(conn) {
			t.Fatalf("the connection of an idle flow is open, want closed")
		}

		writeFrom(t, client, a, "a2")
		if got := readInletConn(t, acceptInletConn(t, ln)); strings.Join(got, " ") != "a2" {
			t.Errorf("packets of the new flow = %q, want %q", got, "a2")
		}
	})

	t.Run("KeptByReads", func(t *testing.T) {
		client, ln := inletTrunk(t, &InletConfig{IdleTimeout: idle})
		writeFrom(t, client, a, "a0")
		conn := acceptInletConn(t, ln)
		for i := range 12 { // Three idle timeouts in all
			time.Sleep(idle / 4)
			writeFrom(t, client, a, fmt.Sprintf("a%d", i+1))
		}
		if isClosedInletConn(conn) {
			t.Errorf("the connection of an active flow is closed, want open")
		}
		if got := readInletConn(t, conn); len(got) != 13 {
			t.Errorf("packets of the flow = %q, want 13 of them", got)
		}
	})

	t.Run("KeptByWrites", func(t *testing.T) {
		client, ln := inletTrunk(t, &InletConfig{IdleTimeout: idle})
		writeFrom(t, client, a, "a0")
		conn := acceptInletConn(t, ln)
		go func() { // Drain the trunk, so that writes never wait
			b := make([]byte, 8)
			for {
				if _, _, err := client.ReadFrom(b); err != nil {
					return
				}
			}
		}()
		for range 12 {
			time.Sleep(idle / 4)
			if _, err := conn.Write([]byte("w")); err != nil {
				t.Fatalf("Write() = %v, want nil", err)
			}
		}
		if isClosedInletConn(conn) {
			t.Errorf("the connection of an active flow is closed, want open")
		}
	})

	t.Run("Released", func(t *testing.T) {
		client, ln := inletTrunk(t, &InletConfig{IdleTimeout: idle, FallbackConn: true})
		writeFrom(t, client, a, "a1")
		if err := acceptInletConn(t, ln).(interface{ Release() error }).Release(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * idle)

		writeFrom(t, client, a, "a2") // A new flow, offered to Accept rather than to the fallback connection
		if got := readInletConn(t, acceptInletConn(t, ln)); strings.Join(got, " ") != "a2" {
			t.Errorf("packets of the new flow = %q, want %q", got, "a2")
		}
		if got := readFallback(t, ln.Fallback()); strings.Join(got, " ") != a.String()+":a1" {
			t.Errorf("packets of the fallback connection = %q, want the released one only", got)
		}
	})

	t.Run("Disabled", func(t *testing.T) {
		client, ln := inletTrunk(t, &InletConfig{IdleTimeout: -1})
		writeFrom(t, client, a, "a1")
		conn := acceptInletConn(t, ln)
		time.Sleep(2 * idle)
		if isClosedInletConn(conn) {
			t.Errorf("the connection of a flow is closed without an idle timeout, want open")
		}
	})
}

// negativeReads is a FromToPacketConn whose failed reads report a negative count, as Linux does
// for a socket closed while reading.
type negativeReads struct {
	FromToPacketConn
}

func (c negativeReads) ReadFromTo(b []byte) (int, net.Addr, net.Addr, int, error) {
	n, src, dst, idx, err := c.FromToPacketConn.ReadFromTo(b)
	if err != nil {
		return -1, src, dst, idx, err
	}
	return n, src, dst, idx, nil
}

// TestInletNegativeRead ensures the read loop takes a failed read with a negative count for an error,
// rather than for a packet, so that the inlet closes once its underlying connection does.
func TestInletNegativeRead(t *testing.T) {
	client, server := BufferedTrunk(16, nil, nil)
	defer func() { _ = client.Close() }()
	ln := Inlet(negativeReads{server}, nil)
	defer func() { _ = ln.Close() }()

	_ = server.Close() // Under the inlet, which notices it by reading
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
		t.Fatalf("Accept() hasn't returned, as the inlet hasn't closed")
	}
	waitInletStats(t, ln, InletStats{})
}
