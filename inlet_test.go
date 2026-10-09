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
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand/v2"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPacketInletAccept(t *testing.T) {
	pc1, pc2 := newLocalPacketListeners("udp4")
	defer func() { _ = pc1.Close() }()

	pi2 := Inlet(pc2)
	defer func() { _ = pi2.Close() }()

	handle := func(mc MixedConn) {
		if addr := mc.LocalAddr(); addr == nil {
			t.Errorf("mc.LocalAddr() = %v, want non-nil", addr)
		}
		if addr := mc.RemoteAddr(); addr == nil {
			t.Errorf("mc.RemoteAddr() = %v, want non-nil", addr)
		}

		b := make([]byte, 32)
		for {
			n, err := mc.Read(b)
			if n == 0 && err != nil {
				break
			}
			n, err = mc.Write(b[:n])
			if n == 0 && err != nil {
				break
			}
		}
	}

	serve := func(l PacketListener, handle func(mc MixedConn)) {
		for {
			mc, err := l.Accept()
			if err != nil {
				return
			}
			go handle(mc)
		}
	}

	go serve(pi2, handle)

	timeout := 50 * time.Millisecond

	if err := pc1.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatalf("pc1.SetWriteDeadline() = %v, want nil", err)
	}

	wb := make([]byte, 1<<10)
	_, _ = io.ReadFull(rand.Reader, wb)

	if n, err := pc1.WriteTo(wb, pi2.Addr()); n == 0 && err != nil {
		t.Fatalf("pc1.WriteTo() = %v, want nil", err)
	}
}

func TestPacketInletWithPairedConns(t *testing.T) {
	packetCount := 1 << 13

	type pair struct {
		name                                   string
		ctor                                   packetConnPairCtor
		minWBS, minSafeWBS, maxSafeWBS, maxWBS int // WBS - write buffer size
	}
	pairs := []pair{
		{name: "StandardUnbufferedSyncPipe", ctor: func() (PacketConn, PacketConn) {
			c1, c2 := net.Pipe()
			return StreamToPacketConn(c1), StreamToPacketConn(c2)
		}, minWBS: packetSizeMin, minSafeWBS: packetSizeMinSafe, maxSafeWBS: packetSizeMaxSafe, maxWBS: packetSizeMax},
		// The smallest packet over our pipes takes a byte, as they send nothing for an empty write, like TCP
		{name: "HybridUnbufferedSyncPipe", ctor: func() (PacketConn, PacketConn) {
			c1, c2 := UnbufferedPipe(false)
			return StreamToPacketConn(c1), StreamToPacketConn(c2)
		}, minWBS: 1, minSafeWBS: packetSizeMinSafe, maxSafeWBS: packetSizeMaxSafe, maxWBS: packetSizeMax},
		{name: "HybridUnbufferedSemiPipe", ctor: func() (PacketConn, PacketConn) {
			c1, c2 := UnbufferedPipe(true)
			return StreamToPacketConn(c1), StreamToPacketConn(c2)
		}, minWBS: 1, minSafeWBS: packetSizeMinSafe, maxSafeWBS: packetSizeMaxSafe, maxWBS: packetSizeMax},
		{name: "HybridBufferedAsyncPipe", ctor: func() (PacketConn, PacketConn) {
			c1, c2 := BufferedPipe(10)
			return StreamToPacketConn(c1), StreamToPacketConn(c2)
		}, minWBS: 1, minSafeWBS: packetSizeMinSafe, maxSafeWBS: packetSizeMaxSafe, maxWBS: packetSizeMax},
		{name: "LocalUDP4Socket", ctor: func() (PacketConn, PacketConn) {
			return newLocalPacketListeners("udp4")
		}, minWBS: udp4PayloadSizeMin, minSafeWBS: udp4PayloadSizeMinSafe, maxSafeWBS: udp4PayloadSizeMaxSafe, maxWBS: udp4PayloadSizeMax},
		{name: "LocalUDP6Socket", ctor: func() (PacketConn, PacketConn) {
			return newLocalPacketListeners("udp6")
		}, minWBS: udp6PayloadSizeMin, minSafeWBS: udp6PayloadSizeMinSafe, maxSafeWBS: udp6PayloadSizeMaxSafe, maxWBS: udp6PayloadSizeMax},
	}

	testFunc := func(t *testing.T, ctor packetConnPairCtor, writeBufferSize int) {
		testPacketInletWithPairedConns(t, ctor, piAcceptQueueSizeDef, poReadQueueSizeDef, packetSizeMax, writeBufferSize, packetCount)
	}

	for _, item := range pairs {
		randomSafeWBS := mathrand.IntN(item.maxSafeWBS-item.minSafeWBS+1) + item.minSafeWBS
		t.Run(item.name, func(t *testing.T) {
			t.Run("MinPayload", func(t *testing.T) {
				testFunc(t, item.ctor, item.minWBS)
			})
			t.Run("MinSafePayload", func(t *testing.T) {
				testFunc(t, item.ctor, item.minSafeWBS)
			})
			t.Run("RandomSafePayload", func(t *testing.T) {
				testFunc(t, item.ctor, randomSafeWBS)
			})
			t.Run("MaxSafePayload", func(t *testing.T) {
				testFunc(t, item.ctor, item.maxSafeWBS)
			})
			t.Run("MaxPayload", func(t *testing.T) {
				testFunc(t, item.ctor, item.maxWBS)
			})
		})
	}
}

func testPacketInletWithPairedConns(t *testing.T, ctor packetConnPairCtor, acceptQueueSize, readQueueSize, readBufferSize, writeBufferSize, packetCount int) {
	t.Helper()
	pc1, pc2 := ctor()

	timeout := 50 * time.Millisecond
	chr, chw, done := make(chan int), make(chan int), make(chan struct{})
	var stats InletStats // Of the inlet, once closed

	connHandler := func(mc MixedConn, ln PacketListener, wg *sync.WaitGroup) {
		defer wg.Done()

		lAddr, rAddr := mc.LocalAddr(), mc.RemoteAddr()
		me := fmt.Sprintf("outlet %v <- %v", lAddr, rAddr)
		logfIf(t, testPacketInletDebug, "%v: started", me)

		defer func() { _ = mc.Close() }()

		b := make([]byte, readBufferSize)
		var addr net.Addr
		var err error
		var n int
		for {
			err = mc.SetReadDeadline(time.Now().Add(2 * timeout))
			if err != nil {
				t.Errorf("%v: pc.SetReadDeadline() = %v, want nil", me, err)
				break
			}

			n, addr, err = mc.ReadFrom(b)
			if n == 0 && err != nil {
				logfIf(t, testPacketInletDebug, "%v: pc.ReadFrom() = %v", me, err)
				break
			}

			if got, want := addr.String(), rAddr.String(); got != want {
				t.Errorf("%v: pc.ReadFrom() = %v, want %v", me, got, want)
			}

			chr <- n
			logfIf(t, testPacketInletDebug, "%v: read %d bytes", me, n)
		}

		logfIf(t, testPacketInletDebug, "%v: stopped", me)
	}

	serverHandler := func(pc PacketConn, wg *sync.WaitGroup) {
		defer wg.Done()

		lAddr := pc.LocalAddr()
		me := fmt.Sprintf("inlet %v", lAddr)
		logfIf(t, testPacketInletDebug, "%v: started", me)

		ln := Inlet(pc,
			WithAcceptQueueSize(acceptQueueSize),
			WithReadQueueSize(readQueueSize),
			WithReadBufferSize(readBufferSize))
		defer func() {
			_ = ln.Close()
			stats = ln.(interface{ Stats() InletStats }).Stats()
		}()

		for {
			newMixedConn, err := ln.Accept()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					logfIf(t, testPacketInletDebug, "%v: ln.Accept() = %v", me, err)
				}
				break
			}

			wg.Add(1)
			go connHandler(newMixedConn, ln, wg)
		}

		logfIf(t, testPacketInletDebug, "%v: stopped", me)
	}

	clientHandler := func(pc PacketConn, to net.Addr, wg *sync.WaitGroup) {
		defer wg.Done()

		lAddr, rAddr := pc.LocalAddr(), to
		me := fmt.Sprintf("client %v -> %v", lAddr, rAddr)
		logfIf(t, testPacketInletDebug, "%v: started", me)

		b := make([]byte, writeBufferSize)
		_, _ = io.ReadFull(rand.Reader, b)

		var err error
		var n int
		for i := 0; i < packetCount; i++ {
			err = pc.SetWriteDeadline(time.Now().Add(2 * timeout))
			if err != nil {
				t.Errorf("%v: pc.SetWriteDeadline() = %v, want nil", me, err)
				break // On to closing done, so that the test doesn't hang
			}

			n, err = pc.WriteTo(b, rAddr)
			if n == 0 && err != nil {
				t.Errorf("%v: pc.WriteTo() = %v, want nil", me, err)
				break
			}

			chw <- n
			logfIf(t, testPacketInletDebug, "%v: wrote %d bytes", me, n)
		}

		time.AfterFunc(timeout, func() {
			close(done)
		})

		logfIf(t, testPacketInletDebug, "%v: stopped", me)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go serverHandler(pc2, &wg)
	go clientHandler(pc1, pc2.LocalAddr(), &wg)

	var acr, acw, anr, anw int
loop:
	for {
		select {
		case n := <-chr:
			acr++
			anr += n
		case n := <-chw:
			acw++
			anw += n
		case <-done:
			_ = pc1.Close()
			_ = pc2.Close()
			break loop
		}
	}

	wg.Wait()

	// The inlet drops a packet rather than wait for a slow reader, and counts it
	dropped := int(stats.ConnQueueFull + stats.AcceptQueueFull + stats.TableFull + stats.NoDestination)
	if acr+dropped != acw || anr+dropped*writeBufferSize != anw {
		message := fmt.Sprintf("r/w statistics: count %d+%d/%d, bytes %d+%d/%d", acr, dropped, acw, anr, dropped*writeBufferSize, anw)
		if pc2.LocalAddr().Network() == "udp" {
			// UDP doesn't guarantee that all packets sent will be received,
			// so we can't consider this test as failing if anything is lost.
			t.Skip(message)
		}

		// However, pipes do guarantee that all packets are delivered,
		// so any loss the inlet doesn't count represents a failure of this test.
		t.Error(message)
	}
}

const (
	testPacketInletDebug = false
)

// testAddr is an address of a network other than UDP, e.g. of a flow over a trunk.
type testAddr string

func (a testAddr) Network() string { return "test" }

func (a testAddr) String() string { return string(a) }

// withInletConfig returns an option that changes the configuration of an inlet directly,
// e.g. to set a limit there is no option for.
func withInletConfig(f func(*inletConfig)) InletOption { return inletOptionFunc(f) }

// waitInletStats waits until the inlet has dropped the given numbers of packets, or fails.
func waitInletStats(t *testing.T, ln PacketListener, want InletStats) {
	t.Helper()
	s := ln.(interface{ Stats() InletStats })
	deadline := time.Now().Add(time.Second)
	for got := s.Stats(); got != want; got = s.Stats() {
		if time.Now().After(deadline) {
			t.Fatalf("Stats() = %+v, want %+v", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// acceptInletConn accepts an inlet connection, or fails if there is none within a second.
func acceptInletConn(t *testing.T, ln PacketListener) MixedConn {
	t.Helper()
	type result struct {
		c   MixedConn
		err error
	}
	ch := make(chan result, 1)
	go func() {
		c, err := ln.Accept()
		ch <- result{c, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("Accept() = %v, want nil", r.err)
		}
		return r.c
	case <-time.After(time.Second):
		t.Fatalf("Accept() hasn't returned")
		return nil
	}
}

// readInletConn returns the packets read from an inlet connection until none comes for a while.
func readInletConn(t *testing.T, c MixedConn) []string {
	t.Helper()
	var pkts []string
	b := make([]byte, 64)
	for {
		_ = c.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		n, err := c.Read(b)
		if err != nil {
			return pkts
		}
		pkts = append(pkts, string(b[:n]))
	}
}

// TestInletTrunkFlows ensures packets over a trunk make a flow per source address, whatever its type,
// destination address and interface index are, and that a flow keeps the latest interface index.
func TestInletTrunkFlows(t *testing.T) {
	client, server := BufferedTrunk(16, nil, nil)
	defer func() { _ = client.Close() }()
	ln := Inlet(server)
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
	))
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
	setup := func(t *testing.T, opts ...InletOption) (write func(src net.Addr, data string), ln PacketListener) {
		client, server := BufferedTrunk(16, nil, nil)
		t.Cleanup(func() { _ = client.Close() })
		ln = Inlet(server, opts...)
		t.Cleanup(func() { _ = ln.Close() })
		return func(src net.Addr, data string) {
			t.Helper()
			if _, err := client.WriteToFrom([]byte(data), nil, src, 0); err != nil {
				t.Fatal(err)
			}
		}, ln
	}

	t.Run("ConnQueueFull", func(t *testing.T) {
		write, ln := setup(t, WithReadQueueSize(1))
		for _, data := range []string{"a1", "a2", "a3"} {
			write(a, data)
		}
		waitInletStats(t, ln, InletStats{ConnQueueFull: 2})
		if got := readInletConn(t, acceptInletConn(t, ln)); strings.Join(got, " ") != "a1" {
			t.Errorf("packets read = %q, want the first one only", got)
		}
	})

	t.Run("AcceptQueueFull", func(t *testing.T) {
		write, ln := setup(t, WithAcceptQueueSize(1))
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
		write, ln := setup(t, withInletConfig(func(cfg *inletConfig) { cfg.activeConnLimit = 1 }))
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
