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
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestInletPairedConns(t *testing.T) {
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
		testInletPairedConns(t, ctor, DefaultInletAcceptQueueSize, DefaultInletReadQueueSize, packetSizeMax, writeBufferSize, packetCount)
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

func testInletPairedConns(t *testing.T, ctor packetConnPairCtor, acceptQueueSize, readQueueSize, readBufferSize, writeBufferSize, packetCount int) {
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
			// Once the inlet has closed, setting a deadline fails, but the packets queued are still read,
			// without waiting, then io.EOF
			err = mc.SetReadDeadline(time.Now().Add(2 * timeout))
			if err != nil && !errors.Is(err, io.ErrClosedPipe) {
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

		ln := Inlet(pc, &InletConfig{
			AcceptQueueSize: acceptQueueSize,
			ReadQueueSize:   readQueueSize,
			ReadBufferSize:  readBufferSize,
			PacketInfo:      true,
		})
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
	message := fmt.Sprintf("r/w statistics: count %d+%d/%d, bytes %d+%d/%d", acr, dropped, acw, anr, dropped*writeBufferSize, anw)
	if pc2.LocalAddr().Network() == "udp" {
		// The system may drop datagrams the inlet never sees, e.g. as the receive buffer of the socket overflows,
		// so fewer packets may be read and counted than written, but never more, and not none at all.
		t.Log(message)
		if acr+dropped > acw || anr+dropped*writeBufferSize > anw || acr == 0 {
			t.Error(message + ", want no more than written, and some read")
		}
		return
	}

	// Pipes deliver every packet, so the inlet reads or counts each one
	if acr+dropped != acw || anr+dropped*writeBufferSize != anw {
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

// inletTrunk returns a trunk end to write to an inlet over the other end from, and the inlet.
func inletTrunk(t *testing.T, config *InletConfig) (FromToPacketConn, InletListener) {
	t.Helper()
	client, server := BufferedTrunk(16, nil, nil)
	t.Cleanup(func() { _ = client.Close() })
	ln := Inlet(server, config)
	t.Cleanup(func() { _ = ln.Close() })
	return client, ln
}

// writeFrom writes data over a trunk end from the given source address, or fails.
func writeFrom(t *testing.T, client FromToPacketConn, src net.Addr, data string) {
	t.Helper()
	if _, err := client.WriteToFrom([]byte(data), nil, src, 0); err != nil {
		t.Fatal(err)
	}
}

// readDefault returns the packets read from a default connection, with their remote addresses,
// until none comes for a while.
func readDefault(t *testing.T, d PacketConn) []string {
	t.Helper()
	var pkts []string
	b := make([]byte, 64)
	for {
		_ = d.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		n, addr, err := d.ReadFrom(b)
		if err != nil {
			return pkts
		}
		pkts = append(pkts, addr.String()+":"+string(b[:n]))
	}
}

// TestInletRelease ensures releasing an inlet connection hands the packets queued and the later packets
// of its flow over to the default connection, in order, while the other flows go on as they were.
func TestInletRelease(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	client, ln := inletTrunk(t, &InletConfig{DefaultConn: true})
	writeFrom(t, client, a, "a1")
	writeFrom(t, client, a, "a2")
	conn := acceptInletConn(t, ln)
	if err := conn.(interface{ Release() error }).Release(); err != nil {
		t.Fatalf("Release() = %v, want nil", err)
	}
	writeFrom(t, client, a, "a3")
	writeFrom(t, client, b, "b1")

	if got, want := strings.Join(readDefault(t, ln.Default()), " "), a.String()+":a1 "+a.String()+":a2 "+a.String()+":a3"; got != want {
		t.Errorf("packets of the default connection = %q, want %q", got, want)
	}
	if other := acceptInletConn(t, ln); other.RemoteAddr().String() != b.String() {
		t.Errorf("RemoteAddr() = %s, want %s", other.RemoteAddr(), b)
	}
	if _, err := conn.Read(make([]byte, 8)); err == nil {
		t.Errorf("Read() after Release() = nil, want an error")
	}
	if err := conn.(interface{ Release() error }).Release(); err == nil {
		t.Errorf("Release() again = nil, want an error")
	}
	if err := conn.Close(); err == nil {
		t.Errorf("Close() after Release() = nil, want an error")
	}
	waitInletStats(t, ln, InletStats{})
}

// TestInletReleaseWithoutDefault ensures releasing an inlet connection without a default connection drops
// the packets of its flow, without starting a new flow.
func TestInletReleaseWithoutDefault(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	client, ln := inletTrunk(t, nil)
	if ln.Default() != nil {
		t.Fatalf("Default() = %v, want nil", ln.Default())
	}
	writeFrom(t, client, a, "a1")
	if err := acceptInletConn(t, ln).(interface{ Release() error }).Release(); err != nil {
		t.Fatalf("Release() = %v, want nil", err)
	}
	writeFrom(t, client, a, "a2")
	writeFrom(t, client, b, "b1")
	waitInletStats(t, ln, InletStats{NoDefault: 2})
	if conn := acceptInletConn(t, ln); conn.RemoteAddr().String() != b.String() { // Not a new flow of a
		t.Errorf("RemoteAddr() = %s, want %s", conn.RemoteAddr(), b)
	}
}

// TestInletDefaultOnly ensures every flow goes to the default connection straight away with WithDefaultOnly,
// that a full queue of the default connection drops packets, and that a closed one does too.
func TestInletDefaultOnly(t *testing.T) {
	a, b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}

	t.Run("Flows", func(t *testing.T) {
		client, ln := inletTrunk(t, &InletConfig{DefaultOnly: true})
		accepted := make(chan error, 1)
		go func() {
			_, err := ln.Accept()
			accepted <- err
		}()
		writeFrom(t, client, a, "a1")
		writeFrom(t, client, b, "b1")
		writeFrom(t, client, a, "a2")
		if got, want := strings.Join(readDefault(t, ln.Default()), " "), a.String()+":a1 "+b.String()+":b1 "+a.String()+":a2"; got != want {
			t.Errorf("packets of the default connection = %q, want %q", got, want)
		}
		_ = ln.Close()
		if err := <-accepted; err == nil {
			t.Errorf("Accept() = nil, want an error once the inlet is closed, as no flow is accepted")
		}
	})

	t.Run("QueueFull", func(t *testing.T) {
		client, ln := inletTrunk(t, &InletConfig{DefaultOnly: true, DefaultQueueSize: 1})
		writeFrom(t, client, a, "a1")
		writeFrom(t, client, a, "a2")
		writeFrom(t, client, b, "b1")
		waitInletStats(t, ln, InletStats{DefaultQueueFull: 2})
		if got, want := strings.Join(readDefault(t, ln.Default()), " "), a.String()+":a1"; got != want {
			t.Errorf("packets of the default connection = %q, want %q", got, want)
		}
	})

	t.Run("Closed", func(t *testing.T) {
		client, ln := inletTrunk(t, &InletConfig{DefaultOnly: true})
		if err := ln.Default().Close(); err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}
		writeFrom(t, client, a, "a1")
		waitInletStats(t, ln, InletStats{NoDefault: 1})
	})
}

// TestInletDefaultWrite ensures the default connection writes to a remote address it has seen from the local
// address and through the interface of its latest flow, and lets the system choose them for another one.
func TestInletDefaultWrite(t *testing.T) {
	a, u := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 9), Port: 9}
	local := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 53}
	client, ln := inletTrunk(t, &InletConfig{DefaultOnly: true})
	if _, err := client.WriteToFrom([]byte("a1"), local, a, 3); err != nil {
		t.Fatal(err)
	}
	def := ln.Default()
	_ = def.SetReadDeadline(time.Now().Add(time.Second))
	b := make([]byte, 8)
	if _, addr, err := def.ReadFrom(b); err != nil || addr.String() != a.String() {
		t.Fatalf("ReadFrom() = %v, %v, want %s, nil", addr, err, a)
	}

	read := func() (string, net.Addr, net.Addr, int) {
		t.Helper()
		_ = client.SetReadDeadline(time.Now().Add(time.Second))
		n, src, dst, idx, err := client.ReadFromTo(b)
		if err != nil {
			t.Fatalf("ReadFromTo() = %v, want nil", err)
		}
		return string(b[:n]), src, dst, idx
	}

	if _, err := def.WriteTo([]byte("x"), a); err != nil {
		t.Fatalf("WriteTo(%s) = %v, want nil", a, err)
	}
	if data, src, dst, idx := read(); data != "x" || src.String() != local.String() || dst.String() != a.String() || idx != 3 {
		t.Errorf("ReadFromTo() = %q from %s to %s through %d, want %q from %s to %s through 3", data, src, dst, idx, "x", local, a)
	}

	if _, err := def.WriteTo([]byte("y"), u); err != nil {
		t.Fatalf("WriteTo(%s) = %v, want nil", u, err)
	}
	if data, src, dst, idx := read(); data != "y" || src.String() == local.String() || dst.String() != u.String() || idx != 0 {
		t.Errorf("ReadFromTo() = %q from %s to %s through %d, want %q from the system's choice to %s through 0", data, src, dst, idx, "y", u)
	}

	if _, err := def.WriteTo([]byte("z"), nil); err == nil {
		t.Errorf("WriteTo(nil) = nil, want an error")
	}
}

// TestInletWriteCopies ensures a write returns once its packet is queued, with a copy of the bytes given.
func TestInletWriteCopies(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := inletTrunk(t, nil)
	writeFrom(t, client, a, "a1")
	conn := acceptInletConn(t, ln)

	b := []byte("abc")
	if n, err := conn.Write(b); n != len(b) || err != nil {
		t.Fatalf("Write() = %d, %v, want %d, nil", n, err, len(b))
	}
	copy(b, "xyz") // As the caller may reuse the buffer once Write returns

	rb := make([]byte, 8)
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if n, _, err := client.ReadFrom(rb); err != nil || string(rb[:n]) != "abc" {
		t.Errorf("ReadFrom() = %q, %v, want %q, nil", rb[:n], err, "abc")
	}
}

// TestInletWriteQueueFull ensures a write waits for room in a full write queue until its write deadline.
func TestInletWriteQueueFull(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, server := BufferedTrunk(1, nil, nil) // Holds one packet, the next one blocks the write loop
	t.Cleanup(func() { _ = client.Close() })
	ln := Inlet(server, &InletConfig{WriteQueueSize: 1, WriteTimeout: -1})
	t.Cleanup(func() { _ = ln.Close() })
	writeFrom(t, client, a, "a1")
	conn := acceptInletConn(t, ln)

	for _, data := range []string{"w1", "w2", "w3"} { // Into the trunk, then blocking the write loop, then queued
		if _, err := conn.Write([]byte(data)); err != nil {
			t.Fatalf("Write(%q) = %v, want nil", data, err)
		}
	}
	_ = conn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
	if _, err := conn.Write([]byte("w4")); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("Write() to a full queue = %v, want os.ErrDeadlineExceeded", err)
	}

	var got []string
	rb := make([]byte, 8)
	for range 3 {
		_ = client.SetReadDeadline(time.Now().Add(time.Second))
		n, _, err := client.ReadFrom(rb)
		if err != nil {
			t.Fatalf("ReadFrom() = %v, want nil", err)
		}
		got = append(got, string(rb[:n]))
	}
	if strings.Join(got, " ") != "w1 w2 w3" {
		t.Errorf("packets written = %q, want those queued", got)
	}
}

// TestInletWriteTimeout ensures the write loop drops and counts a packet it can't write within the write timeout,
// and goes on writing.
func TestInletWriteTimeout(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, server := BufferedTrunk(1, nil, nil)
	t.Cleanup(func() { _ = client.Close() })
	ln := Inlet(server, &InletConfig{WriteTimeout: 20 * time.Millisecond})
	t.Cleanup(func() { _ = ln.Close() })
	writeFrom(t, client, a, "a1")
	conn := acceptInletConn(t, ln)

	for _, data := range []string{"w1", "w2", "w3"} { // Only the first one fits into the trunk
		if _, err := conn.Write([]byte(data)); err != nil {
			t.Fatalf("Write(%q) = %v, want nil", data, err)
		}
	}
	waitInletStats(t, ln, InletStats{WriteTimedOut: 2})

	if _, err := conn.Write([]byte("w4")); err != nil {
		t.Fatal(err)
	}
	var got []string
	rb := make([]byte, 8)
	for range 2 {
		_ = client.SetReadDeadline(time.Now().Add(time.Second))
		n, _, err := client.ReadFrom(rb)
		if err != nil {
			t.Fatalf("ReadFrom() = %v, want nil", err)
		}
		got = append(got, string(rb[:n]))
	}
	if strings.Join(got, " ") != "w1 w4" {
		t.Errorf("packets written = %q, want those not timed out", got)
	}
}

// failingWrites is a FromToPacketConn whose writes to the given address fail with the given error.
type failingWrites struct {
	FromToPacketConn
	to  net.Addr
	err error
}

func (c *failingWrites) WriteToFrom(b []byte, dst, src net.Addr, idx int) (int, error) {
	if dst != nil && dst.String() == c.to.String() {
		return 0, c.err
	}
	return c.FromToPacketConn.WriteToFrom(b, dst, src, idx)
}

// TestInletWriteFailed ensures the write loop drops and counts a packet it fails to write, and goes on
// writing, unless the underlying connection is closed, which closes the inlet.
func TestInletWriteFailed(t *testing.T) {
	a, bad := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 66), Port: 66}
	setup := func(t *testing.T, err error) (FromToPacketConn, InletListener) {
		client, server := BufferedTrunk(16, nil, nil)
		t.Cleanup(func() { _ = client.Close() })
		ln := Inlet(&failingWrites{server, bad, err}, &InletConfig{DefaultOnly: true})
		t.Cleanup(func() { _ = ln.Close() })
		return client, ln
	}

	t.Run("Failed", func(t *testing.T) {
		client, ln := setup(t, errors.New("unreachable"))
		if _, err := ln.Default().WriteTo([]byte("x"), bad); err != nil {
			t.Fatalf("WriteTo(%s) = %v, want nil, as the write fails later", bad, err)
		}
		waitInletStats(t, ln, InletStats{WriteFailed: 1})

		if _, err := ln.Default().WriteTo([]byte("y"), a); err != nil {
			t.Fatal(err)
		}
		rb := make([]byte, 8)
		_ = client.SetReadDeadline(time.Now().Add(time.Second))
		if n, _, err := client.ReadFrom(rb); err != nil || string(rb[:n]) != "y" {
			t.Errorf("ReadFrom() = %q, %v, want %q, nil", rb[:n], err, "y")
		}
	})

	t.Run("Closed", func(t *testing.T) {
		_, ln := setup(t, net.ErrClosed)
		if _, err := ln.Default().WriteTo([]byte("x"), bad); err != nil {
			t.Fatal(err)
		}
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
			t.Errorf("Accept() hasn't returned, as the inlet hasn't closed")
		}
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
		client, ln := inletTrunk(t, &InletConfig{IdleTimeout: idle, DefaultConn: true})
		writeFrom(t, client, a, "a1")
		if err := acceptInletConn(t, ln).(interface{ Release() error }).Release(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * idle)

		writeFrom(t, client, a, "a2") // A new flow, offered to Accept rather than to the default connection
		if got := readInletConn(t, acceptInletConn(t, ln)); strings.Join(got, " ") != "a2" {
			t.Errorf("packets of the new flow = %q, want %q", got, "a2")
		}
		if got := readDefault(t, ln.Default()); strings.Join(got, " ") != a.String()+":a1" {
			t.Errorf("packets of the default connection = %q, want the released one only", got)
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

// TestInletConfig ensures the fields of an InletConfig not set give their defaults, and the others are applied.
func TestInletConfig(t *testing.T) {
	def := inletConfig{
		idleTimeout:      DefaultInletIdleTimeout,
		writeTimeout:     DefaultInletWriteTimeout,
		acceptQueueSize:  DefaultInletAcceptQueueSize,
		defaultQueueSize: DefaultInletDefaultQueueSize,
		maxFlows:         DefaultInletMaxFlows,
		readBufferSize:   DefaultInletReadBufferSize,
		readQueueSize:    DefaultInletReadQueueSize,
		writeQueueSize:   DefaultInletWriteQueueSize,
	}
	with := func(f func(*inletConfig)) inletConfig {
		cfg := def
		f(&cfg)
		return cfg
	}

	for _, test := range []struct {
		name   string
		config *InletConfig
		want   inletConfig
	}{
		{"Nil", nil, def},
		{"Zero", &InletConfig{}, def},
		{"Negative", &InletConfig{AcceptQueueSize: -1, DefaultQueueSize: -1, MaxFlows: -1, ReadBufferSize: -1, ReadQueueSize: -1, WriteQueueSize: -1}, def},
		{"NoTimeouts", &InletConfig{IdleTimeout: -1, WriteTimeout: -1}, with(func(cfg *inletConfig) { cfg.idleTimeout, cfg.writeTimeout = 0, 0 })},
		{"ReadBufferSizeMin", &InletConfig{ReadBufferSize: minInletReadBufferSize - 1}, with(func(cfg *inletConfig) { cfg.readBufferSize = minInletReadBufferSize })},
		{"DefaultConn", &InletConfig{DefaultConn: true}, with(func(cfg *inletConfig) { cfg.defaultConn = true })},
		{"DefaultOnly", &InletConfig{DefaultOnly: true}, with(func(cfg *inletConfig) { cfg.defaultConn, cfg.defaultOnly = true, true })},
		{"Set", &InletConfig{
			AcceptQueueSize:  1,
			ReadQueueSize:    2,
			DefaultQueueSize: 3,
			IdleTimeout:      4,
			MaxFlows:         5,
			ReadBufferSize:   minInletReadBufferSize + 6,
			WriteQueueSize:   7,
			WriteTimeout:     8,
			PacketInfo:       true,
		}, inletConfig{
			idleTimeout:      4,
			writeTimeout:     8,
			acceptQueueSize:  1,
			defaultQueueSize: 3,
			maxFlows:         5,
			readBufferSize:   minInletReadBufferSize + 6,
			readQueueSize:    2,
			writeQueueSize:   7,
			packetInfo:       true,
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.config.resolve(); got != test.want {
				t.Errorf("resolve() = %+v, want %+v", got, test.want)
			}
		})
	}
}

// TestInletClose ensures that once an inlet is closed, Accept fails, an inlet connection returns the packets
// queued and then io.EOF, writes fail, and the default connection fails too.
func TestInletClose(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := inletTrunk(t, &InletConfig{DefaultConn: true})
	writeFrom(t, client, a, "a1")
	writeFrom(t, client, a, "a2")
	conn := acceptInletConn(t, ln)
	time.Sleep(10 * time.Millisecond) // Let the read loop queue a2 too
	if err := ln.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
	if err := ln.Close(); err == nil {
		t.Errorf("Close() again = nil, want an error")
	}

	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("Accept() = %v, want net.ErrClosed", err)
	}
	b := make([]byte, 8)
	for _, want := range []string{"a1", "a2"} {
		if n, err := conn.Read(b); err != nil || string(b[:n]) != want {
			t.Errorf("Read() = %q, %v, want %q, nil", b[:n], err, want)
		}
	}
	if _, err := conn.Read(b); !errors.Is(err, io.EOF) {
		t.Errorf("Read() = %v, want io.EOF", err)
	}
	if _, err := conn.Write([]byte("x")); err == nil {
		t.Errorf("Write() = nil, want an error")
	}
	if _, _, err := ln.Default().ReadFrom(b); !errors.Is(err, io.EOF) {
		t.Errorf("ReadFrom() of the default connection = %v, want io.EOF", err)
	}
	if _, err := ln.Default().WriteTo([]byte("x"), a); err == nil {
		t.Errorf("WriteTo() of the default connection = nil, want an error")
	}
	for _, set := range []func(time.Time) error{conn.SetDeadline, conn.SetReadDeadline, conn.SetWriteDeadline,
		ln.Default().SetDeadline, ln.Default().SetReadDeadline, ln.Default().SetWriteDeadline} {
		if err := set(time.Time{}); err == nil {
			t.Errorf("setting a deadline = nil, want an error")
		}
	}
}

// TestInletUnwrap ensures Unwrap stops the inlet, but leaves the underlying connection open.
func TestInletUnwrap(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := inletTrunk(t, nil)
	pc := ln.(PacketConnWrapper).Unwrap()
	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("Accept() = %v, want net.ErrClosed", err)
	}

	writeFrom(t, client, a, "a1") // Read by nothing but the caller of Unwrap
	_ = pc.SetDeadline(time.Now().Add(time.Second))
	b := make([]byte, 8)
	if n, addr, err := pc.ReadFrom(b); err != nil || string(b[:n]) != "a1" || addr.String() != a.String() {
		t.Errorf("ReadFrom() of the unwrapped connection = %q, %v, %v, want %q, %s, nil", b[:n], addr, err, "a1", a)
	}
}

// TestInletDeadlines ensures the read and write deadlines of inlet connections and of the default connection.
func TestInletDeadlines(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := inletTrunk(t, &InletConfig{DefaultConn: true})
	writeFrom(t, client, a, "a1")
	conn := acceptInletConn(t, ln)
	_ = readInletConn(t, conn)

	past := time.Now().Add(-time.Second)
	b := make([]byte, 8)
	for name, err := range map[string]error{
		"Read":            func() error { _ = conn.SetDeadline(past); _, err := conn.Read(b); return err }(),
		"Write":           func() error { _, err := conn.Write(b); return err }(),
		"DefaultReadFrom": func() error { _ = ln.Default().SetDeadline(past); _, _, err := ln.Default().ReadFrom(b); return err }(),
		"DefaultWriteTo": func() error {
			_ = ln.Default().SetWriteDeadline(past)
			_, err := ln.Default().WriteTo(b, a)
			return err
		}(),
	} {
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("%s() after the deadline = %v, want os.ErrDeadlineExceeded", name, err)
		}
	}

	// A deadline in the future lets a waiting read time out
	_ = ln.Default().SetDeadline(time.Now().Add(20 * time.Millisecond))
	if _, _, err := ln.Default().ReadFrom(b); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("ReadFrom() of the default connection = %v, want os.ErrDeadlineExceeded", err)
	}
	if err := ln.Default().Close(); err != nil {
		t.Fatal(err)
	}
	if err := ln.Default().Close(); err == nil {
		t.Errorf("Close() of the default connection again = nil, want an error")
	}
	if _, _, err := ln.Default().ReadFrom(b); err == nil {
		t.Errorf("ReadFrom() of the closed default connection = nil, want an error")
	}
}

// TestInletDefaultWriteOther ensures the default connection finds the flow of a remote address of a network
// other than UDP, and writes from its local address.
func TestInletDefaultWriteOther(t *testing.T) {
	c, local := testAddr("c"), testAddr("local")
	client, ln := inletTrunk(t, &InletConfig{DefaultOnly: true})
	if _, err := client.WriteToFrom([]byte("c1"), local, c, 0); err != nil {
		t.Fatal(err)
	}
	_ = readDefault(t, ln.Default())
	if _, err := ln.Default().WriteTo([]byte("x"), c); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 8)
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if _, src, dst, _, err := client.ReadFromTo(b); err != nil || src != local || dst != c {
		t.Errorf("ReadFromTo() = from %v to %v, %v, want from %s to %s, nil", src, dst, err, local, c)
	}
}

// TestInletUDPLoopback ensures an inlet over a dual-stack UDP socket tells flows apart by their destination
// addresses too, and replies from the address each flow came to, while an inlet without packet info
// tells them apart by their source addresses alone.
func TestInletUDPLoopback(t *testing.T) {
	switch runtime.GOOS {
	case "js", "plan9", "wasip1":
		t.Skipf("not supported on %s", runtime.GOOS)
	}

	for _, test := range []struct {
		name   string
		config *InletConfig
		info   bool
	}{
		{"PacketInfo", &InletConfig{PacketInfo: true}, true},
		{"NoPacketInfo", nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv6unspecified})
			if err != nil {
				t.Skipf("ListenUDP() = %v", err) // E.g. no IPv6 support
			}
			ln := Inlet(pc, test.config)
			defer func() { _ = ln.Close() }()
			if ln.Addr() != pc.LocalAddr() {
				t.Errorf("Addr() = %v, want %v", ln.Addr(), pc.LocalAddr())
			}
			port := pc.LocalAddr().(*net.UDPAddr).AddrPort().Port()

			for _, to := range []netip.Addr{netip.MustParseAddr("127.0.0.1"), netip.IPv6Loopback()} {
				network := map[bool]string{true: "udp4", false: "udp6"}[to.Is4()]
				client, err := net.ListenUDP(network, nil)
				if err != nil {
					t.Skipf("ListenUDP(%s) = %v", network, err)
				}
				defer func() { _ = client.Close() }()
				dst := netip.AddrPortFrom(to, port)
				if _, err = client.WriteToUDPAddrPort([]byte("ping"), dst); err != nil {
					t.Fatal(err)
				}

				conn := acceptInletConn(t, ln)
				wantLocal := pc.LocalAddr().String()
				if test.info {
					wantLocal = dst.String()
				}
				if local := conn.LocalAddr().String(); local != wantLocal {
					t.Errorf("LocalAddr() = %s, want %s", local, wantLocal)
				}
				if got := readInletConn(t, conn); strings.Join(got, " ") != "ping" {
					t.Errorf("packets of the flow = %q, want %q", got, "ping")
				}

				if _, err = conn.Write([]byte("pong")); err != nil {
					t.Fatal(err)
				}
				b := make([]byte, 8)
				_ = client.SetReadDeadline(time.Now().Add(time.Second))
				n, from, err := client.ReadFromUDPAddrPort(b)
				if err != nil || string(b[:n]) != "pong" || from.Addr().Unmap() != to {
					t.Errorf("client.ReadFromUDPAddrPort() = %q from %s, %v, want %q from %s, nil", b[:n], from, err, "pong", to)
				}
			}
		})
	}
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
