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
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestInlet runs the tests of inlets, of their inlet connections and of their fallback connections.
func TestInlet(t *testing.T) {
	t.Run(testInletBlockedReadsName, testInletBlockedReads)
	t.Run(testInletBlockedWritesName, testInletBlockedWrites)
	t.Run(testInletCloseName, testInletClose)
	t.Run(testInletCloseErrorName, testInletCloseError)
	t.Run(testInletClosedErrorsName, testInletClosedErrors)
	t.Run(testInletConcurrentReadsName, testInletConcurrentReads)
	t.Run(testInletConfigName, testInletConfig)
	t.Run(testInletConnCloseAfterStopName, testInletConnCloseAfterStop)
	t.Run(testInletConnCloseDropsName, testInletConnCloseDrops)
	t.Run(testInletConnWriteToConnectedName, testInletConnWriteToConnected)
	t.Run(testInletDeadlinesName, testInletDeadlines)
	t.Run(testInletEmptyPacketsName, testInletEmptyPackets)
	t.Run(testInletFallbackDropsName, testInletFallbackDrops)
	t.Run(testInletFallbackFromToName, testInletFallbackFromTo)
	t.Run(testInletFallbackWriteName, testInletFallbackWrite)
	t.Run(testInletFallbackWriteCopiesAddrsName, testInletFallbackWriteCopiesAddrs)
	t.Run(testInletFallbackWriteOtherNetworkName, testInletFallbackWriteOtherNetwork)
	t.Run(testInletFallbackWriteUnmappedName, testInletFallbackWriteUnmapped)
	t.Run(testInletIdleTimeoutName, testInletIdleTimeout)
	t.Run(testInletNegativeReadName, testInletNegativeRead)
	t.Run(testInletPacketInfoFlowsName, testInletPacketInfoFlows)
	t.Run(testInletPayloadSizesName, testInletPayloadSizes)
	t.Run(testInletReadCopiesAddrsName, testInletReadCopiesAddrs)
	t.Run(testInletReadDropsName, testInletReadDrops)
	t.Run(testInletReleaseName, testInletRelease)
	t.Run(testInletReleaseWithoutFallbackName, testInletReleaseWithoutFallback)
	t.Run(testInletShortBufferName, testInletShortBuffer)
	t.Run(testInletStreamListenerName, testInletStreamListener)
	t.Run(testInletTrunkFlowsName, testInletTrunkFlows)
	t.Run(testInletUDPSocketFlowsName, testInletUDPSocketFlows)
	t.Run(testInletUDPTruncatedName, testInletUDPTruncated)
	t.Run(testInletUnderlyingDeadlineName, testInletUnderlyingDeadline)
	t.Run(testInletUnwrapName, testInletUnwrap)
	t.Run(testInletUnwrapUDPName, testInletUnwrapUDP)
	t.Run(testInletWriteCopiesBytesName, testInletWriteCopiesBytes)
	t.Run(testInletWriteFailedName, testInletWriteFailed)
	t.Run(testInletWriteQueueFullName, testInletWriteQueueFull)
	t.Run(testInletWriteTimeoutName, testInletWriteTimeout)
	t.Run(testInletWriteUnsentName, testInletWriteUnsent)
}

// testInletPayloadSizes ensures an inlet over each kind of pipe and over UDP sockets reads the packets written
// to it, with payloads from the smallest size to the largest, and counts those it drops.
func testInletPayloadSizes(t *testing.T) {
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
		checkInletPayloads(t, ctor, DefaultInletAcceptQueueSize, DefaultInletReadQueueSize, packetSizeMax, writeBufferSize, packetCount)
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

// checkInletPayloads writes packetCount packets of writeBufferSize bytes from one connection of a pair made
// by ctor to an inlet over the other, reads them from the inlet connections it accepts, and checks that each
// packet has been read, or counted as dropped.
func checkInletPayloads(t *testing.T, ctor packetConnPairCtor, acceptQueueSize, readQueueSize, readBufferSize, writeBufferSize, packetCount int) {
	t.Helper()
	pc1, pc2 := ctor()

	timeout := 50 * time.Millisecond
	chr, chw, done := make(chan int), make(chan int), make(chan struct{})
	var stats InletStats // Of the inlet, once closed

	connHandler := func(mc InletConn, ln InletListener, wg *sync.WaitGroup) {
		defer wg.Done()

		lAddr, rAddr := mc.LocalAddr(), mc.RemoteAddr()
		me := fmt.Sprintf("inlet connection %v <- %v", lAddr, rAddr)
		logfIf(t, testPacketInletDebug, "%v: started", me)

		defer func() { _ = mc.Close() }()

		b := make([]byte, readBufferSize)
		var addr net.Addr
		var err error
		var n int
		for {
			// Once the inlet has closed, the packets queued are still read, then net.ErrClosed
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

		ln := Inlet(pc, &InletConfig{
			AcceptQueueSize: acceptQueueSize,
			ReadQueueSize:   readQueueSize,
			ReadBufferSize:  readBufferSize,
			PacketInfo:      true,
		})
		defer func() {
			_ = ln.Close()
			stats = ln.Stats()
		}()

		for {
			conn, err := ln.Accept()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					logfIf(t, testPacketInletDebug, "%v: ln.Accept() = %v", me, err)
				}
				break
			}

			wg.Add(1)
			go connHandler(conn, ln, wg)
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

// testPacketInletDebug makes checkInletPayloads log what each of its goroutines does.
const testPacketInletDebug = false

// testAddr is an address of a network other than UDP, e.g. of a flow over a trunk.
type testAddr string

// Network returns the network of every testAddr.
func (a testAddr) Network() string { return "test" }

// String returns the address itself.
func (a testAddr) String() string { return string(a) }

// waitInletStats waits until the inlet has dropped the given numbers of packets, or fails.
func waitInletStats(t *testing.T, ln InletListener, want InletStats) {
	t.Helper()
	s := ln
	deadline := time.Now().Add(time.Second)
	for got := s.Stats(); got != want; got = s.Stats() {
		if time.Now().After(deadline) {
			t.Fatalf("Stats() = %+v, want %+v", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// acceptInletConn accepts an inlet connection, or fails if there is none within a second.
func acceptInletConn(t *testing.T, ln InletListener) InletConn {
	t.Helper()
	type result struct {
		c   InletConn
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

// releaseInletConn accepts the next inlet connection and releases it, handing its flow over to the fallback
// connection.
func releaseInletConn(t *testing.T, ln InletListener) {
	t.Helper()
	if err := acceptInletConn(t, ln).Release(); err != nil {
		t.Fatalf("Release() = %v, want nil", err)
	}
}

// readInletConn returns the packets read from an inlet connection until none comes for a while.
func readInletConn(t *testing.T, c InletConn) []string {
	t.Helper()
	var pkts []string
	b := make([]byte, 64)
	for {
		_ = c.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		n, _, err := c.ReadFrom(b)
		if err != nil {
			return pkts
		}
		pkts = append(pkts, string(b[:n]))
	}
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

// readFallback returns the packets read from a fallback connection, with their remote addresses,
// until none comes for a while.
func readFallback(t *testing.T, d PacketConn) []string {
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

// testInletConfig ensures the fields of an InletConfig not set give their defaults, and the others are applied.
func testInletConfig(t *testing.T) {
	def := inletConfig{
		idleTimeout:       DefaultInletIdleTimeout,
		writeTimeout:      DefaultInletWriteTimeout,
		acceptQueueSize:   DefaultInletAcceptQueueSize,
		fallbackQueueSize: DefaultInletFallbackQueueSize,
		maxFlows:          DefaultInletMaxFlows,
		readBufferSize:    DefaultInletReadBufferSize,
		readQueueSize:     DefaultInletReadQueueSize,
		writeQueueSize:    DefaultInletWriteQueueSize,
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
		{"Negative", &InletConfig{AcceptQueueSize: -1, FallbackQueueSize: -1, MaxFlows: -1, ReadBufferSize: -1, ReadQueueSize: -1, WriteQueueSize: -1}, def},
		{"NoTimeouts", &InletConfig{IdleTimeout: -1, WriteTimeout: -1}, with(func(cfg *inletConfig) { cfg.idleTimeout, cfg.writeTimeout = 0, 0 })},
		{"ReadBufferSizeMin", &InletConfig{ReadBufferSize: minInletReadBufferSize - 1}, with(func(cfg *inletConfig) { cfg.readBufferSize = minInletReadBufferSize })},
		{"FallbackConn", &InletConfig{FallbackConn: true}, with(func(cfg *inletConfig) { cfg.fallbackConn = true })},
		{"Set", &InletConfig{
			AcceptQueueSize:   1,
			ReadQueueSize:     2,
			FallbackQueueSize: 3,
			IdleTimeout:       4,
			MaxFlows:          5,
			ReadBufferSize:    minInletReadBufferSize + 6,
			WriteQueueSize:    7,
			WriteTimeout:      8,
			PacketInfo:        true,
		}, inletConfig{
			idleTimeout:       4,
			writeTimeout:      8,
			acceptQueueSize:   1,
			fallbackQueueSize: 3,
			maxFlows:          5,
			readBufferSize:    minInletReadBufferSize + 6,
			readQueueSize:     2,
			writeQueueSize:    7,
			packetInfo:        true,
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.config.resolve(); got != test.want {
				t.Errorf("resolve() = %+v, want %+v", got, test.want)
			}
		})
	}
}

// testInletClose ensures that once an inlet is closed, Accept fails, an inlet connection returns the packets
// queued and then net.ErrClosed, writes fail, the fallback connection fails too, and deadlines can still be set.
func testInletClose(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := inletTrunk(t, &InletConfig{FallbackConn: true})
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
		if n, _, err := conn.ReadFrom(b); err != nil || string(b[:n]) != want {
			t.Errorf("Read() = %q, %v, want %q, nil", b[:n], err, want)
		}
	}
	if _, _, err := conn.ReadFrom(b); !errors.Is(err, net.ErrClosed) {
		t.Errorf("ReadFrom() = %v, want net.ErrClosed", err)
	}
	if _, err := conn.WriteTo([]byte("x"), conn.RemoteAddr()); err == nil {
		t.Errorf("Write() = nil, want an error")
	}
	if _, _, err := ln.Fallback().ReadFrom(b); !errors.Is(err, net.ErrClosed) {
		t.Errorf("ReadFrom() of the fallback connection = %v, want net.ErrClosed", err)
	}
	if _, err := ln.Fallback().WriteTo([]byte("x"), a); err == nil {
		t.Errorf("WriteTo() of the fallback connection = nil, want an error")
	}
	for _, set := range []func(time.Time) error{conn.SetDeadline, conn.SetReadDeadline, conn.SetWriteDeadline,
		ln.Fallback().SetDeadline, ln.Fallback().SetReadDeadline, ln.Fallback().SetWriteDeadline} {
		if err := set(time.Time{}); err != nil { // As the connections are open, though the inlet isn't
			t.Errorf("setting a deadline = %v, want nil", err)
		}
	}
}

// failingClose is a FromToPacketConn that closes, but reports an error doing so.
type failingClose struct {
	FromToPacketConn
	err error
}

// Close closes the connection embedded, and returns the error.
func (c failingClose) Close() error {
	_ = c.FromToPacketConn.Close()
	return c.err
}

// testInletCloseError ensures Close returns the error of closing the underlying connection,
// and net.ErrClosed once the inlet has stopped.
func testInletCloseError(t *testing.T) {
	client, server := BufferedTrunk(16, nil, nil)
	defer func() { _ = client.Close() }()
	errClose := errors.New("close failed")
	ln := Inlet(failingClose{server, errClose}, nil)
	if err := ln.Close(); !errors.Is(err, errClose) {
		t.Errorf("Close() = %v, want %v", err, errClose)
	}
	if err := ln.Close(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("Close() again = %v, want net.ErrClosed", err)
	}
}

// testInletUnderlyingDeadline ensures an inlet clears the deadlines of the connection it takes over, so that
// a deadline passed neither keeps the read loop from reading, nor fails writes without a write timeout.
func testInletUnderlyingDeadline(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, server := BufferedTrunk(16, nil, nil)
	t.Cleanup(func() { _ = client.Close() })
	_ = server.SetDeadline(time.Now().Add(-time.Second))
	ln := Inlet(server, &InletConfig{WriteTimeout: -1})
	t.Cleanup(func() { _ = ln.Close() })

	writeFrom(t, client, a, "a1")
	conn := acceptInletConn(t, ln)
	if _, err := conn.WriteTo([]byte("x"), nil); err != nil {
		t.Fatalf("WriteTo() = %v, want nil", err)
	}
	b := make([]byte, 8)
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if n, _, err := client.ReadFrom(b); err != nil || string(b[:n]) != "x" {
		t.Errorf("ReadFrom() = %q, %v, want %q, nil", b[:n], err, "x")
	}
	waitInletStats(t, ln, InletStats{})
}

// testInletBlockedReads ensures a read waiting on an empty inlet connection, or on the fallback connection,
// returns net.ErrClosed once the inlet stops.
func testInletBlockedReads(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := inletTrunk(t, &InletConfig{FallbackConn: true})
	writeFrom(t, client, a, "a1")
	conn := acceptInletConn(t, ln)
	_ = readInletConn(t, conn)            // So the next read waits
	_ = conn.SetReadDeadline(time.Time{}) // Which readInletConn has set

	errs := make(chan error, 2)
	for _, pc := range []PacketConn{conn, ln.Fallback()} {
		go func() {
			_, _, err := pc.ReadFrom(make([]byte, 8))
			errs <- err
		}()
	}
	time.Sleep(20 * time.Millisecond) // Let both reads wait
	if err := ln.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
	for range 2 {
		select {
		case err := <-errs:
			if !errors.Is(err, net.ErrClosed) {
				t.Errorf("ReadFrom() = %v, want net.ErrClosed", err)
			}
		case <-time.After(time.Second):
			t.Fatalf("ReadFrom() hasn't returned once the inlet has stopped")
		}
	}
}

// testInletUnwrap ensures Unwrap stops the inlet, but leaves the underlying connection open.
func testInletUnwrap(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := inletTrunk(t, nil)
	pc := ln.Unwrap()
	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("Accept() = %v, want net.ErrClosed", err)
	}

	writeFrom(t, client, a, "a1") // Read by nothing but the caller of Unwrap
	b := make([]byte, 8)          // Read with no deadline set, which a deadline in the past would fail
	if n, addr, err := pc.ReadFrom(b); err != nil || string(b[:n]) != "a1" || addr.String() != a.String() {
		t.Errorf("ReadFrom() of the unwrapped connection = %q, %v, %v, want %q, %s, nil", b[:n], addr, err, "a1", a)
	}
}

// testInletUnwrapUDP ensures Unwrap returns the UDP connection given to Inlet, rather than the wrapper
// the inlet reads and writes through, with packet info or without.
func testInletUnwrapUDP(t *testing.T) {
	switch runtime.GOOS {
	case "js", "plan9", "wasip1":
		t.Skipf("not supported on %s", runtime.GOOS)
	}

	for _, info := range []bool{true, false} {
		t.Run(map[bool]string{true: "PacketInfo", false: "NoPacketInfo"}[info], func(t *testing.T) {
			pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = pc.Close() }()
			if got := Inlet(pc, &InletConfig{PacketInfo: info}).Unwrap(); got != pc {
				t.Errorf("Unwrap() = %T, want the %T given", got, pc)
			}

			// Write to itself and read back with no deadlines set, which deadlines in the past would fail
			if _, err := pc.WriteTo([]byte("x"), pc.LocalAddr()); err != nil {
				t.Fatalf("WriteTo() = %v, want nil", err)
			}
			b := make([]byte, 8)
			if n, _, err := pc.ReadFrom(b); err != nil || string(b[:n]) != "x" {
				t.Errorf("ReadFrom() = %q, %v, want %q, nil", b[:n], err, "x")
			}
		})
	}
}

// testInletStreamListener ensures an inlet wrapped with PacketToStreamListener accepts its inlet connections
// as StreamConns, which read and write the packets of their flows, and unwraps to the inlet.
func testInletStreamListener(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := inletTrunk(t, nil)
	sl := PacketToStreamListener(ln) // Infers InletConn
	if sl.Addr() != ln.Addr() {
		t.Errorf("Addr() = %v, want %v", sl.Addr(), ln.Addr())
	}

	writeFrom(t, client, a, "a1")
	conn, err := sl.Accept()
	if err != nil {
		t.Fatalf("Accept() = %v, want nil", err)
	}
	if conn.RemoteAddr().String() != a.String() {
		t.Errorf("RemoteAddr() = %v, want %s", conn.RemoteAddr(), a)
	}
	b := make([]byte, 8)
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if n, err := conn.Read(b); err != nil || string(b[:n]) != "a1" {
		t.Errorf("Read() = %q, %v, want %q, nil", b[:n], err, "a1")
	}
	if _, err := conn.Write([]byte("x")); err != nil {
		t.Fatalf("Write() = %v, want nil", err)
	}
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if n, _, dst, _, err := client.ReadFromTo(b); err != nil || string(b[:n]) != "x" || dst.String() != a.String() {
		t.Errorf("ReadFromTo() = %q to %v, %v, want %q to %s, nil", b[:n], dst, err, "x", a)
	}

	if got := sl.(PacketListenerWrapper[InletConn]).Unwrap(); got != ln {
		t.Errorf("Unwrap() = %v, want the inlet", got)
	}
	if err := sl.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
	if _, err := sl.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("Accept() after Close() = %v, want net.ErrClosed", err)
	}
}

// testInletUDPSocketFlows ensures an inlet over a dual-stack UDP socket tells flows apart by their destination
// addresses too, and replies from the address each flow came to, while an inlet without packet info
// tells them apart by their source addresses alone.
func testInletUDPSocketFlows(t *testing.T) {
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

				if _, err = conn.WriteTo([]byte("pong"), conn.RemoteAddr()); err != nil {
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

const (
	testInletBlockedReadsName              = "BlockedReads"
	testInletBlockedWritesName             = "BlockedWrites"
	testInletCloseName                     = "Close"
	testInletCloseErrorName                = "CloseError"
	testInletClosedErrorsName              = "ClosedErrors"
	testInletConcurrentReadsName           = "ConcurrentReads"
	testInletConfigName                    = "Config"
	testInletConnCloseAfterStopName        = "ConnCloseAfterStop"
	testInletConnCloseDropsName            = "ConnCloseDrops"
	testInletConnWriteToConnectedName      = "ConnWriteToConnected"
	testInletDeadlinesName                 = "Deadlines"
	testInletEmptyPacketsName              = "EmptyPackets"
	testInletFallbackDropsName             = "FallbackDrops"
	testInletFallbackFromToName            = "FallbackFromTo"
	testInletFallbackWriteName             = "FallbackWrite"
	testInletFallbackWriteCopiesAddrsName  = "FallbackWriteCopiesAddrs"
	testInletFallbackWriteOtherNetworkName = "FallbackWriteOtherNetwork"
	testInletFallbackWriteUnmappedName     = "FallbackWriteUnmapped"
	testInletIdleTimeoutName               = "IdleTimeout"
	testInletNegativeReadName              = "NegativeRead"
	testInletPacketInfoFlowsName           = "PacketInfoFlows"
	testInletPayloadSizesName              = "PayloadSizes"
	testInletReadCopiesAddrsName           = "ReadCopiesAddrs"
	testInletReadDropsName                 = "ReadDrops"
	testInletReleaseName                   = "Release"
	testInletReleaseWithoutFallbackName    = "ReleaseWithoutFallback"
	testInletShortBufferName               = "ShortBuffer"
	testInletStreamListenerName            = "StreamListener"
	testInletTrunkFlowsName                = "TrunkFlows"
	testInletUDPSocketFlowsName            = "UDPSocketFlows"
	testInletUDPTruncatedName              = "UDPTruncated"
	testInletUnderlyingDeadlineName        = "UnderlyingDeadline"
	testInletUnwrapName                    = "Unwrap"
	testInletUnwrapUDPName                 = "UnwrapUDP"
	testInletWriteCopiesBytesName          = "WriteCopiesBytes"
	testInletWriteFailedName               = "WriteFailed"
	testInletWriteQueueFullName            = "WriteQueueFull"
	testInletWriteTimeoutName              = "WriteTimeout"
	testInletWriteUnsentName               = "WriteUnsent"
)
