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
	"strings"
	"sync"
	"testing"
	"time"
)

// BenchmarkSplit measures a round trip through a split connection, and through the fallback connection
// once the flow is released, over each kind of underlying connection the splitter tells apart: a PacketConn,
// a FromToPacketConn, a UDPConn and a FromToUDPConn. Each operation is a write to the splitter, the read
// of the packet from the split connection or with ReadFrom from the fallback connection, the write of
// the packet back and its read. A trunk of the first two kinds takes the same allocations of its own in each.
func BenchmarkSplit(b *testing.B) {
	type peer struct {
		write, read func(p []byte) error
	}
	trunk := func(b *testing.B, wrap func(FromToPacketConn) PacketConn) (peer, PacketConn) {
		c, s := BufferedTrunk(16, nil, nil)
		b.Cleanup(func() { _ = c.Close() })
		_ = c.SetReadDeadline(time.Now().Add(time.Minute)) // Fail rather than hang
		src := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1).To4(), Port: 1}
		return peer{
			write: func(p []byte) error { _, err := c.WriteToFrom(p, nil, src, 0); return err },
			read:  func(p []byte) error { _, _, err := c.ReadFrom(p); return err },
		}, wrap(s)
	}
	udp := func(b *testing.B) (peer, PacketConn) {
		skipIfNoUDP(b)
		c, s := listenLoopbackUDP(b), listenLoopbackUDP(b)
		_ = c.SetReadDeadline(time.Now().Add(time.Minute)) // Fail rather than hang if a datagram is lost
		dst := s.LocalAddr().(*net.UDPAddr).AddrPort()
		return peer{
			write: func(p []byte) error { _, err := c.WriteToUDPAddrPort(p, dst); return err },
			read:  func(p []byte) error { _, _, err := c.ReadFromUDPAddrPort(p); return err },
		}, s
	}

	for _, bench := range []struct {
		name   string
		conn   func(b *testing.B) (peer, PacketConn)
		config *SplitConfig
	}{
		{"PacketConn", func(b *testing.B) (peer, PacketConn) {
			return trunk(b, func(c FromToPacketConn) PacketConn { return FakeAddressPacketConn(c, nil, nil) })
		}, nil},
		{"FromToPacketConn", func(b *testing.B) (peer, PacketConn) {
			return trunk(b, func(c FromToPacketConn) PacketConn { return c })
		}, nil},
		{"UDPConn", udp, nil},
		{"FromToUDPConn", udp, &SplitConfig{PacketInfo: true}},
	} {
		run := func(b *testing.B, fallback bool) {
			c, pc := bench.conn(b)
			var config SplitConfig
			if bench.config != nil {
				config = *bench.config
			}
			config.FallbackConn = fallback
			ln := Split(pc, &config)
			b.Cleanup(func() { _ = ln.Close() })

			p := make([]byte, 64)
			if err := c.write(p); err != nil { // Starts the flow
				b.Fatal(err)
			}
			sc, err := ln.AcceptSplit()
			if err != nil {
				b.Fatal(err)
			}
			_ = sc.SetReadDeadline(time.Now().Add(time.Minute))
			echo := func() error {
				if _, err := sc.Read(p); err != nil {
					return err
				}
				if _, err := sc.Write(p); err != nil {
					return err
				}
				return c.read(p)
			}
			if fallback { // Hands the flow, with the packet, over to the fallback connection, as a QUIC server reads it
				if err := sc.Release(); err != nil {
					b.Fatal(err)
				}
				fb := ln.Fallback()
				_ = fb.SetReadDeadline(time.Now().Add(time.Minute))
				echo = func() error {
					_, addr, err := fb.ReadFrom(p)
					if err != nil {
						return err
					}
					if _, err := fb.WriteTo(p, addr); err != nil {
						return err
					}
					return c.read(p)
				}
			}
			if err := echo(); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			for b.Loop() {
				if err := c.write(p); err != nil {
					b.Fatal(err)
				}
				if err := echo(); err != nil {
					b.Fatal(err)
				}
			}
		}
		b.Run(bench.name, func(b *testing.B) {
			b.Run("SplitConn", func(b *testing.B) { run(b, false) })
			b.Run("Fallback", func(b *testing.B) { run(b, true) })
		})
	}
}

// TestSplit runs the tests of splitters, of their split connections and of their fallback connections.
func TestSplit(t *testing.T) {
	t.Run(testSplitAcceptName, testSplitAccept)
	t.Run(testSplitBlockedReadsName, testSplitBlockedReads)
	t.Run(testSplitBlockedWritesName, testSplitBlockedWrites)
	t.Run(testSplitCloseName, testSplitClose)
	t.Run(testSplitCloseErrorName, testSplitCloseError)
	t.Run(testSplitClosedErrorsName, testSplitClosedErrors)
	t.Run(testSplitConcurrentReadsName, testSplitConcurrentReads)
	t.Run(testSplitConfigName, testSplitConfig)
	t.Run(testSplitConnCloseAfterStopName, testSplitConnCloseAfterStop)
	t.Run(testSplitConnCloseDropsName, testSplitConnCloseDrops)
	t.Run(testSplitConnWriteToConnectedName, testSplitConnWriteToConnected)
	t.Run(testSplitDeadlinesName, testSplitDeadlines)
	t.Run(testSplitEmptyPacketsName, testSplitEmptyPackets)
	t.Run(testSplitFallbackDropsName, testSplitFallbackDrops)
	t.Run(testSplitFallbackFromToName, testSplitFallbackFromTo)
	t.Run(testSplitFallbackWriteName, testSplitFallbackWrite)
	t.Run(testSplitFallbackWriteCopiesAddrsName, testSplitFallbackWriteCopiesAddrs)
	t.Run(testSplitFallbackWriteOtherNetworkName, testSplitFallbackWriteOtherNetwork)
	t.Run(testSplitFallbackWriteUnmappedName, testSplitFallbackWriteUnmapped)
	t.Run(testSplitIdleTimeoutName, testSplitIdleTimeout)
	t.Run(testSplitModeName, testSplitMode)
	t.Run(testSplitNegativeReadName, testSplitNegativeRead)
	t.Run(testSplitPacketInfoFlowsName, testSplitPacketInfoFlows)
	t.Run(testSplitPayloadSizesName, testSplitPayloadSizes)
	t.Run(testSplitReadCopiesAddrsName, testSplitReadCopiesAddrs)
	t.Run(testSplitReadDropsName, testSplitReadDrops)
	t.Run(testSplitReleaseName, testSplitRelease)
	t.Run(testSplitReleaseWithoutFallbackName, testSplitReleaseWithoutFallback)
	t.Run(testSplitShortBufferName, testSplitShortBuffer)
	t.Run(testSplitStopName, testSplitStop)
	t.Run(testSplitTrunkFlowsName, testSplitTrunkFlows)
	t.Run(testSplitUDPMaxDatagramName, testSplitUDPMaxDatagram)
	t.Run(testSplitUDPSocketFlowsName, testSplitUDPSocketFlows)
	t.Run(testSplitUDPTruncatedName, testSplitUDPTruncated)
	t.Run(testSplitUnderlyingDeadlineName, testSplitUnderlyingDeadline)
	t.Run(testSplitUnwrapName, testSplitUnwrap)
	t.Run(testSplitUnwrapUDPName, testSplitUnwrapUDP)
	t.Run(testSplitWriteCopiesBytesName, testSplitWriteCopiesBytes)
	t.Run(testSplitWriteFailedName, testSplitWriteFailed)
	t.Run(testSplitWriteQueueFullName, testSplitWriteQueueFull)
	t.Run(testSplitWriteTimeoutName, testSplitWriteTimeout)
	t.Run(testSplitWriteUnsentName, testSplitWriteUnsent)
}

// testSplitPayloadSizes ensures a splitter over each kind of pipe and trunk and over UDP sockets reads the packets
// written to it, with payloads from the smallest size to the largest, and counts those it drops.
func testSplitPayloadSizes(t *testing.T) {
	packetCount := 1 << 13

	type pair struct {
		name                                   string
		ctor                                   packetConnPairCtor
		minWBS, minSafeWBS, maxSafeWBS, maxWBS int // WBS - write buffer size
	}
	pairs := []pair{
		{name: "StandardUnbufferedSyncPipe", ctor: func(testing.TB) (PacketConn, PacketConn) {
			c1, c2 := net.Pipe()
			return StreamToPacketConn(c1), StreamToPacketConn(c2)
		}, minWBS: packetSizeMin, minSafeWBS: packetSizeMinSafe, maxSafeWBS: packetSizeMaxSafe, maxWBS: packetSizeMax},
		// The smallest packet over our pipes takes a byte, as they send nothing for an empty write, like TCP
		{name: "HybridUnbufferedSyncPipe", ctor: func(testing.TB) (PacketConn, PacketConn) {
			c1, c2 := UnbufferedPipe(false)
			return StreamToPacketConn(c1), StreamToPacketConn(c2)
		}, minWBS: 1, minSafeWBS: packetSizeMinSafe, maxSafeWBS: packetSizeMaxSafe, maxWBS: packetSizeMax},
		{name: "HybridUnbufferedSemiPipe", ctor: func(testing.TB) (PacketConn, PacketConn) {
			c1, c2 := UnbufferedPipe(true)
			return StreamToPacketConn(c1), StreamToPacketConn(c2)
		}, minWBS: 1, minSafeWBS: packetSizeMinSafe, maxSafeWBS: packetSizeMaxSafe, maxWBS: packetSizeMax},
		{name: "HybridBufferedAsyncPipe", ctor: func(testing.TB) (PacketConn, PacketConn) {
			c1, c2 := BufferedPipe(10)
			return StreamToPacketConn(c1), StreamToPacketConn(c2)
		}, minWBS: 1, minSafeWBS: packetSizeMinSafe, maxSafeWBS: packetSizeMaxSafe, maxWBS: packetSizeMax},
		// Unlike our pipes, trunks keep empty packets, as UDP does, and the splitter reads them as FromToPacketConns
		{name: "HybridUnbufferedTrunk", ctor: func(testing.TB) (PacketConn, PacketConn) {
			return UnbufferedTrunk(nil, nil)
		}, minWBS: packetSizeMin, minSafeWBS: packetSizeMinSafe, maxSafeWBS: packetSizeMaxSafe, maxWBS: packetSizeMax},
		{name: "HybridBufferedTrunk", ctor: func(testing.TB) (PacketConn, PacketConn) {
			return BufferedTrunk(10, nil, nil)
		}, minWBS: packetSizeMin, minSafeWBS: packetSizeMinSafe, maxSafeWBS: packetSizeMaxSafe, maxWBS: packetSizeMax},
		{name: "LocalUDP4Socket", ctor: func(tb testing.TB) (PacketConn, PacketConn) {
			return newLocalPacketListeners(tb, "udp4")
		}, minWBS: udp4PayloadSizeMin, minSafeWBS: udp4PayloadSizeMinSafe, maxSafeWBS: udp4PayloadSizeMaxSafe, maxWBS: udp4PayloadSizeMax},
		{name: "LocalUDP6Socket", ctor: func(tb testing.TB) (PacketConn, PacketConn) {
			return newLocalPacketListeners(tb, "udp6")
		}, minWBS: udp6PayloadSizeMin, minSafeWBS: udp6PayloadSizeMinSafe, maxSafeWBS: udp6PayloadSizeMaxSafe, maxWBS: udp6PayloadSizeMax},
	}

	testFunc := func(t *testing.T, ctor packetConnPairCtor, writeBufferSize int) {
		checkSplitPayloads(t, ctor, DefaultSplitAcceptQueueSize, DefaultSplitReadQueueSize, packetSizeMax, writeBufferSize, packetCount)
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

// checkSplitPayloads writes packetCount packets of writeBufferSize bytes from one connection of a pair made
// by ctor to a splitter over the other, reads them from the split connections it accepts, and checks that each
// packet has been read, or counted as dropped.
func checkSplitPayloads(t *testing.T, ctor packetConnPairCtor, acceptQueueSize, readQueueSize, readBufferSize, writeBufferSize, packetCount int) {
	t.Helper()
	pc1, pc2 := ctor(t)

	timeout := 50 * time.Millisecond
	chr, chw, done := make(chan int), make(chan int), make(chan struct{})
	var stats SplitStats // Of the splitter, once closed

	connHandler := func(mc SplitConn, ln SplitListener, wg *sync.WaitGroup) {
		defer wg.Done()

		lAddr, rAddr := mc.LocalAddr(), mc.RemoteAddr()
		me := fmt.Sprintf("split connection %v <- %v", lAddr, rAddr)
		logfIf(t, testPacketSplitDebug, "%v: started", me)

		defer func() { _ = mc.Close() }()

		b := make([]byte, readBufferSize)
		var addr net.Addr
		var err error
		var n int
		for {
			// Once the splitter has closed, the packets queued are still read, then net.ErrClosed
			err = mc.SetReadDeadline(time.Now().Add(2 * timeout))
			if err != nil {
				t.Errorf("%v: pc.SetReadDeadline() = %v, want nil", me, err)
				break
			}

			n, addr, err = mc.ReadFrom(b)
			if n == 0 && err != nil {
				logfIf(t, testPacketSplitDebug, "%v: pc.ReadFrom() = %v", me, err)
				break
			}

			if got, want := addr.String(), rAddr.String(); got != want {
				t.Errorf("%v: pc.ReadFrom() = %v, want %v", me, got, want)
			}

			chr <- n
			logfIf(t, testPacketSplitDebug, "%v: read %d bytes", me, n)
		}

		logfIf(t, testPacketSplitDebug, "%v: stopped", me)
	}

	serverHandler := func(pc PacketConn, wg *sync.WaitGroup) {
		defer wg.Done()

		lAddr := pc.LocalAddr()
		me := fmt.Sprintf("splitter %v", lAddr)
		logfIf(t, testPacketSplitDebug, "%v: started", me)

		ln := Split(pc, &SplitConfig{
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
			conn, err := ln.AcceptSplit()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					logfIf(t, testPacketSplitDebug, "%v: ln.AcceptSplit() = %v", me, err)
				}
				break
			}

			wg.Add(1)
			go connHandler(conn, ln, wg)
		}

		logfIf(t, testPacketSplitDebug, "%v: stopped", me)
	}

	clientHandler := func(pc PacketConn, to net.Addr, wg *sync.WaitGroup) {
		defer wg.Done()

		lAddr, rAddr := pc.LocalAddr(), to
		me := fmt.Sprintf("client %v -> %v", lAddr, rAddr)
		logfIf(t, testPacketSplitDebug, "%v: started", me)

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
			logfIf(t, testPacketSplitDebug, "%v: wrote %d bytes", me, n)
		}

		time.AfterFunc(timeout, func() {
			close(done)
		})

		logfIf(t, testPacketSplitDebug, "%v: stopped", me)
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

	// The splitter drops a packet rather than wait for a slow reader, and counts it
	dropped := int(stats.ConnQueueFull + stats.AcceptQueueFull + stats.TableFull + stats.NoDestination)
	message := fmt.Sprintf("r/w statistics: count %d+%d/%d, bytes %d+%d/%d", acr, dropped, acw, anr, dropped*writeBufferSize, anw)
	if pc2.LocalAddr().Network() == "udp" {
		// The system may drop datagrams the splitter never sees, e.g. as the receive buffer of the socket overflows,
		// so fewer packets may be read and counted than written, but never more, and not none at all.
		t.Log(message)
		if acr+dropped > acw || anr+dropped*writeBufferSize > anw || acr == 0 {
			t.Error(message + ", want no more than written, and some read")
		}
		return
	}

	// Pipes deliver every packet, so the splitter reads or counts each one
	if acr+dropped != acw || anr+dropped*writeBufferSize != anw {
		t.Error(message)
	}
}

// testPacketSplitDebug makes checkSplitPayloads log what each of its goroutines does.
const testPacketSplitDebug = false

// testAddr is an address of a network other than UDP, e.g. of a flow over a trunk.
type testAddr string

// Network returns the network of every testAddr.
func (a testAddr) Network() string { return "test" }

// String returns the address itself.
func (a testAddr) String() string { return string(a) }

// waitSplitStats waits until the splitter has dropped the given numbers of packets, or fails.
func waitSplitStats(t *testing.T, ln SplitListener, want SplitStats) {
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

// acceptSplitConn accepts a split connection, or fails if there is none within a second.
func acceptSplitConn(t *testing.T, ln SplitListener) SplitConn {
	t.Helper()
	type result struct {
		c   SplitConn
		err error
	}
	ch := make(chan result, 1)
	go func() {
		c, err := ln.AcceptSplit()
		ch <- result{c, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("AcceptSplit() = %v, want nil", r.err)
		}
		return r.c
	case <-time.After(time.Second):
		t.Fatalf("AcceptSplit() hasn't returned")
		return nil
	}
}

// releaseSplitConn accepts the next split connection and releases it, handing its flow over to the fallback
// connection.
func releaseSplitConn(t *testing.T, ln SplitListener) {
	t.Helper()
	if err := acceptSplitConn(t, ln).Release(); err != nil {
		t.Fatalf("Release() = %v, want nil", err)
	}
}

// readSplitConn returns the packets read from a split connection until none comes for a while.
func readSplitConn(t *testing.T, c SplitConn) []string {
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

// splitTrunk returns a trunk end to write to a splitter over the other end from, and the splitter.
func splitTrunk(t *testing.T, config *SplitConfig) (FromToPacketConn, SplitListener) {
	t.Helper()
	client, server := BufferedTrunk(16, nil, nil)
	t.Cleanup(func() { _ = client.Close() })
	ln := Split(server, config)
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

// testSplitConfig ensures the fields of a SplitConfig not set give their defaults, and the others are applied.
func testSplitConfig(t *testing.T) {
	def := splitConfig{
		idleTimeout:       DefaultSplitIdleTimeout,
		writeTimeout:      DefaultSplitWriteTimeout,
		acceptQueueSize:   DefaultSplitAcceptQueueSize,
		fallbackQueueSize: DefaultSplitFallbackQueueSize,
		maxFlows:          DefaultSplitMaxFlows,
		readBufferSize:    DefaultSplitReadBufferSize,
		readQueueSize:     DefaultSplitReadQueueSize,
		writeQueueSize:    DefaultSplitWriteQueueSize,
	}
	with := func(f func(*splitConfig)) splitConfig {
		cfg := def
		f(&cfg)
		return cfg
	}

	for _, test := range []struct {
		name   string
		config *SplitConfig
		want   splitConfig
	}{
		{"Nil", nil, def},
		{"Zero", &SplitConfig{}, def},
		{"Negative", &SplitConfig{AcceptQueueSize: -1, FallbackQueueSize: -1, MaxFlows: -1, ReadBufferSize: -1, ReadQueueSize: -1, WriteQueueSize: -1}, def},
		{"NoTimeouts", &SplitConfig{IdleTimeout: -1, WriteTimeout: -1}, with(func(cfg *splitConfig) { cfg.idleTimeout, cfg.writeTimeout = 0, 0 })},
		{"ReadBufferSizeMin", &SplitConfig{ReadBufferSize: minSplitReadBufferSize - 1}, with(func(cfg *splitConfig) { cfg.readBufferSize = minSplitReadBufferSize })},
		{"FallbackConn", &SplitConfig{FallbackConn: true}, with(func(cfg *splitConfig) { cfg.fallbackConn = true })},
		{"Set", &SplitConfig{
			AcceptQueueSize:   1,
			ReadQueueSize:     2,
			FallbackQueueSize: 3,
			IdleTimeout:       4,
			MaxFlows:          5,
			ReadBufferSize:    minSplitReadBufferSize + 6,
			WriteQueueSize:    7,
			WriteTimeout:      8,
			PacketInfo:        true,
		}, splitConfig{
			idleTimeout:       4,
			writeTimeout:      8,
			acceptQueueSize:   1,
			fallbackQueueSize: 3,
			maxFlows:          5,
			readBufferSize:    minSplitReadBufferSize + 6,
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

// testSplitClose ensures that once a splitter is closed, Accept fails, a split connection returns the packets
// queued and then net.ErrClosed, writes fail, the fallback connection fails too, and deadlines can still be set.
func testSplitClose(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := splitTrunk(t, &SplitConfig{FallbackConn: true})
	writeFrom(t, client, a, "a1")
	writeFrom(t, client, a, "a2")
	conn := acceptSplitConn(t, ln)
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
		if err := set(time.Time{}); err != nil { // As the connections are open, though the splitter isn't
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

// testSplitCloseError ensures Close returns the error of closing the underlying connection,
// and net.ErrClosed once the splitter has stopped.
func testSplitCloseError(t *testing.T) {
	client, server := BufferedTrunk(16, nil, nil)
	defer func() { _ = client.Close() }()
	errClose := errors.New("close failed")
	ln := Split(failingClose{server, errClose}, nil)
	if err := ln.Close(); !errors.Is(err, errClose) {
		t.Errorf("Close() = %v, want %v", err, errClose)
	}
	if err := ln.Close(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("Close() again = %v, want net.ErrClosed", err)
	}
}

// testSplitUnderlyingDeadline ensures a splitter clears the deadlines of the connection it takes over, so that
// a deadline passed neither keeps the read loop from reading, nor fails writes without a write timeout.
func testSplitUnderlyingDeadline(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, server := BufferedTrunk(16, nil, nil)
	t.Cleanup(func() { _ = client.Close() })
	_ = server.SetDeadline(time.Now().Add(-time.Second))
	ln := Split(server, &SplitConfig{WriteTimeout: -1})
	t.Cleanup(func() { _ = ln.Close() })

	writeFrom(t, client, a, "a1")
	conn := acceptSplitConn(t, ln)
	if _, err := conn.WriteTo([]byte("x"), nil); err != nil {
		t.Fatalf("WriteTo() = %v, want nil", err)
	}
	b := make([]byte, 8)
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if n, _, err := client.ReadFrom(b); err != nil || string(b[:n]) != "x" {
		t.Errorf("ReadFrom() = %q, %v, want %q, nil", b[:n], err, "x")
	}
	waitSplitStats(t, ln, SplitStats{})
}

// testSplitBlockedReads ensures a read waiting on an empty split connection, or on the fallback connection,
// returns net.ErrClosed once the splitter stops.
func testSplitBlockedReads(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := splitTrunk(t, &SplitConfig{FallbackConn: true})
	writeFrom(t, client, a, "a1")
	conn := acceptSplitConn(t, ln)
	_ = readSplitConn(t, conn)            // So the next read waits
	_ = conn.SetReadDeadline(time.Time{}) // Which readSplitConn has set

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
			t.Fatalf("ReadFrom() hasn't returned once the splitter has stopped")
		}
	}
}

// testSplitStop ensures Stop stops the splitter, but leaves the underlying connection open with no deadlines,
// and that stopping it again, or closing it, fails with net.ErrClosed and leaves the connection as it is.
func testSplitStop(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := splitTrunk(t, nil)
	if err := ln.Stop(); err != nil {
		t.Fatalf("Stop() = %v, want nil", err)
	}
	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("Accept() = %v, want net.ErrClosed", err)
	}

	pc := ln.Unwrap()
	writeFrom(t, client, a, "a1") // Read by nothing but the caller of Stop
	b := make([]byte, 8)          // Read with no deadline set, which a deadline in the past would fail
	if n, addr, err := pc.ReadFrom(b); err != nil || string(b[:n]) != "a1" || addr.String() != a.String() {
		t.Errorf("ReadFrom() of the unwrapped connection = %q, %v, %v, want %q, %s, nil", b[:n], addr, err, "a1", a)
	}

	// A deadline set by the caller since is kept, rather than cleared, as the splitter has stopped already
	if err := pc.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	for name, stop := range map[string]func() error{"Stop": ln.Stop, "Close": ln.Close} {
		if err := stop(); !errors.Is(err, net.ErrClosed) {
			t.Errorf("%s() after Stop() = %v, want net.ErrClosed", name, err)
		}
	}
	unblock := time.AfterFunc(2*time.Second, func() { _ = client.Close() }) // Rather than hang if cleared
	defer unblock.Stop()
	if _, _, err := pc.ReadFrom(b); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("ReadFrom() of the unwrapped connection = %v, want os.ErrDeadlineExceeded", err)
	}
}

// testSplitUnwrap ensures Unwrap returns the underlying connection as given to Split, and leaves the splitter
// running.
func testSplitUnwrap(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, server := BufferedTrunk(16, nil, nil)
	defer func() { _ = client.Close() }()
	ln := Split(server, nil)
	defer func() { _ = ln.Close() }()

	if got := ln.Unwrap(); got != server {
		t.Errorf("Unwrap() = %v, want the trunk end given", got)
	}

	writeFrom(t, client, a, "a1") // Read by the splitter still running
	conn := acceptSplitConn(t, ln)
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	b := make([]byte, 8)
	if n, err := conn.Read(b); err != nil || string(b[:n]) != "a1" {
		t.Errorf("Read() = %q, %v, want %q, nil", b[:n], err, "a1")
	}
}

// testSplitUnwrapUDP ensures Unwrap returns the UDP connection given to Split, rather than the wrapper
// the splitter reads and writes through, with packet info or without, and Stop leaves it open with no deadlines.
func testSplitUnwrapUDP(t *testing.T) {
	skipIfNoUDP(t)

	for _, info := range []bool{true, false} {
		t.Run(map[bool]string{true: "PacketInfo", false: "NoPacketInfo"}[info], func(t *testing.T) {
			pc := listenLoopbackUDP(t)
			ln := Split(pc, &SplitConfig{PacketInfo: info})
			if got := ln.Unwrap(); got != pc {
				t.Errorf("Unwrap() = %T, want the %T given", got, pc)
			}
			if err := ln.Stop(); err != nil {
				t.Fatalf("Stop() = %v, want nil", err)
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

// testSplitAccept ensures Accept returns split connections as is, which read and write the packets of their flows
// as StreamConns, and a nil StreamConn once the splitter stops.
func testSplitAccept(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	client, ln := splitTrunk(t, nil)
	var sl StreamListener = ln

	writeFrom(t, client, a, "a1")
	conn, err := sl.Accept()
	if err != nil {
		t.Fatalf("Accept() = %v, want nil", err)
	}
	if _, ok := conn.(SplitConn); !ok {
		t.Errorf("Accept() = %T, want the SplitConn as is", conn)
	}
	if conn.RemoteAddr().String() != a.String() {
		t.Errorf("RemoteAddr() = %v, want %s", conn.RemoteAddr(), a)
	}
	writeFrom(t, client, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}, "b1") // Of another flow
	writeFrom(t, client, a, "abcdef")
	b := make([]byte, 8)
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if n, err := conn.Read(b); err != nil || string(b[:n]) != "a1" {
		t.Errorf("Read() = %q, %v, want %q, nil", b[:n], err, "a1")
	}
	if n, err := conn.Read(b[:3]); !errors.Is(err, io.ErrShortBuffer) || string(b[:n]) != "abc" {
		t.Errorf("Read() = %q, %v, want %q, io.ErrShortBuffer, unlike a connected UDP socket", b[:n], err, "abc")
	}
	if _, err := conn.Write([]byte("x")); err != nil {
		t.Fatalf("Write() = %v, want nil", err)
	}
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if n, _, dst, _, err := client.ReadFromTo(b); err != nil || string(b[:n]) != "x" || dst.String() != a.String() {
		t.Errorf("ReadFromTo() = %q to %v, %v, want %q to %s, nil", b[:n], dst, err, "x", a)
	}

	if err := sl.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
	if c, err := sl.Accept(); !errors.Is(err, net.ErrClosed) || c != nil {
		t.Errorf("Accept() after Close() = %#v, %v, want nil, net.ErrClosed", c, err)
	}
	if c, err := ln.AcceptSplit(); !errors.Is(err, net.ErrClosed) || c != nil {
		t.Errorf("AcceptSplit() after Close() = %#v, %v, want nil, net.ErrClosed", c, err)
	}
}

// testSplitUDPSocketFlows ensures a splitter over a dual-stack UDP socket tells flows apart by their destination
// addresses too, and replies from the address each flow came to, while a splitter without packet info
// tells them apart by their source addresses alone.
func testSplitUDPSocketFlows(t *testing.T) {
	skipIfNoUDP(t)

	for _, test := range []struct {
		name   string
		config *SplitConfig
		info   bool
	}{
		{"PacketInfo", &SplitConfig{PacketInfo: true}, true},
		{"NoPacketInfo", nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv6unspecified})
			if err != nil {
				t.Skipf("ListenUDP() = %v", err) // E.g. no IPv6 support
			}
			ln := Split(pc, test.config)
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
				t.Cleanup(func() { _ = client.Close() })
				dst := netip.AddrPortFrom(to, port)
				if _, err = client.WriteToUDPAddrPort([]byte("ping"), dst); err != nil {
					t.Fatal(err)
				}

				conn := acceptSplitConn(t, ln)
				wantLocal := pc.LocalAddr().String()
				if test.info {
					wantLocal = dst.String()
				}
				if local := conn.LocalAddr().String(); local != wantLocal {
					t.Errorf("LocalAddr() = %s, want %s", local, wantLocal)
				}
				// Unmapped, with packet info or without, though the socket reads an IPv4 address mapped,
				// which String doesn't tell
				wantRemote := netip.AddrPortFrom(to, client.LocalAddr().(*net.UDPAddr).AddrPort().Port())
				if remote := conn.RemoteAddr().(*net.UDPAddr).AddrPort(); remote != wantRemote {
					t.Errorf("RemoteAddr() = %s, want %s", remote, wantRemote)
				}
				if got := readSplitConn(t, conn); strings.Join(got, " ") != "ping" {
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

// testSplitMode ensures a splitter picks the mode for the interfaces its underlying connection implements,
// and uses destination addresses to tell flows apart only if the mode reports them.
func testSplitMode(t *testing.T) {
	trunk, _ := BufferedTrunk(1, nil, nil)
	defer func() { _ = trunk.Close() }()
	udp := func(t *testing.T) *net.UDPConn {
		skipIfNoUDP(t)
		return listenLoopbackUDP(t)
	}
	packetInfoUDP := func(t *testing.T) FromToUDPConn {
		u, err := PacketInfoUDPConn(udp(t))
		if err != nil {
			t.Skipf("PacketInfoUDPConn() = %v", err)
		}
		return u
	}

	for _, test := range []struct {
		name  string
		pc    func(t *testing.T) PacketConn
		info  bool // Ignored unless pc is a UDPConn only
		mode  splitMode
		byDst bool
	}{
		{"PacketConn", func(*testing.T) PacketConn { return FakeAddressPacketConn(trunk, nil, nil) }, true,
			splitPacketMode{}, false},
		{"FromToPacketConn", func(*testing.T) PacketConn { return trunk }, true, splitFromToPacketMode{}, true},
		{"UDPConn", func(t *testing.T) PacketConn { return udp(t) }, false, splitUDPMode{}, false},
		{"UDPConnPacketInfo", func(t *testing.T) PacketConn {
			return packetInfoUDP(t).(Wrapper[PacketConn]).Unwrap() // Packet info is enabled again by the splitter
		}, true, splitFromToUDPMode{}, true},
		{"FromToUDPConn", func(t *testing.T) PacketConn { return packetInfoUDP(t) }, false, splitFromToUDPMode{}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			mode, byDst := newSplitMode(test.pc(t), test.info)
			if fmt.Sprintf("%T", mode) != fmt.Sprintf("%T", test.mode) || byDst != test.byDst {
				t.Errorf("newSplitMode() = %T, %v, want %T, %v", mode, byDst, test.mode, test.byDst)
			}
		})
	}
}

const (
	testSplitAcceptName                    = "Accept"
	testSplitBlockedReadsName              = "BlockedReads"
	testSplitBlockedWritesName             = "BlockedWrites"
	testSplitCloseName                     = "Close"
	testSplitCloseErrorName                = "CloseError"
	testSplitClosedErrorsName              = "ClosedErrors"
	testSplitConcurrentReadsName           = "ConcurrentReads"
	testSplitConfigName                    = "Config"
	testSplitConnCloseAfterStopName        = "ConnCloseAfterStop"
	testSplitConnCloseDropsName            = "ConnCloseDrops"
	testSplitConnWriteToConnectedName      = "ConnWriteToConnected"
	testSplitDeadlinesName                 = "Deadlines"
	testSplitEmptyPacketsName              = "EmptyPackets"
	testSplitFallbackDropsName             = "FallbackDrops"
	testSplitFallbackFromToName            = "FallbackFromTo"
	testSplitFallbackWriteName             = "FallbackWrite"
	testSplitFallbackWriteCopiesAddrsName  = "FallbackWriteCopiesAddrs"
	testSplitFallbackWriteOtherNetworkName = "FallbackWriteOtherNetwork"
	testSplitFallbackWriteUnmappedName     = "FallbackWriteUnmapped"
	testSplitIdleTimeoutName               = "IdleTimeout"
	testSplitModeName                      = "Mode"
	testSplitNegativeReadName              = "NegativeRead"
	testSplitPacketInfoFlowsName           = "PacketInfoFlows"
	testSplitPayloadSizesName              = "PayloadSizes"
	testSplitReadCopiesAddrsName           = "ReadCopiesAddrs"
	testSplitReadDropsName                 = "ReadDrops"
	testSplitReleaseName                   = "Release"
	testSplitReleaseWithoutFallbackName    = "ReleaseWithoutFallback"
	testSplitShortBufferName               = "ShortBuffer"
	testSplitStopName                      = "Stop"
	testSplitTrunkFlowsName                = "TrunkFlows"
	testSplitUDPMaxDatagramName            = "UDPMaxDatagram"
	testSplitUDPSocketFlowsName            = "UDPSocketFlows"
	testSplitUDPTruncatedName              = "UDPTruncated"
	testSplitUnderlyingDeadlineName        = "UnderlyingDeadline"
	testSplitUnwrapName                    = "Unwrap"
	testSplitUnwrapUDPName                 = "UnwrapUDP"
	testSplitWriteCopiesBytesName          = "WriteCopiesBytes"
	testSplitWriteFailedName               = "WriteFailed"
	testSplitWriteQueueFullName            = "WriteQueueFull"
	testSplitWriteTimeoutName              = "WriteTimeout"
	testSplitWriteUnsentName               = "WriteUnsent"
)
