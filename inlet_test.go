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
		{name: "HybridUnbufferedSyncPipe", ctor: func() (PacketConn, PacketConn) {
			c1, c2 := Pipe(0, false)
			return StreamToPacketConn(c1), StreamToPacketConn(c2)
		}, minWBS: packetSizeMin, minSafeWBS: packetSizeMinSafe, maxSafeWBS: packetSizeMaxSafe, maxWBS: packetSizeMax},
		{name: "HybridUnbufferedSemiPipe", ctor: func() (PacketConn, PacketConn) {
			c1, c2 := Pipe(0, true)
			return StreamToPacketConn(c1), StreamToPacketConn(c2)
		}, minWBS: packetSizeMin, minSafeWBS: packetSizeMinSafe, maxSafeWBS: packetSizeMaxSafe, maxWBS: packetSizeMax},
		{name: "HybridBufferedAsyncPipe", ctor: func() (PacketConn, PacketConn) {
			c1, c2 := Pipe(10, true)
			return StreamToPacketConn(c1), StreamToPacketConn(c2)
		}, minWBS: packetSizeMin, minSafeWBS: packetSizeMinSafe, maxSafeWBS: packetSizeMaxSafe, maxWBS: packetSizeMax},
		{name: "LocalUDP4Socket", ctor: func() (PacketConn, PacketConn) {
			return newLocalPacketListeners("udp4")
		}, minWBS: udp4PayloadSizeMin, minSafeWBS: udp4PayloadSizeMinSafe, maxSafeWBS: udp4PayloadSizeMaxSafe, maxWBS: udp4PayloadSizeMax},
		{name: "LocalUDP6Socket", ctor: func() (PacketConn, PacketConn) {
			return newLocalPacketListeners("udp6")
		}, minWBS: udp6PayloadSizeMin, minSafeWBS: udp6PayloadSizeMinSafe, maxSafeWBS: udp6PayloadSizeMaxSafe, maxWBS: udp6PayloadSizeMax},
	}

	testFunc := func(t *testing.T, ctor packetConnPairCtor, writeBufferSize int) {
		testPacketInletWithPairedConns(t, ctor, piAcceptQueueSizeDef, poReadQueueSizeDef, packetSizeMax, writeBufferSize, packetCount)
		// testPacketInletWithPairedConns2(t, ctor, piAcceptQueueDefSize, poReadQueueDefSize, packetSizeMax, writeBufferSize, packetCount)
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
				t.Fatalf("%v: pc.SetReadDeadline() = %v, want nil", me, err)
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
		defer func() { _ = ln.Close() }()

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
				t.Fatalf("%v: pc.SetWriteDeadline() = %v, want nil", me, err)
			}

			n, err = pc.WriteTo(b, rAddr)
			if n == 0 && err != nil {
				t.Fatalf("%v: pc.WriteTo() = %v, want nil", me, err)
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
	if acr != acw || anr != anw {
		message := fmt.Sprintf("r/w statistics: count %d/%d, bytes %d/%d", acr, acw, anr, anw)
		if pc2.LocalAddr().Network() == "udp" {
			// UDP doesn't guarantee that all packets sent will be received,
			// so we can't consider this test as failing if anything is lost.
			t.Skip(message)
		}

		// However, pipes do guarantee that all packets are delivered,
		// so any loss represents a failure of this test.
		t.Error(message)
	}
}

func testPacketInletWithPairedConns2(t *testing.T, ctor packetConnPairCtor, acceptQueueSize, readQueueSize, readBufferSize, writeBufferSize, packetCount int) {
	t.Helper()
	pc1, pc2 := ctor()

	timeout := 50 * time.Millisecond
	done := make(chan struct{})

	serverHandler := func(pc PacketConn, wg *sync.WaitGroup) {
		defer wg.Done()

		lAddr := pc.LocalAddr()
		me := fmt.Sprintf("inlet %v", lAddr)
		logfIf(t, testPacketInletDebug, "%v: started", me)

		ln := Inlet(pc,
			WithAcceptQueueSize(acceptQueueSize),
			WithReadQueueSize(readQueueSize),
			WithReadBufferSize(readBufferSize))
		defer func() { _ = ln.Close() }()

		for {
			mc, err := ln.Accept()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					logfIf(t, testPacketInletDebug, "%v: ln.Accept() = %v", me, err)
				}
				break
			}

			wg.Add(1)
			go func(t *testing.T, mc MixedConn) {
				defer func() { _ = mc.Close() }()

				testPacketConnRoundTrip(t, pc1, mc, writeBufferSize, timeout)
			}(t, mc)
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
		err = pc.SetWriteDeadline(time.Now().Add(2 * timeout))
		if err != nil {
			t.Fatalf("%v: pc.SetWriteDeadline() = %v, want nil", me, err)
		}

		n, err = pc.WriteTo(b, rAddr)
		if n == 0 && err != nil {
			t.Fatalf("%v: pc.WriteTo() = %v, want nil", me, err)
		}

		logfIf(t, testPacketInletDebug, "%v: wrote %d bytes", me, n)

		time.AfterFunc(timeout, func() {
			close(done)
		})

		logfIf(t, testPacketInletDebug, "%v: stopped", me)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go serverHandler(pc2, &wg)
	go clientHandler(pc1, pc2.LocalAddr(), &wg)

	<-done

	if err := pc1.Close(); err != nil {
		t.Fatalf("pc1.Close() = %v, want nil", err)
	}

	if err := pc2.Close(); err != nil {
		t.Fatalf("pc2.Close() = %v, want nil", err)
	}
}

const (
	testPacketInletDebug = false
)
