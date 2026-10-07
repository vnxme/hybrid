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
	"bytes"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
	"golang.org/x/net/nettest"
)

type packetConnPairCtor func() (PacketConn, PacketConn)

func newLocalPacketListeners(network string) (pc1, pc2 PacketConn) {
	pc1, _ = nettest.NewLocalPacketListener(network)
	pc2, _ = nettest.NewLocalPacketListener(network)
	return
}

func TestPacketConn(t *testing.T) {
	t.Run("LocalUDP4", func(t *testing.T) {
		ctor := func() (pc1, pc2 PacketConn) { return newLocalPacketListeners("udp4") }
		t.Run("BasicIO", func(t *testing.T) { testPacketConnBasicIO(t, ctor) })
		t.Run("CloseError", func(t *testing.T) { testPacketConnCloseError(t, ctor) })
	})
	t.Run("LocalUDP6", func(t *testing.T) {
		ctor := func() (pc1, pc2 PacketConn) { return newLocalPacketListeners("udp6") }
		t.Run("BasicIO", func(t *testing.T) { testPacketConnBasicIO(t, ctor) })
		t.Run("CloseError", func(t *testing.T) { testPacketConnCloseError(t, ctor) })
	})
	t.Run("UnbufferedTrunk", func(t *testing.T) {
		ctor := func() (pc1, pc2 PacketConn) { return UnbufferedTrunk(nil, nil) }
		t.Run("BasicIO", func(t *testing.T) { testPacketConnBasicIO(t, ctor) })
		t.Run("CloseError", func(t *testing.T) { testPacketConnCloseError(t, ctor) })
	})
	t.Run("BufferedTrunk", func(t *testing.T) {
		ctor := func() (pc1, pc2 PacketConn) { return BufferedTrunk(10, nil, nil) }
		t.Run("BasicIO", func(t *testing.T) { testPacketConnBasicIO(t, ctor) })
		t.Run("CloseError", func(t *testing.T) { testPacketConnCloseError(t, ctor) })
	})
}

func testPacketConnBasicIO(t *testing.T, ctor packetConnPairCtor) {
	pc1, pc2 := ctor()

	if addr1 := pc1.LocalAddr(); addr1 == nil {
		t.Fatalf("pc1.LocalAddr() = %v, want non-nil", addr1)
	}

	if addr2 := pc2.LocalAddr(); addr2 == nil {
		t.Fatalf("pc2.LocalAddr() = %v, want non-nil", addr2)
	}

	timeout := 50 * time.Millisecond
	sMin, sMinSafe, sMaxSafe, sMax := testPacketConnPayloadSizes(t, pc1)
	t.Run("MinPayload", func(t *testing.T) { testPacketConnRoundTrip(t, pc1, pc2, sMin, timeout) })
	t.Run("MinSafePayload", func(t *testing.T) { testPacketConnRoundTrip(t, pc1, pc2, sMinSafe, timeout) })
	t.Run("MaxSafePayload", func(t *testing.T) { testPacketConnRoundTrip(t, pc1, pc2, sMaxSafe, timeout) })
	t.Run("MaxPayload", func(t *testing.T) { testPacketConnRoundTrip(t, pc1, pc2, sMax, timeout) })

	if err := pc1.Close(); err != nil {
		t.Fatalf("pc1.Close() = %v, want nil", err)
	}

	if err := pc2.Close(); err != nil {
		t.Fatalf("pc2.Close() = %v, want nil", err)
	}
}

func testPacketConnPayloadSizes(t *testing.T, pc PacketConn) (sMin, sMinSafe, sMaxSafe, sMax int) {
	t.Helper()

	if udpAddr, ok := pc.LocalAddr().(*net.UDPAddr); ok {
		if udpAddr.IP.To4() != nil {
			return udp4PayloadSizeMin, udp4PayloadSizeMinSafe, udp4PayloadSizeMaxSafe, udp4PayloadSizeMax
		} else if udpAddr.IP.To16() != nil {
			return udp6PayloadSizeMin, udp6PayloadSizeMinSafe, udp6PayloadSizeMaxSafe, udp6PayloadSizeMax
		}
	}
	return packetSizeMin, packetSizeMinSafe, packetSizeMaxSafe, packetSizeMax
}

func testPacketConnRoundTrip(t *testing.T, pc1, pc2 PacketConn, size int, timeout time.Duration) {
	t.Helper()

	var addr1, addr2 net.Addr
	var err error
	var nr1, nr2, nw1, nw2 int

	var wg sync.WaitGroup
	wg.Add(2)

	wb := make([]byte, size)
	rand.New(rand.NewSource(0)).Read(wb)

	go func() {
		defer wg.Done()

		if err = pc1.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			t.Errorf("pc1.SetWriteDeadline() = %v, want nil", err)
			return
		}

		if nw1, err = pc1.WriteTo(wb, pc2.LocalAddr()); nw1 == 0 && err != nil {
			t.Errorf("pc1.WriteTo() = %v, want nil", err)
			return
		}

		if err = pc1.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			t.Errorf("pc1.SetReadDeadline() = %v, want nil", err)
			return
		}

		rb1 := make([]byte, len(wb))
		if nr1, addr1, err = pc1.ReadFrom(rb1); nr1 == 0 && err != nil {
			t.Errorf("pc1.ReadFrom() = %v, want nil", err)
			return
		}
		if addr1 == nil || (size > 0 && addr1.String() != pc2.LocalAddr().String()) {
			t.Errorf("pc1.ReadFrom() = %v; want %v", addr1, pc2.LocalAddr())
		}

		if !bytes.Equal(wb[:nw2][:nr1], rb1[:nr1]) {
			t.Error("transmitted data mismatch")
		}
	}()

	go func() {
		defer wg.Done()

		if err = pc2.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			t.Errorf("pc2.SetReadDeadline() = %v, want nil", err)
			return
		}

		rb2 := make([]byte, len(wb))
		if nr2, addr2, err = pc2.ReadFrom(rb2); nr2 == 0 && err != nil {
			t.Errorf("pc2.ReadFrom() = %v, want nil; %d", err, nr2)
			return
		}

		if addr2 == nil || (size > 0 && addr2.String() != pc1.LocalAddr().String()) {
			t.Errorf("pc2.ReadFrom() = %v; want %v", addr2, pc1.LocalAddr())
		}

		if !bytes.Equal(wb[:nw1][:nr2], rb2[:nr2]) {
			t.Error("transmitted data mismatch")
		}

		if err = pc2.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			t.Errorf("pc2.SetWriteDeadline() = %v, want nil", err)
			return
		}

		if _, err = pc2.WriteTo(wb, pc1.LocalAddr()); err != nil {
			t.Errorf("pc2.WriteTo() = %v, want nil", err)
			return
		}
	}()

	wg.Wait()
}

func testPacketConnCloseError(t *testing.T, ctor packetConnPairCtor) {
	pc, _ := ctor()
	_ = pc.Close()

	b := make([]byte, 1<<5)
	if _, _, err := pc.ReadFrom(b); err == nil {
		t.Error("pc.ReadFrom() = nil; want error")
	}

	if _, err := pc.WriteTo(b[:0], pc.LocalAddr()); err == nil {
		t.Error("pc.WriteTo() = nil; want error")
	}

	if err := pc.SetDeadline(time.Time{}); err == nil {
		t.Error("pc.SetDeadline() = nil; want error")
	}

	//if err := pc.Close(); err == nil {
	//	t.Error("pc.Close() = nil; want error")
	//}
}

// The minimum payload size is 0 bytes for both UDP/IPv4 and UDP/IPv6.
// Such payload-less UDP packets may be used for signaling and keep-alive purposes.
//
// The minimum safe payload size is 548 bytes for UDP/IPv4 and 1232 bytes for UDP/IPv6,
// and the maximum safe payload size is 1472 bytes for UDP/IPv4 and 1452 bytes for UDP/IPv6.
// This is due to the following:
// - the standard MTU size is 1500 bytes for both IPv4 and IPv6;
// - the minimum MTU is 576 bytes for IPv4 and 1280 bytes for IPv6;
// - the IP header size is 20 bytes for IPv4 and 40 bytes for IPv6;
// - the UDP header size is 8 bytes for both IPv4 and IPv6.
//
// UDP payload is limited to 65527 bytes for UDP/IPv6, but to 65507 for UDP/IPv4.
// This is due to the IPv4 header limiting the size of the total IPv4 packet to 65535 bytes.
// No IPv6 jumbograms are supported yet.
//
// See also: https://www.rfc-editor.org/rfc/rfc8085.html
const (
	ethStandardMTU = 1500
	ethJumboMTU    = 9000

	ip4MinimumMTU = 576
	ip6MinimumMTU = 1280

	udpHeaderSize = 8

	udp4HeaderSize = ipv4.HeaderLen + udpHeaderSize
	udp6HeaderSize = ipv6.HeaderLen + udpHeaderSize

	packetSizeMin     = 0
	packetSizeMinSafe = 1280
	packetSizeMaxSafe = 1500
	packetSizeMax     = 1<<16 - 1

	udp4PayloadSizeMin     = packetSizeMin
	udp4PayloadSizeMinSafe = ip4MinimumMTU - udp4HeaderSize
	udp4PayloadSizeMaxSafe = ethStandardMTU - udp4HeaderSize
	udp4PayloadSizeMax     = packetSizeMax - udp4HeaderSize

	udp6PayloadSizeMin     = packetSizeMin
	udp6PayloadSizeMinSafe = ip6MinimumMTU - udp6HeaderSize
	udp6PayloadSizeMaxSafe = ethStandardMTU - udp6HeaderSize
	udp6PayloadSizeMax     = packetSizeMax - udpHeaderSize
)
