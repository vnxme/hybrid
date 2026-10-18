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
	"errors"
	"io"
	"math"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"golang.org/x/net/nettest"
)

// Payload sizes to test packet connections with: the smallest, the smallest and the largest typical ones,
// which fit any path and a standard Ethernet frame respectively, and the largest.
//
// The smallest payload is empty, as UDP datagrams with no payload are valid, e.g. for signaling and keep-alives.
//
// The smallest typical payload is 548 bytes over IPv4 and 1232 bytes over IPv6, and the largest typical one
// is 1472 bytes over IPv4 and 1452 bytes over IPv6, given that:
//   - every IPv4 host accepts datagrams of 576 bytes, and every IPv6 link has an MTU of at least 1280 bytes;
//   - the standard Ethernet MTU is 1500 bytes;
//   - the IP header takes 20 bytes over IPv4, without options, and 40 bytes over IPv6;
//   - the UDP header takes 8 bytes.
//
// The largest payload is 65507 bytes over IPv4, as the total length of an IPv4 packet, header included,
// is limited to 65535 bytes, but 65527 bytes over IPv6, as the payload length of IPv6 excludes its header.
// IPv6 jumbograms aren't supported. Packet connections not over IP, e.g. trunks and pipes, are tested
// with the typical sizes of IPv6, and with up to 65535 bytes.
//
// See also RFC 8085, Section 3.2.
const (
	ethStandardMTU           = 1500
	ip4MinimumReassemblySize = 576 // Every IPv4 host accepts datagrams this large, while the minimum MTU is 68
	ip6MinimumMTU            = 1280

	ip4HeaderSize  = 20 // Without options
	ip6HeaderSize  = 40
	udpHeaderSize  = 8
	udp4HeaderSize = ip4HeaderSize + udpHeaderSize
	udp6HeaderSize = ip6HeaderSize + udpHeaderSize

	packetSizeMin     = 0
	packetSizeMinSafe = ip6MinimumMTU
	packetSizeMaxSafe = ethStandardMTU
	packetSizeMax     = math.MaxUint16

	udp4PayloadSizeMin     = packetSizeMin
	udp4PayloadSizeMinSafe = ip4MinimumReassemblySize - udp4HeaderSize
	udp4PayloadSizeMaxSafe = ethStandardMTU - udp4HeaderSize
	udp4PayloadSizeMax     = packetSizeMax - udp4HeaderSize

	udp6PayloadSizeMin     = packetSizeMin
	udp6PayloadSizeMinSafe = ip6MinimumMTU - udp6HeaderSize
	udp6PayloadSizeMaxSafe = ethStandardMTU - udp6HeaderSize
	udp6PayloadSizeMax     = packetSizeMax - udpHeaderSize
)

// packetConnPairCtor returns a pair of packet connections to test with, which may write to each other.
type packetConnPairCtor func(tb testing.TB) (PacketConn, PacketConn)

// closeConcurrently closes c from n goroutines at once, and returns how many of them have succeeded,
// failing the test if any other has failed with an error other than net.ErrClosed.
func closeConcurrently(t *testing.T, c io.Closer, n int) int {
	t.Helper()
	var succeeded atomic.Int32
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			switch err := c.Close(); {
			case err == nil:
				succeeded.Add(1)
			case !errors.Is(err, net.ErrClosed):
				t.Errorf("Close() = %v, want nil or %v", err, net.ErrClosed)
			}
		})
	}
	wg.Wait()
	return int(succeeded.Load())
}

// listenLoopbackUDP listens on 127.0.0.1 with a port chosen by the system, and closes the socket
// once the test ends.
func listenLoopbackUDP(tb testing.TB) *net.UDPConn {
	tb.Helper()
	pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = pc.Close() })
	return pc
}

// listenLoopbackPacketInfoUDP listens as listenLoopbackUDP does, and returns the socket wrapped
// with PacketInfoUDPConn too.
func listenLoopbackPacketInfoUDP(tb testing.TB) (*net.UDPConn, FromToUDPConn) {
	tb.Helper()
	pc := listenLoopbackUDP(tb)
	c, err := PacketInfoUDPConn(pc)
	if err != nil {
		tb.Fatal(err)
	}
	return pc, c
}

// logfIf calls [testing.TB.Logf] if condition is true.
func logfIf(tb testing.TB, condition bool, format string, args ...any) {
	tb.Helper()
	if condition {
		tb.Logf(format, args...)
	}
}

// newLocalPacketListeners listens twice on the loopback address of the given UDP network with ports chosen
// by the system, skipping the test if the network isn't supported, e.g. udp6 without IPv6, and closes both
// sockets once the test ends.
func newLocalPacketListeners(tb testing.TB, network string) (PacketConn, PacketConn) {
	tb.Helper()
	skipIfNoUDP(tb)
	var pcs [2]PacketConn
	for i := range pcs {
		pc, err := nettest.NewLocalPacketListener(network)
		if err != nil {
			tb.Skipf("NewLocalPacketListener(%q) = %v", network, err)
		}
		tb.Cleanup(func() { _ = pc.Close() })
		pcs[i] = pc
	}
	return pcs[0], pcs[1]
}

// skipIfNoUDP skips the test or benchmark on the platforms without UDP sockets.
func skipIfNoUDP(tb testing.TB) {
	tb.Helper()
	switch runtime.GOOS {
	case "js", "plan9", "wasip1":
		tb.Skipf("not supported on %s", runtime.GOOS)
	}
}

func TestCloneAddr(t *testing.T) {
	ip := func() net.IP { return net.IPv4(192, 0, 2, 1).To4() }
	for _, addr := range []net.Addr{
		&net.UDPAddr{IP: ip(), Port: 1, Zone: "z"},
		&net.TCPAddr{IP: ip(), Port: 2, Zone: "z"},
		&net.IPAddr{IP: ip(), Zone: "z"},
		&net.UnixAddr{Name: "/tmp/sock", Net: "unixgram"},
	} {
		c := cloneAddr(addr)
		if c == addr || !equalAddr(c, addr) {
			t.Errorf("cloneAddr(%v) = %v, want an equal copy", addr, c)
		}
		want := addr.String()
		switch a := c.(type) {
		case *net.UDPAddr:
			a.IP[3] = 99
		case *net.TCPAddr:
			a.IP[3] = 99
		case *net.IPAddr:
			a.IP[3] = 99
		case *net.UnixAddr:
			a.Name = "/tmp/other"
		}
		if addr.String() != want {
			t.Errorf("modifying a copy changed the original to %v, want %v", addr, want)
		}
	}

	// Other addresses, including nil ones, are returned as is
	for _, addr := range []net.Addr{nil, (*net.UDPAddr)(nil), oneTrunkAddr, onePipeAddr} {
		if c := cloneAddr(addr); c != addr {
			t.Errorf("cloneAddr(%#v) = %#v, want it as is", addr, c)
		}
	}
}
