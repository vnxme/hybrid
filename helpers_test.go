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
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

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
