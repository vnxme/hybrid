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
	"context"
	"errors"
	"math"
	"net"
	"net/netip"
	"runtime"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/vnxme/hybrid/internal/pktinfo"
)

// TestControlUDPPacketInfo ensures packet info is enabled for every IP version a socket accepts,
// including IPv4 on a dual-stack socket, for which Control is called with network udp6.
func TestControlUDPPacketInfo(t *testing.T) {
	skipIfNoUDP(t)

	lo4, lo6 := netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("::1")
	tests := []struct {
		network, address string
		dsts             []netip.Addr
	}{
		{"udp4", "0.0.0.0:0", []netip.Addr{lo4}},
		{"udp6", "[::]:0", []netip.Addr{lo6}},
		{"udp", "0.0.0.0:0", []netip.Addr{lo4}},
		{"udp", "[::]:0", []netip.Addr{lo4, lo6}},
		{"udp", ":0", []netip.Addr{lo4, lo6}},
	}
	lc := net.ListenConfig{Control: ControlUDPPacketInfo}
	for _, test := range tests {
		t.Run(test.network+","+test.address, func(t *testing.T) {
			pc, err := lc.ListenPacket(context.Background(), test.network, test.address)
			if err != nil {
				t.Skipf("ListenPacket() = %v", err) // E.g. no IPv6 support
			}
			defer func() { _ = pc.Close() }()
			conn := pc.(*net.UDPConn)
			port := uint16(conn.LocalAddr().(*net.UDPAddr).Port) //nolint:gosec // G115

			for _, dst := range test.dsts {
				if dst.Is6() && conn.LocalAddr().(*net.UDPAddr).IP.To4() != nil {
					continue // An IPv4 socket, e.g. on a host without IPv6
				}
				client, err := net.DialUDP("udp", nil, net.UDPAddrFromAddrPort(netip.AddrPortFrom(dst, port)))
				if err != nil {
					t.Fatalf("DialUDP(%s) = %v", dst, err)
				}
				if _, err = client.Write([]byte{1}); err != nil {
					t.Fatalf("client.Write() to %s = %v", dst, err)
				}
				_ = client.Close()

				_ = conn.SetReadDeadline(time.Now().Add(time.Second))
				oob := make([]byte, pktinfo.BufferSize)
				_, oobN, _, _, err := conn.ReadMsgUDPAddrPort(make([]byte, 1), oob)
				if err != nil {
					t.Fatalf("ReadMsgUDPAddrPort() from %s = %v", dst, err)
				}
				if got, _, ok := pktinfo.Parse(oob[:oobN]); !ok || got.Unmap() != dst {
					t.Errorf("pktinfo.Parse() of a packet to %s = %v, %v, want %s, true", dst, got, ok, dst)
				}
			}
		})
	}

	t.Run("NotUDP", func(t *testing.T) {
		if ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0"); err == nil {
			_ = ln.Close()
			t.Errorf("Listen(tcp) = nil, want an error")
		}
	})
}

// TestNeedPacketInfoUDPConn ensures packet info is reported as needed only for sockets bound to
// an unspecified address, and not yet wrapped with PacketInfoUDPConn.
func TestNeedPacketInfoUDPConn(t *testing.T) {
	skipIfNoUDP(t)

	tests := []struct {
		network, address string
		want             bool
	}{
		{"udp4", "127.0.0.1:0", false},
		{"udp4", "0.0.0.0:0", true},
		{"udp", "0.0.0.0:0", true},
		{"udp6", "[::1]:0", false},
		{"udp6", "[::]:0", true},
		{"udp", "[::]:0", true},
		{"udp", ":0", true},
	}
	for _, test := range tests {
		t.Run(test.network+","+test.address, func(t *testing.T) {
			pc, err := net.ListenPacket(test.network, test.address)
			if err != nil {
				t.Skipf("ListenPacket() = %v", err) // E.g. no IPv6 support
			}
			defer func() { _ = pc.Close() }()
			conn := pc.(*net.UDPConn)
			if got := NeedPacketInfoUDPConn(conn); got != test.want {
				t.Errorf("NeedPacketInfoUDPConn() = %v, want %v", got, test.want)
			}

			c, err := PacketInfoUDPConn(conn)
			if err != nil {
				t.Fatal(err)
			}
			if NeedPacketInfoUDPConn(c) {
				t.Errorf("NeedPacketInfoUDPConn() of a wrapped conn = true, want false")
			}
		})
	}

	t.Run("Nil", func(t *testing.T) {
		if NeedPacketInfoUDPConn(nil) || NeedPacketInfoUDPConn((*net.UDPConn)(nil)) {
			t.Errorf("NeedPacketInfoUDPConn() of nil = true, want false")
		}
	})

	t.Run("BadLocalAddr", func(t *testing.T) { // A wildcard socket, but not as its local address says
		pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = pc.Close() }()
		for _, addr := range []net.Addr{
			nil,                                   // No local address
			oneTrunkAddr,                          // Not a *net.UDPAddr
			(*net.UDPAddr)(nil),                   // No port
			&net.UDPAddr{IP: net.IPv4zero},        // Not bound to a port
			&net.UDPAddr{IP: net.IPv6unspecified}, // Not bound to a port
		} {
			if NeedPacketInfoUDPConn(localAddrConn{pc, addr}) {
				t.Errorf("NeedPacketInfoUDPConn() with local address %#v = true, want false", addr)
			}
		}
	})
}

// TestPacketInfoUDPConn ensures nil is rejected, a FromToUDPConn is returned as is, and unwrapping gives
// the UDPConn wrapped.
func TestPacketInfoUDPConn(t *testing.T) {
	t.Run("Nil", func(t *testing.T) {
		for _, conn := range []UDPConn{nil, (*net.UDPConn)(nil)} {
			if c, err := PacketInfoUDPConn(conn); c != nil || err == nil {
				t.Errorf("PacketInfoUDPConn(%#v) = %v, %v, want nil, an error", conn, c, err)
			}
		}
	})

	t.Run("Wrapped", func(t *testing.T) {
		skipIfNoUDP(t)

		pc, c := listenLoopbackPacketInfoUDP(t)
		if got, err := PacketInfoUDPConn(c); got != c || err != nil {
			t.Errorf("PacketInfoUDPConn() of a wrapped conn = %v, %v, want %v, nil", got, err, c)
		}
		if got := c.(Wrapper[PacketConn]).Unwrap(); got != pc {
			t.Errorf("Unwrap() = %T, want the %T given", got, pc)
		}
	})
}

// TestPacketInfoUDPConnNilAddrs ensures unknown addresses are returned as nil net.Addr values,
// not as nil *net.UDPAddr ones, which wouldn't compare equal to nil.
func TestPacketInfoUDPConnNilAddrs(t *testing.T) {
	skipIfNoUDP(t)

	_, c := listenLoopbackPacketInfoUDP(t)

	// A failed read knows neither address
	_ = c.SetReadDeadline(time.Now().Add(-time.Second))
	if _, src, dst, _, err := c.ReadFromTo(make([]byte, 1)); err == nil || src != nil || dst != nil {
		t.Errorf("ReadFromTo() = %#v, %#v, %v, want nil, nil, an error", src, dst, err)
	}

	// A write rejected for its addresses reports only the valid ones
	_, err := c.WriteToFromUDPAddrPort(nil, netip.AddrPort{}, netip.AddrPort{}, 0)
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Source != nil || opErr.Addr != nil {
		t.Errorf("WriteToFromUDPAddrPort() = %v, want net.OpError with nil Source and Addr", err)
	}
}

// TestPacketInfoUDPConnWriteValidation ensures a write is rejected unless dst is of an IP version
// the socket accepts and src, if any, is of the same IP version, whether or not they are mapped,
// and unless idx is in the range every platform has room for.
func TestPacketInfoUDPConnWriteValidation(t *testing.T) {
	skipIfNoUDP(t)

	tooBig := math.MaxInt32
	tooBig++ // Negative on 32-bit platforms, which is invalid too

	sink4 := listenLoopbackUDP(t)
	sink6, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Skipf("ListenUDP() = %v", err) // E.g. no IPv6 support
	}
	defer func() { _ = sink6.Close() }()
	port4, port6 := uint16(sink4.LocalAddr().(*net.UDPAddr).Port), uint16(sink6.LocalAddr().(*net.UDPAddr).Port) //nolint:gosec // G115

	addrs := map[string]netip.Addr{
		"none":   {},
		"v4":     netip.MustParseAddr("127.0.0.1"),
		"mapped": netip.MustParseAddr("::ffff:127.0.0.1"),
		"v6":     netip.IPv6Loopback(),
	}
	srcs := map[string][]string{ // Kinds of dst mapped to kinds of src accepted with it
		"v4":     {"none", "v4", "mapped"},
		"mapped": {"none", "v4", "mapped"},
		"v6":     {"none", "v6"},
	}
	sockets := []struct {
		network, address string
		dsts             []string // Kinds of dst accepted
	}{
		{"udp4", "127.0.0.1:0", []string{"v4", "mapped"}},
		{"udp6", "[::1]:0", []string{"v6"}},
		{"udp", "[::]:0", []string{"v4", "mapped", "v6"}},
	}
	for _, socket := range sockets {
		t.Run(socket.network+","+socket.address, func(t *testing.T) {
			pc, err := net.ListenPacket(socket.network, socket.address)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = pc.Close() }()
			c, err := PacketInfoUDPConn(pc.(*net.UDPConn))
			if err != nil {
				t.Fatal(err)
			}

			for dstKind, dstIP := range addrs {
				port := port4
				if dstKind == "v6" {
					port = port6
				}
				dst := netip.AddrPortFrom(dstIP, port)
				if !dstIP.IsValid() {
					dst = netip.AddrPort{}
				}
				for srcKind, srcIP := range addrs {
					src := netip.AddrPortFrom(srcIP, 0)
					if !srcIP.IsValid() {
						src = netip.AddrPort{}
					}
					for _, idx := range []int{-1, 0, tooBig} {
						valid := idx >= 0 && idx <= math.MaxInt32 && slices.Contains(socket.dsts, dstKind) && slices.Contains(srcs[dstKind], srcKind)
						_, err := c.WriteToFromUDPAddrPort([]byte{1}, dst, src, idx)
						if invalid := errors.Is(err, syscall.EINVAL); valid && err != nil || !valid && !invalid {
							t.Errorf("WriteToFromUDPAddrPort(dst %s, src %s, idx %d) = %v, want valid %v", dstKind, srcKind, idx, err, valid)
						}
					}
				}
			}
		})
	}
}

// TestPacketInfoUDPConnWriteCount ensures a write with packet info reports all the bytes
// of a datagram as written, which WriteMsgUDPAddrPort doesn't on Windows since Go 1.26.
func TestPacketInfoUDPConnWriteCount(t *testing.T) {
	skipIfNoUDP(t)

	lo := netip.MustParseAddr("127.0.0.1")
	_, c := listenLoopbackPacketInfoUDP(t)
	client := listenLoopbackUDP(t)

	payload := []byte("abc")
	dst := client.LocalAddr().(*net.UDPAddr).AddrPort()
	if n, err := c.WriteToFromUDPAddrPort(payload, dst, netip.AddrPortFrom(lo, 0), 0); n != len(payload) || err != nil {
		t.Errorf("WriteToFromUDPAddrPort() = %d, %v, want %d, nil", n, err, len(payload))
	}
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	b := make([]byte, 8)
	if n, err := client.Read(b); string(b[:n]) != string(payload) || err != nil {
		t.Errorf("client.Read() = %q, %v, want %q, nil", b[:n], err, payload)
	}
}

// TestPacketInfoUDPConnTruncatedOOB ensures control message buffers grow once a read finds its control
// messages truncated, so that packet info is read again, rather than lost for every packet afterwards.
func TestPacketInfoUDPConnTruncatedOOB(t *testing.T) {
	skipIfNoUDP(t)
	skipIfEmulated(t, "MSG_CTRUNC on control messages truncated for the guest only") // QEMU overwrites it with the host's flags

	lo := netip.MustParseAddr("127.0.0.1")
	pc, c := listenLoopbackPacketInfoUDP(t)
	const size = 8 // Too short for any control message header
	c.(*packetInfoUDPConn).rdOOBSize.Store(size)

	client, err := net.DialUDP("udp4", nil, pc.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	b := make([]byte, 8)
	for i := range 8 { // Enough to double the buffer up to any size packet info takes
		if _, err = client.Write([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		_, _, dst, _, err := c.ReadFromToUDPAddrPort(b)
		switch {
		case err != nil && (i > 0 || runtime.GOOS != "windows"): // Windows fails a read with truncated ones
			t.Fatalf("ReadFromToUDPAddrPort() #%d = %v, want nil", i, err)
		case i == 0 && dst.IsValid():
			t.Fatalf("ReadFromToUDPAddrPort() #0 = %v, want an invalid dst, as control messages are truncated", dst)
		case dst.Addr() == lo:
			if got := c.(*packetInfoUDPConn).rdOOBSize.Load(); got <= size {
				t.Errorf("rdOOBSize = %d, want more than %d", got, size)
			}
			return
		}
	}
	t.Errorf("ReadFromToUDPAddrPort() hasn't returned dst %s after control message buffers grew", lo)
}

// TestPacketInfoUDPConnUnmapped ensures a dual-stack socket returns src and dst of IPv4 packets unmapped,
// and that it replies to them.
func TestPacketInfoUDPConnUnmapped(t *testing.T) {
	skipIfNoUDP(t)

	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv6unspecified})
	if err != nil {
		t.Skipf("ListenUDP() = %v", err) // E.g. no IPv6 support
	}
	defer func() { _ = pc.Close() }()
	c, err := PacketInfoUDPConn(pc)
	if err != nil {
		t.Fatal(err)
	}
	port := pc.LocalAddr().(*net.UDPAddr).Port

	for _, ip := range []netip.Addr{netip.MustParseAddr("127.0.0.1"), netip.IPv6Loopback()} {
		t.Run(ip.String(), func(t *testing.T) {
			client, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: ip.AsSlice(), Port: port})
			if err != nil {
				t.Skipf("DialUDP() = %v", err)
			}
			defer func() { _ = client.Close() }()
			from := client.LocalAddr().(*net.UDPAddr).AddrPort()
			to := netip.AddrPortFrom(ip, uint16(port)) //nolint:gosec // G115

			b := make([]byte, 8)
			for range 2 {
				if _, err := client.Write([]byte("abc")); err != nil {
					t.Fatal(err)
				}
			}
			_ = c.SetReadDeadline(time.Now().Add(time.Second))

			_, src, dst, idx, err := c.ReadFromToUDPAddrPort(b)
			if src != from || dst != to || err != nil {
				t.Errorf("ReadFromToUDPAddrPort() = src %s, dst %s, %v, want %s, %s, nil", src, dst, err, from, to)
			}

			// The *net.UDPAddr variant has 4-byte IPs for IPv4 too
			_, srcUDP, dstUDP, _, err := c.ReadFromToUDP(b)
			if err != nil || srcUDP == nil || dstUDP == nil ||
				len(srcUDP.IP) != ip.BitLen()/8 || len(dstUDP.IP) != ip.BitLen()/8 {
				t.Errorf("ReadFromToUDP() = src %v, dst %v, %v, want %d-byte IPs", srcUDP, dstUDP, err, ip.BitLen()/8)
			}

			// Reply with the addresses read
			if _, err := c.WriteToFromUDPAddrPort([]byte("def"), src, dst, idx); err != nil {
				t.Fatalf("WriteToFromUDPAddrPort() = %v, want nil", err)
			}
			_ = client.SetReadDeadline(time.Now().Add(time.Second))
			if n, err := client.Read(b); string(b[:n]) != "def" || err != nil {
				t.Errorf("client.Read() = %q, %v, want %q, nil", b[:n], err, "def")
			}
		})
	}
}

// localAddrConn is a UDPConn whose local address is replaced, e.g. with an address of another type.
type localAddrConn struct {
	*net.UDPConn
	addr net.Addr
}

func (c localAddrConn) LocalAddr() net.Addr { return c.addr }

// TestPacketInfoUDPConnFamily ensures the IP versions a socket accepts are detected from its local address
// and IPV6_V6ONLY, and that a connection without a local *net.UDPAddr is rejected.
func TestPacketInfoUDPConnFamily(t *testing.T) {
	skipIfNoUDP(t)

	tests := []struct {
		network, address string
		v4Only, v6Only   bool
		net              string
	}{
		{"udp4", "127.0.0.1:0", true, false, "udp4"},
		{"udp4", "0.0.0.0:0", true, false, "udp4"},
		{"udp", "127.0.0.1:0", true, false, "udp4"},
		{"udp6", "[::1]:0", false, true, "udp6"},
		{"udp6", "[::]:0", false, true, "udp6"},
		{"udp", "[::1]:0", false, true, "udp6"},
		{"udp", "[::]:0", false, false, "udp"},
	}
	for _, test := range tests {
		t.Run(test.network+","+test.address, func(t *testing.T) {
			pc, err := net.ListenPacket(test.network, test.address)
			if err != nil {
				t.Skipf("ListenPacket() = %v", err) // E.g. no IPv6 support
			}
			defer func() { _ = pc.Close() }()
			c, err := PacketInfoUDPConn(pc.(*net.UDPConn))
			if err != nil {
				t.Fatalf("PacketInfoUDPConn() = %v, want nil", err)
			}
			u := c.(*packetInfoUDPConn)
			if u.v4Only != test.v4Only || u.v6Only != test.v6Only || u.network() != test.net {
				t.Errorf("PacketInfoUDPConn() = v4Only %v, v6Only %v, net %s, want %v, %v, %s",
					u.v4Only, u.v6Only, u.network(), test.v4Only, test.v6Only, test.net)
			}
			if want := uint16(pc.LocalAddr().(*net.UDPAddr).Port); u.port != want { //nolint:gosec // G115
				t.Errorf("PacketInfoUDPConn() = port %d, want %d", u.port, want)
			}
		})
	}

	t.Run("BadLocalAddr", func(t *testing.T) {
		pc := listenLoopbackUDP(t)
		for _, addr := range []net.Addr{
			nil,                                      // No local address
			oneTrunkAddr,                             // Not a *net.UDPAddr
			(*net.UDPAddr)(nil),                      // No port
			&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}, // Not bound to a port
		} {
			if c, err := PacketInfoUDPConn(localAddrConn{pc, addr}); err == nil {
				t.Errorf("PacketInfoUDPConn() with local address %#v = %v, nil, want an error", addr, c)
			}
		}
	})
}

// BenchmarkPacketInfoUDPConn measures writes and reads with packet info over loopback, next to
// those of the underlying *net.UDPConn. Each operation is a write and the read of the same datagram.
func BenchmarkPacketInfoUDPConn(b *testing.B) {
	skipIfNoUDP(b)

	lo := netip.MustParseAddr("127.0.0.1")
	listen := func() (*net.UDPConn, netip.AddrPort) {
		pc := listenLoopbackUDP(b)
		_ = pc.SetReadDeadline(time.Now().Add(time.Minute)) // Fail rather than hang if a datagram is lost
		return pc, pc.LocalAddr().(*net.UDPAddr).AddrPort()
	}
	pc, self := listen()
	peer, other := listen()
	c, err := PacketInfoUDPConn(pc)
	if err != nil {
		b.Fatal(err)
	}

	p := make([]byte, 64)
	write := func() error { // From the peer to the packet info conn
		_, err := peer.WriteToUDPAddrPort(p, self)
		return err
	}
	read := func() error { // By the peer from the packet info conn
		_, _, err := peer.ReadFromUDPAddrPort(p)
		return err
	}
	for _, bench := range []struct {
		name string
		op   func() error
	}{
		{"WriteToFromUDPAddrPort", func() error {
			if _, err := c.WriteToFromUDPAddrPort(p, other, netip.AddrPortFrom(lo, 0), 0); err != nil {
				return err
			}
			return read()
		}},
		{"WriteToUDPAddrPort", func() error { // Without packet info
			if _, err := pc.WriteToUDPAddrPort(p, other); err != nil {
				return err
			}
			return read()
		}},
		{"ReadFromToUDPAddrPort", func() error {
			if err := write(); err != nil {
				return err
			}
			_, _, _, _, err := c.ReadFromToUDPAddrPort(p)
			return err
		}},
		{"ReadFromTo", func() error {
			if err := write(); err != nil {
				return err
			}
			_, _, _, _, err := c.ReadFromTo(p)
			return err
		}},
		{"ReadFromUDPAddrPort", func() error { // Without packet info
			if err := write(); err != nil {
				return err
			}
			_, _, err := pc.ReadFromUDPAddrPort(p)
			return err
		}},
	} {
		b.Run(bench.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := bench.op(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// localAddrs returns the addresses of each interface that is up, with zones for IPv6 link-local ones,
// except for those packets to which don't loop back, e.g. link-local addresses of macOS utun interfaces,
// whose packets may go to the peer, and of awdl0 and llw0, which may stay up while inactive.
func localAddrs(t *testing.T) [][]netip.Addr {
	t.Helper()
	ifs, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	var addrs [][]netip.Addr
	for _, ifc := range ifs {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagRunning == 0 {
			continue
		}
		ips, err := ifc.Addrs()
		if err != nil {
			t.Fatal(err)
		}
		var ifcAddrs []netip.Addr
		for _, ip := range ips {
			if ipNet, ok := ip.(*net.IPNet); ok {
				addr, _ := netip.AddrFromSlice(ipNet.IP)
				if addr = addr.Unmap(); addr.Is6() && addr.IsLinkLocalUnicast() {
					addr = addr.WithZone(ifc.Name)
				}
				if !loopsBack(addr) {
					t.Logf("skipping %s, as packets to it don't loop back", addr)
					continue
				}
				ifcAddrs = append(ifcAddrs, addr)
			}
		}
		addrs = append(addrs, ifcAddrs)
	}
	return addrs
}

// loopsBack reports whether a packet to addr reaches a plain UDP socket bound to it, and a reply comes back.
func loopsBack(addr netip.Addr) bool {
	network := "udp6"
	if addr.Is4() {
		network = "udp4"
	}
	server, err := net.ListenUDP(network, net.UDPAddrFromAddrPort(netip.AddrPortFrom(addr, 0)))
	if err != nil {
		return false
	}
	defer func() { _ = server.Close() }()
	client, err := net.ListenUDP(network, nil)
	if err != nil {
		return false
	}
	defer func() { _ = client.Close() }()

	b, deadline := make([]byte, 1), time.Now().Add(250*time.Millisecond)
	_ = server.SetReadDeadline(deadline)
	_ = client.SetReadDeadline(deadline)
	if _, err = client.WriteToUDPAddrPort(b, server.LocalAddr().(*net.UDPAddr).AddrPort()); err != nil {
		return false
	}
	_, from, err := server.ReadFromUDPAddrPort(b)
	if err != nil {
		return false
	}
	if _, err = server.WriteToUDPAddrPort(b, from); err != nil {
		return false
	}
	_, _, err = client.ReadFromUDPAddrPort(b)
	return err == nil
}

// TestPacketInfoUDPConnInterfaces ensures a packet to any local address is read with this address as dst
// and with a local interface index, and that a reply written with them comes from this address,
// as well as a reply written with another address of the same kind on the same interface as src.
func TestPacketInfoUDPConnInterfaces(t *testing.T) {
	skipIfNoUDP(t)
	skipIfEmulated(t, "netlink messages listing addresses that Go can parse")

	addrs := localAddrs(t)
	for _, socket := range []struct{ network, address string }{
		{"udp4", "0.0.0.0:0"},
		{"udp6", "[::]:0"},
		{"udp", "[::]:0"},
	} {
		t.Run(socket.network+","+socket.address, func(t *testing.T) {
			pc, err := net.ListenPacket(socket.network, socket.address)
			if err != nil {
				t.Skipf("ListenPacket() = %v", err) // E.g. no IPv6 support
			}
			defer func() { _ = pc.Close() }()
			c, err := PacketInfoUDPConn(pc.(*net.UDPConn))
			if err != nil {
				t.Fatal(err)
			}
			u := c.(*packetInfoUDPConn)

			b := make([]byte, 8)
			for _, ifcAddrs := range addrs {
				for _, to := range ifcAddrs {
					if to.Is4() && u.v6Only || to.Is6() && u.v4Only {
						continue
					}
					t.Run(to.String(), func(t *testing.T) {
						network := "udp6"
						if to.Is4() {
							network = "udp4"
						}
						client, err := net.ListenUDP(network, nil)
						if err != nil {
							t.Fatal(err)
						}
						defer func() { _ = client.Close() }()

						if _, err = client.WriteToUDPAddrPort([]byte("abc"), netip.AddrPortFrom(to, u.port)); err != nil {
							t.Fatal(err)
						}
						_ = c.SetReadDeadline(time.Now().Add(time.Second))
						_, src, dst, idx, err := c.ReadFromToUDPAddrPort(b)
						if err != nil {
							t.Fatalf("ReadFromToUDPAddrPort() = %v, want nil", err)
						}
						if dst.Addr().WithZone("") != to.WithZone("") || dst.Port() != u.port || idx <= 0 {
							t.Errorf("ReadFromToUDPAddrPort() = dst %s, idx %d, want %s, a positive index",
								dst, idx, netip.AddrPortFrom(to, u.port))
						}

						// reply writes from the given source address, or from the one the system chooses if it's invalid
						reply := func(from netip.Addr, replyIdx int, want netip.Addr) {
							t.Helper()
							if _, err := c.WriteToFromUDPAddrPort([]byte("def"), src, netip.AddrPortFrom(from, 0), replyIdx); err != nil {
								t.Errorf("WriteToFromUDPAddrPort(src %s, idx %d) = %v, want nil", from, replyIdx, err)
								return
							}
							_ = client.SetReadDeadline(time.Now().Add(time.Second))
							_, got, err := client.ReadFromUDPAddrPort(b)
							if err != nil || got.Addr().Unmap().WithZone("") != want.WithZone("") || got.Port() != u.port {
								t.Errorf("client.ReadFromUDPAddrPort() after a reply from %s with idx %d = %s, %v, want %s, nil",
									from, replyIdx, got, err, netip.AddrPortFrom(want, u.port))
							}
						}

						reply(netip.Addr{}, idx, to) // The system chooses the address the packet came to
						for _, from := range ifcAddrs {
							if from.Is4() != to.Is4() || from.IsLinkLocalUnicast() != to.IsLinkLocalUnicast() {
								continue
							}
							reply(from, idx, from)
							reply(from, 0, from)
						}
					})
				}
			}
		})
	}
}

// negativeReadUDPConn is a UDPConn whose reads fail with a negative count, as Linux reports them for
// a socket closed while reading.
type negativeReadUDPConn struct {
	*net.UDPConn
}

func (c negativeReadUDPConn) ReadMsgUDPAddrPort(_, _ []byte) (int, int, int, netip.AddrPort, error) {
	return -1, 0, 0, netip.AddrPort{}, net.ErrClosed
}

// TestPacketInfoUDPConnNegativeRead ensures a failed read never reports a negative count.
func TestPacketInfoUDPConnNegativeRead(t *testing.T) {
	skipIfNoUDP(t)

	pc := listenLoopbackUDP(t)
	c, err := PacketInfoUDPConn(negativeReadUDPConn{pc})
	if err != nil {
		t.Fatal(err)
	}
	if n, _, _, _, err := c.ReadFromToUDPAddrPort(make([]byte, 8)); n != 0 || !errors.Is(err, net.ErrClosed) {
		t.Errorf("ReadFromToUDPAddrPort() = %d, %v, want 0, net.ErrClosed", n, err)
	}
}
