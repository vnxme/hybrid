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
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
	"golang.org/x/sys/windows"
)

// TestUDPListenWithPacketInfo ensures packet info is enabled for every IP version a socket accepts,
// including IPv4 on a dual-stack socket, for which Control is called with network udp6.
func TestUDPListenWithPacketInfo(t *testing.T) {
	switch runtime.GOOS {
	case "js", "plan9", "wasip1":
		t.Skipf("not supported on %s", runtime.GOOS)
	}

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
	for _, test := range tests {
		t.Run(test.network+","+test.address, func(t *testing.T) {
			pc, err := UDPListenWithPacketInfo.ListenPacket(context.Background(), test.network, test.address)
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
				oob := make([]byte, cMsgBufferSize)
				_, oobN, _, _, err := conn.ReadMsgUDPAddrPort(make([]byte, 1), oob)
				if err != nil {
					t.Fatalf("ReadMsgUDPAddrPort() from %s = %v", dst, err)
				}
				if got, _, ok := parsePacketInfo(oob[:oobN]); !ok || got.Unmap() != dst {
					t.Errorf("parsePacketInfo() of a packet to %s = %v, %v, want %s, true", dst, got, ok, dst)
				}
			}
		})
	}
}

func TestNeedSelectiveUDPConn(t *testing.T) {
	l41, _ := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	l42, _ := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(0, 0, 0, 0), Port: 0})
	l43, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(0, 0, 0, 0), Port: 0})
	l61, _ := net.ListenUDP("udp6", &net.UDPAddr{IP: net.ParseIP("::1"), Port: 0})
	l62, _ := net.ListenUDP("udp6", &net.UDPAddr{IP: net.ParseIP("::"), Port: 0})
	l63, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("::"), Port: 0})

	type args struct {
		conn *net.UDPConn
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"nil", args{nil}, false},
		{l41.LocalAddr().String(), args{l41}, false},
		{l42.LocalAddr().String(), args{l42}, true},
		{l43.LocalAddr().String(), args{l43}, true},
		{l61.LocalAddr().String(), args{l61}, false},
		{l62.LocalAddr().String(), args{l62}, true},
		{l63.LocalAddr().String(), args{l63}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NeedSelectiveUDPConn(tt.args.conn); got != tt.want {
				t.Errorf("NeedSelectiveUDPConn() = %v, want %v", got, tt.want)
			}
		})
	}

	_ = l41.Close()
	_ = l42.Close()
	_ = l43.Close()
	_ = l61.Close()
	_ = l62.Close()
	_ = l63.Close()
}

func TestNewSelectiveUDPConn(t *testing.T) {
	type args struct {
		conn *net.UDPConn
	}
	tests := []struct {
		name    string
		args    args
		want    SelectivePacketConn
		wantErr bool
	}{
		{"nil", args{nil}, nil, true},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewSelectiveUDPConn(tt.args.conn)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewSelectiveUDPConn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewSelectiveUDPConn() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSelectiveUDPConnNilAddrs ensures unknown addresses are returned as nil net.Addr values,
// not as nil *net.UDPAddr ones, which wouldn't compare equal to nil.
func TestSelectiveUDPConnNilAddrs(t *testing.T) {
	switch runtime.GOOS {
	case "js", "plan9", "wasip1":
		t.Skipf("not supported on %s", runtime.GOOS)
	}

	pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	c, err := NewSelectiveUDPConn(pc)
	if err != nil {
		t.Fatal(err)
	}

	// A failed read knows neither address
	_ = c.SetReadDeadline(time.Now().Add(-time.Second))
	if _, src, dst, _, err := c.ReadFromTo(make([]byte, 1)); err == nil || src != nil || dst != nil {
		t.Errorf("ReadFromTo() = %#v, %#v, %v, want nil, nil, an error", src, dst, err)
	}

	// A write rejected for its addresses reports only the valid ones
	_, err = c.WriteToFromUDPAddrPort(nil, netip.AddrPort{}, netip.AddrPort{}, 0)
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Source != nil || opErr.Addr != nil {
		t.Errorf("WriteToFromUDPAddrPort() = %v, want net.OpError with nil Source and Addr", err)
	}
}

// TestSelectiveUDPConnWriteCount ensures a write with packet info reports all the bytes
// of a datagram as written, which WriteMsgUDPAddrPort doesn't on Windows since Go 1.26.
func TestSelectiveUDPConnWriteCount(t *testing.T) {
	switch runtime.GOOS {
	case "js", "plan9", "wasip1":
		t.Skipf("not supported on %s", runtime.GOOS)
	}

	lo := netip.MustParseAddr("127.0.0.1")
	pc, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(netip.AddrPortFrom(lo, 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	c, err := NewSelectiveUDPConn(pc)
	if err != nil {
		t.Fatal(err)
	}
	client, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(netip.AddrPortFrom(lo, 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

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

// TestSelectiveUDPConnTruncatedOOB ensures control message buffers grow once a read finds its control
// messages truncated, so that packet info is read again, rather than lost for every packet afterwards.
func TestSelectiveUDPConnTruncatedOOB(t *testing.T) {
	switch runtime.GOOS {
	case "js", "plan9", "wasip1":
		t.Skipf("not supported on %s", runtime.GOOS)
	}

	lo := netip.MustParseAddr("127.0.0.1")
	pc, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(netip.AddrPortFrom(lo, 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	c, err := NewSelectiveUDPConn(pc)
	if err != nil {
		t.Fatal(err)
	}
	const size = 8 // Too short for any control message header
	c.(*udpConnWithDualStackPacketInfo).rdOOBSize.Store(size)

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
			if got := c.(*udpConnWithDualStackPacketInfo).rdOOBSize.Load(); got <= size {
				t.Errorf("rdOOBSize = %d, want more than %d", got, size)
			}
			return
		}
	}
	t.Errorf("ReadFromToUDPAddrPort() hasn't returned dst %s after control message buffers grew", lo)
}

// TestSelectiveUDPConnUnmapped ensures a dual-stack socket returns src and dst of IPv4 packets unmapped,
// and that it replies to them.
func TestSelectiveUDPConnUnmapped(t *testing.T) {
	switch runtime.GOOS {
	case "js", "plan9", "wasip1":
		t.Skipf("not supported on %s", runtime.GOOS)
	}

	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv6unspecified})
	if err != nil {
		t.Skipf("ListenUDP() = %v", err) // E.g. no IPv6 support
	}
	defer func() { _ = pc.Close() }()
	c, err := NewSelectiveUDPConn(pc)
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

// TestNewSelectiveUDPConnFamily ensures the IP versions a socket accepts are detected from its local address
// and IPV6_V6ONLY, and that a connection without a local *net.UDPAddr is rejected.
func TestNewSelectiveUDPConnFamily(t *testing.T) {
	switch runtime.GOOS {
	case "js", "plan9", "wasip1":
		t.Skipf("not supported on %s", runtime.GOOS)
	}

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
			c, err := NewSelectiveUDPConn(pc.(*net.UDPConn))
			if err != nil {
				t.Fatalf("NewSelectiveUDPConn() = %v, want nil", err)
			}
			u := c.(*udpConnWithDualStackPacketInfo)
			if u.v4Only != test.v4Only || u.v6Only != test.v6Only || u.net != test.net {
				t.Errorf("NewSelectiveUDPConn() = v4Only %v, v6Only %v, net %s, want %v, %v, %s",
					u.v4Only, u.v6Only, u.net, test.v4Only, test.v6Only, test.net)
			}
			if want := uint16(pc.LocalAddr().(*net.UDPAddr).Port); u.port != want { //nolint:gosec // G115
				t.Errorf("NewSelectiveUDPConn() = port %d, want %d", u.port, want)
			}
		})
	}

	t.Run("NotUDPAddr", func(t *testing.T) {
		pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = pc.Close() }()
		if _, err = NewSelectiveUDPConn(localAddrConn{pc, oneTrunkAddr}); err == nil {
			t.Errorf("NewSelectiveUDPConn() = nil, want an error")
		}
	})
}

func TestUDPAddrFromValidAddrPort(t *testing.T) {
	type args struct {
		ap netip.AddrPort
	}
	tests := []struct {
		name string
		args args
		want *net.UDPAddr
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := UDPAddrFromValidAddrPort(tt.args.ap); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UDPAddrFromValidAddrPort() = %v, want %v", got, tt.want)
			}
		})
	}
}

//func Test_selectiveUDPConn_ReadFromTo(t *testing.T) {
//	type fields struct {
//		UDPConn   *net.UDPConn
//		rdOOB     []byte
//		wr4OOB    []byte
//		wr6OOB    []byte
//		rdMutex   sync.Mutex
//		wrMutex   sync.Mutex
//		fdNet     string
//		fdNetHas4 bool
//		fdNetHas6 bool
//	}
//	type args struct {
//		b []byte
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		want    int
//		want1   net.Addr
//		want2   net.Addr
//		want3   int
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			u := &selectiveUDPConn{
//				UDPConn:   tt.fields.UDPConn,
//				rdOOB:     tt.fields.rdOOB,
//				wr4OOB:    tt.fields.wr4OOB,
//				wr6OOB:    tt.fields.wr6OOB,
//				rdMutex:   tt.fields.rdMutex,
//				wrMutex:   tt.fields.wrMutex,
//				fdNet:     tt.fields.fdNet,
//				fdNetHas4: tt.fields.fdNetHas4,
//				fdNetHas6: tt.fields.fdNetHas6,
//			}
//			got, got1, got2, got3, err := u.ReadFromTo(tt.args.b)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("ReadFromTo() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if got != tt.want {
//				t.Errorf("ReadFromTo() got = %v, want %v", got, tt.want)
//			}
//			if !reflect.DeepEqual(got1, tt.want1) {
//				t.Errorf("ReadFromTo() got1 = %v, want %v", got1, tt.want1)
//			}
//			if !reflect.DeepEqual(got2, tt.want2) {
//				t.Errorf("ReadFromTo() got2 = %v, want %v", got2, tt.want2)
//			}
//			if got3 != tt.want3 {
//				t.Errorf("ReadFromTo() got3 = %v, want %v", got3, tt.want3)
//			}
//		})
//	}
//}
//
//func Test_selectiveUDPConn_ReadFromToUDP(t *testing.T) {
//	type fields struct {
//		UDPConn   *net.UDPConn
//		rdOOB     []byte
//		wrOOB     []byte
//		rdMutex   sync.Mutex
//		wrMutex   sync.Mutex
//		fdNet     string
//		fdNetHas4 bool
//		fdNetHas6 bool
//	}
//	type args struct {
//		b []byte
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		want    int
//		want1   *net.UDPAddr
//		want2   *net.UDPAddr
//		want3   int
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			u := &selectiveUDPConn{
//				UDPConn:   tt.fields.UDPConn,
//				rdOOB:     tt.fields.rdOOB,
//				wrOOB:     tt.fields.wrOOB,
//				rdMutex:   tt.fields.rdMutex,
//				wrMutex:   tt.fields.wrMutex,
//				fdNet:     tt.fields.fdNet,
//				fdNetHas4: tt.fields.fdNetHas4,
//				fdNetHas6: tt.fields.fdNetHas6,
//			}
//			got, got1, got2, got3, err := u.ReadFromToUDP(tt.args.b)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("ReadFromToUDP() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if got != tt.want {
//				t.Errorf("ReadFromToUDP() got = %v, want %v", got, tt.want)
//			}
//			if !reflect.DeepEqual(got1, tt.want1) {
//				t.Errorf("ReadFromToUDP() got1 = %v, want %v", got1, tt.want1)
//			}
//			if !reflect.DeepEqual(got2, tt.want2) {
//				t.Errorf("ReadFromToUDP() got2 = %v, want %v", got2, tt.want2)
//			}
//			if got3 != tt.want3 {
//				t.Errorf("ReadFromToUDP() got3 = %v, want %v", got3, tt.want3)
//			}
//		})
//	}
//}
//
//func Test_selectiveUDPConn_ReadFromToUDPAddrPort(t *testing.T) {
//	type fields struct {
//		UDPConn   *net.UDPConn
//		rdOOB     []byte
//		wrOOB     []byte
//		rdMutex   sync.Mutex
//		wrMutex   sync.Mutex
//		fdNet     string
//		fdNetHas4 bool
//		fdNetHas6 bool
//	}
//	type args struct {
//		b []byte
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		want    int
//		want1   netip.AddrPort
//		want2   netip.AddrPort
//		want3   int
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			u := &selectiveUDPConn{
//				UDPConn:   tt.fields.UDPConn,
//				rdOOB:     tt.fields.rdOOB,
//				wrOOB:     tt.fields.wrOOB,
//				rdMutex:   tt.fields.rdMutex,
//				wrMutex:   tt.fields.wrMutex,
//				fdNet:     tt.fields.fdNet,
//				fdNetHas4: tt.fields.fdNetHas4,
//				fdNetHas6: tt.fields.fdNetHas6,
//			}
//			got, got1, got2, got3, err := u.ReadFromToUDPAddrPort(tt.args.b)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("ReadFromToUDPAddrPort() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if got != tt.want {
//				t.Errorf("ReadFromToUDPAddrPort() got = %v, want %v", got, tt.want)
//			}
//			if !reflect.DeepEqual(got1, tt.want1) {
//				t.Errorf("ReadFromToUDPAddrPort() got1 = %v, want %v", got1, tt.want1)
//			}
//			if !reflect.DeepEqual(got2, tt.want2) {
//				t.Errorf("ReadFromToUDPAddrPort() got2 = %v, want %v", got2, tt.want2)
//			}
//			if got3 != tt.want3 {
//				t.Errorf("ReadFromToUDPAddrPort() got3 = %v, want %v", got3, tt.want3)
//			}
//		})
//	}
//}
//
//func Test_selectiveUDPConn_WriteToFrom(t *testing.T) {
//	type fields struct {
//		UDPConn   *net.UDPConn
//		rdOOB     []byte
//		wrOOB     []byte
//		rdMutex   sync.Mutex
//		wrMutex   sync.Mutex
//		fdNet     string
//		fdNetHas4 bool
//		fdNetHas6 bool
//	}
//	type args struct {
//		b   []byte
//		dst net.Addr
//		src net.Addr
//		idx int
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		want    int
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			u := &selectiveUDPConn{
//				UDPConn:   tt.fields.UDPConn,
//				rdOOB:     tt.fields.rdOOB,
//				wrOOB:     tt.fields.wrOOB,
//				rdMutex:   tt.fields.rdMutex,
//				wrMutex:   tt.fields.wrMutex,
//				fdNet:     tt.fields.fdNet,
//				fdNetHas4: tt.fields.fdNetHas4,
//				fdNetHas6: tt.fields.fdNetHas6,
//			}
//			got, err := u.WriteToFrom(tt.args.b, tt.args.dst, tt.args.src, tt.args.idx)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("WriteToFrom() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if got != tt.want {
//				t.Errorf("WriteToFrom() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func Test_selectiveUDPConn_WriteToFromUDP(t *testing.T) {
//	type fields struct {
//		UDPConn   *net.UDPConn
//		rdOOB     []byte
//		wrOOB     []byte
//		rdMutex   sync.Mutex
//		wrMutex   sync.Mutex
//		fdNet     string
//		fdNetHas4 bool
//		fdNetHas6 bool
//	}
//	type args struct {
//		b   []byte
//		dst *net.UDPAddr
//		src *net.UDPAddr
//		idx int
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		want    int
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			u := &selectiveUDPConn{
//				UDPConn:   tt.fields.UDPConn,
//				rdOOB:     tt.fields.rdOOB,
//				wrOOB:     tt.fields.wrOOB,
//				rdMutex:   tt.fields.rdMutex,
//				wrMutex:   tt.fields.wrMutex,
//				fdNet:     tt.fields.fdNet,
//				fdNetHas4: tt.fields.fdNetHas4,
//				fdNetHas6: tt.fields.fdNetHas6,
//			}
//			got, err := u.WriteToFromUDP(tt.args.b, tt.args.dst, tt.args.src, tt.args.idx)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("WriteToFromUDP() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if got != tt.want {
//				t.Errorf("WriteToFromUDP() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func Test_selectiveUDPConn_WriteToFromUDPAddrPort(t *testing.T) {
//	type fields struct {
//		UDPConn   *net.UDPConn
//		rdOOB     []byte
//		wrOOB     []byte
//		rdMutex   sync.Mutex
//		wrMutex   sync.Mutex
//		fdNet     string
//		fdNetHas4 bool
//		fdNetHas6 bool
//	}
//	type args struct {
//		b   []byte
//		dst netip.AddrPort
//		src netip.AddrPort
//		idx int
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		wantN   int
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			u := &selectiveUDPConn{
//				UDPConn:   tt.fields.UDPConn,
//				rdOOB:     tt.fields.rdOOB,
//				wrOOB:     tt.fields.wrOOB,
//				rdMutex:   tt.fields.rdMutex,
//				wrMutex:   tt.fields.wrMutex,
//				fdNet:     tt.fields.fdNet,
//				fdNetHas4: tt.fields.fdNetHas4,
//				fdNetHas6: tt.fields.fdNetHas6,
//			}
//			gotN, err := u.WriteToFromUDPAddrPort(tt.args.b, tt.args.dst, tt.args.src, tt.args.idx)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("WriteToFromUDPAddrPort() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if gotN != tt.wantN {
//				t.Errorf("WriteToFromUDPAddrPort() gotN = %v, want %v", gotN, tt.wantN)
//			}
//		})
//	}
//}

func TestDualStackPacketInfo(t *testing.T) {
	switch runtime.GOOS {
	case "js", "plan9", "wasip1":
		t.Skipf("not supported on %s", runtime.GOOS)
	}

	ifs, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}

	type config struct {
		name   string
		rdNet  string
		wrNet  string
		rdAddr netip.Addr
		wrAddr netip.Addr
		rdSrc  netip.Addr
		rdDst  netip.Addr
		wrDst  netip.Addr
		wrSrc  netip.Addr
		rdIdx  int
		wrIdx  int
	}
	configs := make([]config, 0, 100)
	makeConfig := func(name, net string, addr, rdSrc, rdDst, wrDst, wrSrc netip.Addr, rdIdx, wrIdx int) config {
		return config{
			name:   name,
			rdNet:  net,
			wrNet:  net,
			rdAddr: addr,
			wrAddr: addr,
			rdSrc:  rdSrc,
			rdDst:  rdDst,
			wrDst:  wrDst,
			wrSrc:  wrSrc,
			rdIdx:  rdIdx,
			wrIdx:  wrIdx,
		}
	}

	var addr, a1, a2 netip.Addr
	var addrs []netip.Addr
	var ifc net.Interface
	var ipc net.Addr
	var ipi, loIdx int
	var ips []net.Addr
	var lo127001, lo127002 netip.Addr
	var ok bool
	for _, l := range []struct {
		net  string
		addr netip.Addr
	}{
		{net: "udp", addr: netip.IPv6Unspecified()},
		{net: "udp4", addr: netip.IPv4Unspecified().Unmap()},
		{net: "udp6", addr: netip.IPv6Unspecified()},
	} {
		for _, ifc = range ifs {
			if ifc.Flags&net.FlagUp > 0 && ifc.Flags&net.FlagRunning > 0 {
				ips, err = ifc.Addrs()
				if err != nil {
					t.Error(err)
					continue
				} else if ifc.Flags&net.FlagLoopback > 0 {
					loIdx = ifc.Index
				} else if len(ips) == 0 {
					continue
				}

				// t.Logf("Found interface #%d `%s` with addresses:", ifc.Index, ifc.Name)

				addrs = make([]netip.Addr, len(ips))
				for ipi, ipc = range ips {
					switch ipc.(type) {
					case *net.IPAddr:
						if addr, ok = netip.AddrFromSlice(ipc.(*net.IPAddr).IP); ok {
							addr = addr.WithZone(ipc.(*net.IPAddr).Zone)
							if addr.IsLinkLocalUnicast() {
								addr = addr.WithZone(strconv.Itoa(ifc.Index))
							}
							addr = addr.Unmap()
							addrs[ipi] = addr
							// t.Logf(" - %s", addr)
						}
					case *net.IPNet:
						if addr, ok = netip.AddrFromSlice(ipc.(*net.IPNet).IP); ok {
							if addr.IsLinkLocalUnicast() {
								addr = addr.WithZone(zoneCache.name(ifc.Index))
							}
							addr = addr.Unmap()
							addrs[ipi] = addr
							// t.Logf(" - %s", addr)
						}
					}
				}

				for _, a1 = range addrs {
					if !a1.IsValid() || (a1.Is4() && l.net == "udp6") || (a1.Is6() && l.net == "udp4") {
						continue
					}

					configs = append(configs, makeConfig(
						fmt.Sprintf("%s,blank,0", a1), l.net, l.addr, a1, a1, a1, netip.Addr{}, ifc.Index, 0))
					configs = append(configs, makeConfig(
						fmt.Sprintf("%s,blank,%d", a1, ifc.Index), l.net, l.addr, a1, a1, a1, netip.Addr{}, ifc.Index, ifc.Index))

					for _, a2 = range addrs {
						if a1.Is4() == a2.Is4() && a1.IsLinkLocalUnicast() == a2.IsLinkLocalUnicast() {
							configs = append(configs, makeConfig(
								fmt.Sprintf("%s,%s,0", a1, a2), l.net, l.addr, a2, a1, a1, a2, ifc.Index, 0))
							configs = append(configs, makeConfig(
								fmt.Sprintf("%s,%s,%d", a1, a2, ifc.Index), l.net, l.addr, a2, a1, a1, a2, ifc.Index, ifc.Index))
						}
					}
				}
			}
		}

		if l.net != "udp6" {
			lo127001 = netip.AddrFrom4([4]byte{127, 0, 0, 1})
			lo127002 = netip.AddrFrom4([4]byte{127, 0, 0, 2})
			configs = append(configs, makeConfig(
				fmt.Sprintf("%s,blank,0", lo127002), l.net, l.addr, lo127001, lo127002, lo127002, netip.Addr{}, loIdx, 0))
			configs = append(configs, makeConfig(
				fmt.Sprintf("%s,blank,%d", lo127002, loIdx), l.net, l.addr, lo127001, lo127002, lo127002, netip.Addr{}, loIdx, loIdx))
			configs = append(configs, makeConfig(
				fmt.Sprintf("%s,%s,0", lo127002, lo127001), l.net, l.addr, lo127001, lo127002, lo127002, lo127001, loIdx, 0))
			configs = append(configs, makeConfig(
				fmt.Sprintf("%s,%s,%d", lo127002, lo127001, loIdx), l.net, l.addr, lo127001, lo127002, lo127002, lo127001, loIdx, loIdx))
		}
	}

	var done chan struct{}
	var lc net.ListenConfig
	var rdc, wrc SelectiveUDPConn
	var rdErr, wrErr error
	var rdPC, wrPC PacketConn
	var rdPort, wrPort uint16
	var wg sync.WaitGroup
	rdb, wrb := make([]byte, 1), make([]byte, 0)
	for _, c := range configs {
		t.Run(fmt.Sprintf("(%s,%s)", c.rdNet, c.name), func(t *testing.T) {
			rdPC, rdErr = lc.ListenPacket(context.Background(), c.rdNet, netip.AddrPortFrom(c.rdAddr, rdPort).String())
			if rdErr != nil {
				t.Error(rdErr)
				_ = rdPC.Close()
				return
			}
			wrPC, wrErr = lc.ListenPacket(context.Background(), c.wrNet, netip.AddrPortFrom(c.wrAddr, wrPort).String())
			if wrErr != nil {
				t.Error(wrErr)
				_ = rdPC.Close()
				_ = wrPC.Close()
				return
			}

			rdc, rdErr = NewSelectiveUDPConn(rdPC.(*net.UDPConn))
			if rdErr != nil {
				t.Error(rdErr)
				_ = rdPC.Close()
				_ = wrPC.Close()
				return
			}
			wrc, wrErr = NewSelectiveUDPConn(wrPC.(*net.UDPConn))
			if wrErr != nil {
				t.Error(wrErr)
				_ = rdPC.Close()
				_ = wrPC.Close()
				return
			}

			rdPort, wrPort = uint16((rdPC.LocalAddr()).(*net.UDPAddr).Port), uint16((wrPC.LocalAddr()).(*net.UDPAddr).Port)

			done = make(chan struct{})
			wg.Add(1)
			go func(t *testing.T, c SelectiveUDPConn, b []byte, dst, src netip.AddrPort, idx int, done chan struct{}, wg *sync.WaitGroup) {
				var err error
				for {
					_ = c.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
					_, err = c.WriteToFromUDPAddrPort(b, dst, src, idx)
					if err != nil {
						t.Error(err)
						break
					} else {
						t.Logf("wrc.WriteToFromUDPAddrPort(_, %v, %v, %v) = _, nil", dst, src, idx)
					}
					time.Sleep(10 * time.Millisecond)
					if isClosedChan(done) {
						break
					}
				}
				wg.Done()
			}(t, wrc, wrb, netip.AddrPortFrom(c.wrDst, rdPort), netip.AddrPortFrom(c.wrSrc, wrPort), c.wrIdx, done, &wg)

			err = rdc.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			if err != nil {
				t.Error(err)
				return
			}
			_, src, dst, idx, err := rdc.ReadFromToUDPAddrPort(rdb)
			if err != nil {
				t.Error(err)
			} else {
				t.Logf("rdc.ReadFromToUDPAddrPort(_) = _, %v, %v, %v, _", src, dst, idx)
			}
			switch {
			case (c.wrSrc.IsValid() || c.wrIdx == 0) && src.Addr().Unmap() != c.rdSrc:
				t.Errorf("rdc.ReadFromToUDPAddrPort(_): src.Addr().Unmap() = %v; want %v", src.Addr().Unmap(), c.rdSrc)
			case !(c.wrSrc.IsValid() || c.wrIdx == 0) && !src.Addr().Unmap().IsUnspecified():
				t.Errorf("rdc.ReadFromToUDPAddrPort(_): src.Addr().Unmap() = %v; want %v", src.Addr().Unmap(), c.rdSrc)
			case dst.Addr().Unmap() != c.rdDst:
				t.Errorf("rdc.ReadFromToUDPAddrPort(_): dst.Addr().Unmap() = %v; want %v", dst.Addr().Unmap(), c.rdDst)
			case idx != c.rdIdx:
				t.Errorf("rdc.ReadFromToUDPAddrPort(_): idx = %d; want %d", idx, c.rdIdx)
			}
			close(done)
			wg.Wait()

			if rdErr = rdPC.Close(); rdErr != nil {
				t.Error(rdErr)
			}
			if wrErr = wrPC.Close(); wrErr != nil {
				t.Error(wrErr)
			}
		})
	}
}

func TestUDPConn2(t *testing.T) {
	// Listen on a UDP socket
	server, err := net.ListenPacket("udp4", ":12345")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()

	// Get the raw connection
	rawConn, err := server.(interface {
		SyscallConn() (syscall.RawConn, error)
	}).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}

	// Enable IP_PKTINFO socket option
	err = rawConn.Control(func(fd uintptr) {
		//if runtime.GOOS == "linux" {
		//	err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_PKTINFO, 1)
		//} else if runtime.GOOS == "darwin" || runtime.GOOS == "freebsd" {
		//	// Use IP_RECVDSTADDR on BSD-based systems for similar functionality
		//	err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_RECVDSTADDR, 1)
		//}
		err = windows.SetsockoptInt(windows.Handle(fd), windows.IPPROTO_IP, windows.IP_PKTINFO, 1)
		if err != nil {
			t.Fatalf("failed to set socket option: %v\n", err)
		}
		//err = windows.SetsockoptInt(windows.Handle(fd), windows.IPPROTO_IP, windows.IP_PKTINFO, 1)
		//if err != nil {
		//	t.Fatalf("failed to set socket option: %v\n", err)
		//}
		//err = windows.SetsockoptInt(windows.Handle(fd), windows.IPPROTO_IPV6, windows.IPV6_PKTINFO, 1)
		//if err != nil {
		//	t.Fatalf("failed to set socket option: %v\n", err)
		//}
		//err = windows.SetsockoptInt(windows.Handle(fd), windows.IPPROTO_IPV6, windows.IPV6_PKTINFO, 1)
		//if err != nil {
		//	t.Fatalf("failed to set socket option: %v\n", err)
		//}
	})

	go func(t *testing.T) {
		client, err := net.ListenPacket("udp4", ":0")
		if err != nil {
			t.Error(err)
		}
		defer func() { _ = client.Close() }()

		_, err = client.WriteTo([]byte{}, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345})
		if err != nil {
			t.Error(err)
		}
	}(t)

	// Use ipv4.NewPacketConn to read messages with control data
	pc := ipv4.NewPacketConn(server)

	// Read a packet and control message
	buffer := make([]byte, 1500)
	// oob := make([]byte, 100) // Out-of-band data buffer
	_, cm, srcAddr, err := pc.ReadFrom(buffer)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("PacketConn.ReadFrom() = %v, %v", cm, srcAddr)
}

func TestUDPConn3(t *testing.T) {
	serverPC, err := net.ListenPacket("udp4", ":12345")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = serverPC.Close() }()

	p4 := ipv4.NewPacketConn(serverPC)
	p6 := ipv6.NewPacketConn(serverPC)

	err = p4.SetControlMessage(ipv4.FlagDst|ipv4.FlagInterface, true)
	if err != nil {
		t.Logf("IPv4 SetControlMessage failed: %v", err)
	}
	err = p6.SetControlMessage(ipv6.FlagDst|ipv6.FlagInterface, true)
	if err != nil {
		t.Logf("IPv6 SetControlMessage failed: %v", err)
	}

	go func(t *testing.T) {
		clientPC, err := net.ListenPacket("udp4", ":0")
		if err != nil {
			t.Error(err)
		}
		defer func() { _ = clientPC.Close() }()

		_, err = clientPC.WriteTo([]byte{}, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345})
		if err != nil {
			t.Error(err)
		}
	}(t)

	// Read a packet and control message
	buffer := make([]byte, 1500)
	// oob := make([]byte, 100) // Out-of-band data buffer
	_, cm, srcAddr, err := p4.ReadFrom(buffer)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("IPv4 PacketConn.ReadFrom() = %v, %v", cm, srcAddr)
}
