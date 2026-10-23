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
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// TestWrapper runs the tests of the wrappers of connections.
func TestWrapper(t *testing.T) {
	t.Run(testWrapperFakeAddressPacketConnName, testWrapperFakeAddressPacketConn)
	t.Run(testWrapperFakeAddressStreamConnName, testWrapperFakeAddressStreamConn)
	t.Run(testWrapperFakeCloseName, testWrapperFakeClose)
	t.Run(testWrapperIdleTimeoutName, testWrapperIdleTimeout)
	t.Run(testWrapperPacketToFromToPacketConnName, testWrapperPacketToFromToPacketConn)
	t.Run(testWrapperPacketToStreamConnName, testWrapperPacketToStreamConn)
	t.Run(testWrapperStreamToPacketConnName, testWrapperStreamToPacketConn)
}

// testWrapperFakeAddressPacketConn ensures FakeAddressPacketConn returns the addresses given instead of the real ones,
// sends every packet to the remote address given, and passes the real addresses through if none are given.
func testWrapperFakeAddressPacketConn(t *testing.T) {
	a1 := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 1}
	a2 := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 2}
	local := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 3}
	remote := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 4}
	other := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 3), Port: 5}
	b := make([]byte, 8)

	for _, test := range []struct {
		name                    string
		local, remote           net.Addr
		wantLocal, wantFrom, to net.Addr // Wanted local address, wanted source of a packet read, destination written to
	}{
		{"Real", nil, nil, a1, a2, other},
		{"Fake", local, remote, local, remote, remote},
	} {
		t.Run(test.name, func(t *testing.T) {
			c1, c2 := BufferedTrunk(4, a1, a2)
			defer func() { _ = c2.Close() }()
			w := FakeAddressPacketConn(c1, test.local, test.remote)

			if got := w.LocalAddr(); got.String() != test.wantLocal.String() {
				t.Errorf("LocalAddr() = %v, want %v", got, test.wantLocal)
			}

			if _, err := w.WriteTo([]byte("ping"), other); err != nil {
				t.Fatalf("WriteTo() = %v, want nil", err)
			}
			if n, _, dst, _, err := c2.ReadFromTo(b); err != nil || string(b[:n]) != "ping" || dst.String() != test.to.String() {
				t.Errorf("ReadFromTo() = %q to %v, %v, want %q to %v, nil", b[:n], dst, err, "ping", test.to)
			}

			if _, err := c2.WriteTo([]byte("pong"), nil); err != nil {
				t.Fatalf("WriteTo() = %v, want nil", err)
			}
			if n, src, err := w.ReadFrom(b); err != nil || string(b[:n]) != "pong" || src.String() != test.wantFrom.String() {
				t.Errorf("ReadFrom() = %q from %v, %v, want %q from %v, nil", b[:n], src, err, "pong", test.wantFrom)
			}

			if got := w.Unwrap(); got != c1 {
				t.Errorf("Unwrap() = %v, want the trunk end", got)
			}

			_ = w.Close()
			if _, src, err := w.ReadFrom(b); !errors.Is(err, net.ErrClosed) || src != nil {
				t.Errorf("ReadFrom() after Close() = from %v, %v, want from nil, net.ErrClosed", src, err)
			}
		})
	}
}

// testWrapperFakeAddressStreamConn ensures FakeAddressStreamConn returns the addresses given instead of the real
// ones, and passes the real addresses through if none are given.
func testWrapperFakeAddressStreamConn(t *testing.T) {
	local := &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	remote := &net.TCPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	c1, c2 := net.Pipe()
	defer func() { _, _ = c1.Close(), c2.Close() }()

	for _, test := range []struct {
		name                  string
		local, remote         net.Addr
		wantLocal, wantRemote net.Addr
	}{
		{"Real", nil, nil, c1.LocalAddr(), c1.RemoteAddr()},
		{"Fake", local, remote, local, remote},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := FakeAddressStreamConn(c1, test.local, test.remote)
			if got := w.LocalAddr(); got != test.wantLocal {
				t.Errorf("LocalAddr() = %v, want %v", got, test.wantLocal)
			}
			if got := w.RemoteAddr(); got != test.wantRemote {
				t.Errorf("RemoteAddr() = %v, want %v", got, test.wantRemote)
			}
			if got := w.Unwrap(); got != c1 {
				t.Errorf("Unwrap() = %v, want the pipe end", got)
			}
		})
	}
}

// testWrapperFakeClose ensures FakeClosePacketConn and FakeCloseStreamConn call the close function given, if any,
// on every Close instead of closing the underlying connection, which goes on working, and return its error.
func testWrapperFakeClose(t *testing.T) {
	type subject struct {
		w         io.Closer
		closes    *atomic.Int32 // Closings of the underlying connection
		unwrap    func() any
		under     any
		roundTrip func() error // Sends a byte through the wrapper, and then the other way
	}

	for _, kind := range []struct {
		name string
		ctor func(t *testing.T, closeFunc func() error) subject
	}{
		{"PacketConn", func(t *testing.T, closeFunc func() error) subject {
			c1, c2 := BufferedTrunk(4, nil, nil)
			t.Cleanup(func() { _, _ = c1.Close(), c2.Close() })
			u := &closeCountingPacketConn{PacketConn: c1}
			w := FakeClosePacketConn(u, closeFunc)
			b := make([]byte, 8)
			return subject{w, &u.closes, func() any { return w.Unwrap() }, u, func() error {
				if _, err := w.WriteTo([]byte{1}, nil); err != nil {
					return err
				}
				if _, _, err := c2.ReadFrom(b); err != nil {
					return err
				}
				if _, err := c2.WriteTo([]byte{2}, nil); err != nil {
					return err
				}
				_, _, err := w.ReadFrom(b)
				return err
			}}
		}},
		{"StreamConn", func(t *testing.T, closeFunc func() error) subject {
			c1, c2 := BufferedPipe(4)
			t.Cleanup(func() { _, _ = c1.Close(), c2.Close() })
			u := &closeCountingStreamConn{StreamConn: c1}
			w := FakeCloseStreamConn(u, closeFunc)
			b := make([]byte, 8)
			return subject{w, &u.closes, func() any { return w.Unwrap() }, u, func() error {
				if _, err := w.Write([]byte{1}); err != nil {
					return err
				}
				if _, err := c2.Read(b); err != nil {
					return err
				}
				if _, err := c2.Write([]byte{2}); err != nil {
					return err
				}
				_, err := w.Read(b)
				return err
			}}
		}},
	} {
		t.Run(kind.name, func(t *testing.T) {
			errHook := errors.New("hook")
			for _, test := range []struct {
				name  string
				hook  bool
				calls int   // Wanted calls of the hook after two closings
				err   error // Wanted error of each closing
			}{
				{"NilHook", false, 0, nil},
				{"Hook", true, 2, errHook},
			} {
				t.Run(test.name, func(t *testing.T) {
					var calls int
					var closeFunc func() error
					if test.hook {
						closeFunc = func() error { calls++; return errHook }
					}
					s := kind.ctor(t, closeFunc)

					for i := range 2 {
						if err := s.w.Close(); !errors.Is(err, test.err) {
							t.Errorf("Close() #%d = %v, want %v", i+1, err, test.err)
						}
					}
					if calls != test.calls {
						t.Errorf("calls of the hook = %d, want %d", calls, test.calls)
					}
					if n := s.closes.Load(); n != 0 {
						t.Errorf("closures of the underlying connection = %d, want 0", n)
					}
					if err := s.roundTrip(); err != nil {
						t.Errorf("round trip after Close() = %v, want nil", err)
					}
					if got := s.unwrap(); got != s.under {
						t.Errorf("Unwrap() = %v, want the connection given", got)
					}
				})
			}
		})
	}
}

// testWrapperIdleTimeout ensures IdleTimeoutPacketConn and IdleTimeoutStreamConn close the underlying connection
// once idle for the timeout, kept open by reads and by writes, never with a timeout that isn't positive, and
// never once Close or Stop has stopped the timer, even if the wrapper is still used, while Unwrap stops nothing.
func testWrapperIdleTimeout(t *testing.T) {
	const idle = 200 * time.Millisecond // Long enough for the scheduling stalls of a busy machine

	// subject is a wrapper with an idle timeout over a connection counting its closures,
	// and the means to read and write through the wrapper.
	type subject struct {
		w interface {
			io.Closer
			Stopper
		}
		under  any
		closes *atomic.Int32
		unwrap func() any
		read   func() error
		write  func() error
	}
	kinds := []struct {
		name string
		ctor func(t *testing.T, timeout time.Duration) subject
	}{
		{"PacketConn", func(t *testing.T, timeout time.Duration) subject {
			c1, c2 := BufferedTrunk(64, nil, nil)
			t.Cleanup(func() { _, _ = c1.Close(), c2.Close() })
			u := &closeCountingPacketConn{PacketConn: c1}
			w := IdleTimeoutPacketConn(u, timeout)
			b := make([]byte, 8)
			return subject{w, u, &u.closes,
				func() any { return w.Unwrap() },
				func() error {
					if _, err := c2.WriteTo([]byte{1}, nil); err != nil {
						return err
					}
					_, _, err := w.ReadFrom(b)
					return err
				},
				func() error { _, err := w.WriteTo([]byte{1}, nil); return err },
			}
		}},
		{"StreamConn", func(t *testing.T, timeout time.Duration) subject {
			c1, c2 := BufferedPipe(64)
			t.Cleanup(func() { _, _ = c1.Close(), c2.Close() })
			u := &closeCountingStreamConn{StreamConn: c1}
			w := IdleTimeoutStreamConn(u, timeout)
			b := make([]byte, 8)
			return subject{w, u, &u.closes,
				func() any { return w.Unwrap() },
				func() error {
					if _, err := c2.Write([]byte{1}); err != nil {
						return err
					}
					_, err := w.Read(b)
					return err
				},
				func() error { _, err := w.Write([]byte{1}); return err },
			}
		}},
	}

	for _, kind := range kinds {
		t.Run(kind.name, func(t *testing.T) {
			t.Run("Expired", func(t *testing.T) {
				s := kind.ctor(t, idle)
				time.Sleep(2 * idle)
				if got := s.closes.Load(); got != 1 {
					t.Fatalf("closures of an idle connection = %d, want 1", got)
				}
				if err := s.w.Stop(); !errors.Is(err, net.ErrClosed) {
					t.Errorf("Stop() = %v, want net.ErrClosed, as the timer has closed the connection", err)
				}
				if err := s.w.Close(); !errors.Is(err, net.ErrClosed) {
					t.Errorf("Close() = %v, want net.ErrClosed, as the timer has closed the connection", err)
				}
			})

			for _, act := range []string{"KeptByReads", "KeptByWrites"} {
				t.Run(act, func(t *testing.T) {
					s := kind.ctor(t, idle)
					op := s.read
					if act == "KeptByWrites" {
						op = s.write
					}
					for range 12 { // Three idle timeouts in all
						time.Sleep(idle / 4)
						if err := op(); err != nil {
							t.Fatalf("%s: %v", act, err)
						}
					}
					if got := s.closes.Load(); got != 0 {
						t.Fatalf("closures of an active connection = %d, want 0", got)
					}
					time.Sleep(2 * idle)
					if got := s.closes.Load(); got != 1 {
						t.Errorf("closures of a connection idle since = %d, want 1", got)
					}
				})
			}

			for _, timeout := range []time.Duration{0, -idle} {
				t.Run("Disabled"+timeout.String(), func(t *testing.T) {
					s := kind.ctor(t, timeout)
					if err := s.write(); err != nil {
						t.Fatalf("write: %v", err)
					}
					time.Sleep(idle / 4) // A timer would have fired at once
					if got := s.closes.Load(); got != 0 {
						t.Errorf("closures with timeout %v = %d, want 0", timeout, got)
					}
				})
			}

			t.Run("StoppedByClose", func(t *testing.T) {
				s := kind.ctor(t, idle)
				if err := s.w.Close(); err != nil {
					t.Fatalf("Close() = %v, want nil", err)
				}
				time.Sleep(2 * idle)
				if got := s.closes.Load(); got != 1 {
					t.Errorf("closures = %d, want 1, by Close only", got)
				}
			})

			t.Run("StoppedByStop", func(t *testing.T) {
				s := kind.ctor(t, idle)
				if err := s.w.Stop(); err != nil {
					t.Fatalf("Stop() = %v, want nil", err)
				}
				if err := s.write(); err != nil { // Using the wrapper after Stop mustn't restart the timer
					t.Fatalf("write: %v", err)
				}
				time.Sleep(2 * idle)
				if got := s.closes.Load(); got != 0 {
					t.Errorf("closures of a connection with a stopped timer = %d, want 0", got)
				}
				if err := s.w.Stop(); !errors.Is(err, net.ErrClosed) {
					t.Errorf("Stop() again = %v, want net.ErrClosed", err)
				}
				if err := s.w.Close(); err != nil || s.closes.Load() != 1 {
					t.Errorf("Close() after Stop() = %v with %d closures, want nil with 1", err, s.closes.Load())
				}
			})

			t.Run("KeptByUnwrap", func(t *testing.T) {
				s := kind.ctor(t, idle)
				if got := s.unwrap(); got != s.under {
					t.Fatalf("Unwrap() = %v, want the underlying connection", got)
				}
				time.Sleep(2 * idle)
				if got := s.closes.Load(); got != 1 {
					t.Errorf("closures of an idle connection unwrapped = %d, want 1, as Unwrap stops nothing", got)
				}
			})
		})
	}
}

// testWrapperPacketToFromToPacketConn ensures PacketToFromToPacketConn returns a FromToPacketConn as is, enables
// packet info on a UDP connection if asked to and supported, and otherwise wraps a PacketConn, so that it reads
// no destination address and interface index, and ignores the source address and interface index it writes.
func testWrapperPacketToFromToPacketConn(t *testing.T) {
	a1 := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 1}
	a2 := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 2}
	src := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 3}
	dst := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 4}
	b := make([]byte, 8)

	t.Run("FromToPacketConn", func(t *testing.T) {
		c1, c2 := BufferedTrunk(4, a1, a2)
		defer func() { _, _ = c1.Close(), c2.Close() }()
		if got := PacketToFromToPacketConn(c1, true); got != c1 {
			t.Errorf("PacketToFromToPacketConn() = %v, want the trunk end as is", got)
		}
	})

	t.Run("PacketConn", func(t *testing.T) {
		c1, c2 := BufferedTrunk(4, a1, a2)
		defer func() { _, _ = c1.Close(), c2.Close() }()
		pc := FakeAddressPacketConn(c1, nil, nil) // Hides the methods of FromToPacketConn
		w := PacketToFromToPacketConn(pc, true)

		if _, err := c2.WriteToFrom([]byte("ping"), dst, src, 7); err != nil {
			t.Fatalf("WriteToFrom() = %v, want nil", err)
		}
		if n, from, to, idx, err := w.ReadFromTo(b); err != nil || string(b[:n]) != "ping" ||
			from.String() != src.String() || to != nil || idx != 0 {
			t.Errorf("ReadFromTo() = %q from %v to %v on %d, %v, want %q from %v to nil on 0, nil",
				b[:n], from, to, idx, err, "ping", src)
		}

		if _, err := w.WriteToFrom([]byte("pong"), dst, src, 7); err != nil {
			t.Fatalf("WriteToFrom() = %v, want nil", err)
		}
		if n, from, to, idx, err := c2.ReadFromTo(b); err != nil || string(b[:n]) != "pong" ||
			from.String() != a1.String() || to.String() != dst.String() || idx != 0 {
			t.Errorf("ReadFromTo() = %q from %v to %v on %d, %v, want %q from %v to %v on 0, nil",
				b[:n], from, to, idx, err, "pong", a1, dst)
		}

		if got := w.(Wrapper[PacketConn]).Unwrap(); got != pc {
			t.Errorf("Unwrap() = %v, want the PacketConn given", got)
		}
	})

	t.Run("UDPConn", func(t *testing.T) {
		for _, try := range []bool{false, true} {
			conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			_, unsupported := PacketInfoUDPConn(conn)
			w := PacketToFromToPacketConn(conn, try)
			if _, ok := w.(FromToUDPConn); ok != (try && unsupported == nil) {
				t.Errorf("PacketToFromToPacketConn(%v) is a FromToUDPConn = %v, want %v, as PacketInfoUDPConn() = %v",
					try, ok, !ok, unsupported)
			}
			if got := w.(Wrapper[PacketConn]).Unwrap(); got != conn {
				t.Errorf("PacketToFromToPacketConn(%v).Unwrap() = %v, want the UDPConn given", try, got)
			}
			_ = conn.Close()
		}
	})
}

// testWrapperPacketToStreamConn ensures PacketToStreamConn writes to the remote address given, reads a packet
// from any address unless filtered, or from the remote address only, dropping the others, and truncates a packet
// longer than the read buffer, as the underlying PacketConn does.
func testWrapperPacketToStreamConn(t *testing.T) {
	a1 := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 1}
	a2 := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 2}
	remote := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 3}
	mapped := &net.UDPAddr{IP: net.ParseIP("::ffff:192.0.2.1"), Port: 3} // As a dual-stack socket reports remote
	other := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 3}

	// pair returns the wrapper over one end of a trunk, and the other end, sending packets from a2 unless told.
	pair := func(t *testing.T, remote net.Addr, filter bool) (StreamOverPacketConn, FromToPacketConn) {
		c1, c2 := BufferedTrunk(8, a1, a2)
		t.Cleanup(func() { _, _ = c1.Close(), c2.Close() })
		w := PacketToStreamConn(c1, remote, filter)
		if got := w.Unwrap(); got != c1 {
			t.Errorf("Unwrap() = %v, want the trunk end", got)
		}
		return w, c2
	}
	send := func(t *testing.T, c FromToPacketConn, data string, src net.Addr) {
		if _, err := c.WriteToFrom([]byte(data), nil, src, 0); err != nil {
			t.Fatalf("WriteToFrom() = %v, want nil", err)
		}
	}

	t.Run("NoFilter", func(t *testing.T) {
		w, c2 := pair(t, remote, false)
		b := make([]byte, 8)

		if got := w.RemoteAddr(); got != remote {
			t.Errorf("RemoteAddr() = %v, want %v", got, remote)
		}

		if _, err := w.Write([]byte("ping")); err != nil {
			t.Fatalf("Write() = %v, want nil", err)
		}
		if n, _, to, _, err := c2.ReadFromTo(b); err != nil || string(b[:n]) != "ping" || to.String() != remote.String() {
			t.Errorf("ReadFromTo() = %q to %v, %v, want %q to %v, nil", b[:n], to, err, "ping", remote)
		}

		send(t, c2, "pong", nil)
		send(t, c2, "truncated", other)
		if n, err := w.Read(b[:4]); err != nil || string(b[:n]) != "pong" {
			t.Errorf("Read() = %q, %v, want %q, nil", b[:n], err, "pong")
		}
		if n, err := w.Read(b[:4]); !errors.Is(err, io.ErrShortBuffer) || string(b[:n]) != "trun" {
			t.Errorf("Read() = %q, %v, want %q, io.ErrShortBuffer", b[:n], err, "trun")
		}
	})

	t.Run("Filter", func(t *testing.T) {
		w, c2 := pair(t, remote, true)
		b := make([]byte, 8)

		send(t, c2, "a", other)
		send(t, c2, "b", remote)
		send(t, c2, "truncated", other) // Its error is dropped along with it
		send(t, c2, "c", nil)           // From a2
		send(t, c2, "d", mapped)
		send(t, c2, "truncated", remote)
		for _, want := range []string{"b", "d"} {
			if n, err := w.Read(b[:4]); err != nil || string(b[:n]) != want {
				t.Errorf("Read() = %q, %v, want %q, nil", b[:n], err, want)
			}
		}
		if n, err := w.Read(b[:4]); !errors.Is(err, io.ErrShortBuffer) || string(b[:n]) != "trun" {
			t.Errorf("Read() = %q, %v, want %q, io.ErrShortBuffer", b[:n], err, "trun")
		}
	})

	t.Run("FilterDeadline", func(t *testing.T) {
		w, c2 := pair(t, remote, true)
		b := make([]byte, 8)

		send(t, c2, "a", other)
		send(t, c2, "b", nil)
		_ = w.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		var nerr net.Error
		if n, err := w.Read(b); !errors.As(err, &nerr) || !nerr.Timeout() || n != 0 {
			t.Errorf("Read() = %q, %v, want \"\", a timeout", b[:n], err)
		}
	})

	t.Run("FilterNilRemote", func(t *testing.T) { // The filter is ignored, rather than every packet dropped
		w, c2 := pair(t, nil, true)
		b := make([]byte, 8)

		send(t, c2, "a", other)
		if n, err := w.Read(b); err != nil || string(b[:n]) != "a" {
			t.Errorf("Read() = %q, %v, want %q, nil", b[:n], err, "a")
		}
	})

	// readClosed ensures Read returns net.ErrClosed, rather than goes on reading.
	readClosed := func(t *testing.T, w StreamOverPacketConn) {
		done := make(chan error, 1)
		go func() {
			_, err := w.Read(make([]byte, 8))
			done <- err
		}()
		select {
		case err := <-done:
			if !errors.Is(err, net.ErrClosed) {
				t.Errorf("Read() = %v, want net.ErrClosed", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Read() hasn't returned once closed")
		}
	}

	t.Run("FilterClose", func(t *testing.T) {
		w, c2 := pair(t, remote, true)

		send(t, c2, "a", other)
		time.AfterFunc(50*time.Millisecond, func() { _ = w.Close() })
		readClosed(t, w)
	})

	t.Run("FilterStaleSource", func(t *testing.T) { // A failed read returns, whatever address it reports
		readClosed(t, PacketToStreamConn(&staleSourcePacketConn{src: other}, remote, true))
	})
}

// testWrapperStreamToPacketConn ensures StreamToPacketConn reads from the remote address of the StreamConn,
// and writes to it whatever address it's given.
func testWrapperStreamToPacketConn(t *testing.T) {
	c1, c2 := BufferedPipe(4)
	defer func() { _, _ = c1.Close(), c2.Close() }()
	w := StreamToPacketConn(c1)
	b := make([]byte, 8)

	if _, err := w.WriteTo([]byte("ping"), &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}); err != nil {
		t.Fatalf("WriteTo() = %v, want nil", err)
	}
	if n, err := c2.Read(b); err != nil || string(b[:n]) != "ping" {
		t.Errorf("Read() = %q, %v, want %q, nil", b[:n], err, "ping")
	}

	if _, err := c2.Write([]byte("pong")); err != nil {
		t.Fatalf("Write() = %v, want nil", err)
	}
	if n, from, err := w.ReadFrom(b); err != nil || string(b[:n]) != "pong" || from != c1.RemoteAddr() {
		t.Errorf("ReadFrom() = %q from %v, %v, want %q from %v, nil", b[:n], from, err, "pong", c1.RemoteAddr())
	}

	if got := w.Unwrap(); got != c1 {
		t.Errorf("Unwrap() = %v, want the pipe end", got)
	}
}

// closeCountingPacketConn is a PacketConn counting the calls of Close.
type closeCountingPacketConn struct {
	PacketConn
	closes atomic.Int32
}

func (c *closeCountingPacketConn) Close() error {
	c.closes.Add(1)
	return c.PacketConn.Close()
}

// closeCountingStreamConn is a StreamConn counting the calls of Close.
type closeCountingStreamConn struct {
	StreamConn
	closes atomic.Int32
}

func (c *closeCountingStreamConn) Close() error {
	c.closes.Add(1)
	return c.StreamConn.Close()
}

// staleSourcePacketConn is a closed PacketConn whose ReadFrom reports a stale source address with its error,
// as the PacketConn contract allows.
type staleSourcePacketConn struct {
	PacketConn
	src net.Addr
}

func (c *staleSourcePacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, c.src, net.ErrClosed
}

const (
	testWrapperFakeAddressPacketConnName    = "FakeAddressPacketConn"
	testWrapperFakeAddressStreamConnName    = "FakeAddressStreamConn"
	testWrapperFakeCloseName                = "FakeClose"
	testWrapperIdleTimeoutName              = "IdleTimeout"
	testWrapperPacketToFromToPacketConnName = "PacketToFromToPacketConn"
	testWrapperPacketToStreamConnName       = "PacketToStreamConn"
	testWrapperStreamToPacketConnName       = "StreamToPacketConn"
)
