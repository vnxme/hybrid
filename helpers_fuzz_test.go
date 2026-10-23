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
	"net"
	"net/netip"
	"reflect"
	"testing"
)

// FuzzAddr ensures that for any IP, port and zone, addrKeyOf gives a UDP, TCP and IP address the same key,
// unmapped and valid only for an IP of 4 or 16 bytes, that addrPortToUDPAddr turns the key back into
// an address with this key, as net.UDPAddrFromAddrPort does, and that cloneAddr copies each address
// in full without sharing its IP.
func FuzzAddr(f *testing.F) {
	f.Add([]byte(net.IPv4(192, 0, 2, 1).To4()), uint16(53), "")
	f.Add([]byte(net.IPv4(192, 0, 2, 1)), uint16(53), "eth0") // IPv4-mapped
	f.Add([]byte(net.ParseIP("2001:db8::1")), uint16(443), "")
	f.Add([]byte(net.ParseIP("fe80::1")), uint16(0), "eth0")
	f.Add([]byte{}, uint16(1), "")
	f.Add([]byte{1, 2, 3, 4, 5}, uint16(65535), "x")
	f.Fuzz(func(t *testing.T, ip []byte, port uint16, zone string) {
		if len(ip) == 0 {
			ip = nil // As a UDPAddr without an IP has it, which cloneAddr keeps nil
		}
		udp := &net.UDPAddr{IP: ip, Port: int(port), Zone: zone}
		tcp := &net.TCPAddr{IP: ip, Port: int(port), Zone: zone}
		ipa := &net.IPAddr{IP: ip, Zone: zone}

		k := addrKeyOf(udp)
		if valid := len(ip) == net.IPv4len || len(ip) == net.IPv6len; k.valid() != valid || k.str != "" {
			t.Fatalf("addrKeyOf(%v) = %+v, want valid %t, with no string", udp, k, valid)
		}
		if k.ap.Addr().Is4In6() {
			t.Errorf("addrKeyOf(%v) = %+v, want unmapped", udp, k)
		}
		if got := addrKeyOf(tcp); got != k {
			t.Errorf("addrKeyOf(%v) = %+v, want %+v as of a UDP address", tcp, got, k)
		}
		if got, want := addrKeyOf(ipa), (addrKey{ap: netip.AddrPortFrom(k.ap.Addr(), 0)}); k.valid() && got != want {
			t.Errorf("addrKeyOf(%v) = %+v, want %+v", ipa, got, want)
		}

		if k.valid() {
			got, want := addrPortToUDPAddr(k.ap), net.UDPAddrFromAddrPort(k.ap)
			if !reflect.DeepEqual(got, want) || addrKeyOf(got) != k {
				t.Errorf("addrPortToUDPAddr(%v) = %#v, want %#v, with key %+v", k.ap, got, want, k)
			}
		}

		for _, a := range []net.Addr{udp, tcp, ipa} {
			c := cloneAddr(a)
			if !reflect.DeepEqual(c, a) {
				t.Errorf("cloneAddr(%#v) = %#v, want equal", a, c)
			}
			if cip := reflect.ValueOf(c).Elem().FieldByName("IP").Bytes(); len(cip) > 0 {
				cip[0]++ // Visible in a only if they share the IP
				if reflect.DeepEqual(c, a) {
					t.Errorf("cloneAddr(%#v) shares the IP", a)
				}
			}
		}
	})
}
