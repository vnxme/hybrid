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

// Package pktinfo enables, marshals and parses packet info control messages, which carry
// the local address and the local interface index of each UDP packet read or written.
//
// [Enable] turns packet info on for the IP versions a socket accepts. A read with an OOB buffer
// of [BufferSize] bytes then gets packet info, which [Parse] finds, and a write gets packet info
// that [Marshal] puts into such a buffer. Where packet info isn't supported, [Enable] fails
// with [errors.ErrUnsupported].
//
// IPv6 packet info is the same everywhere: IPV6_RECVPKTINFO turns on IPV6_PKTINFO messages
// with in6_pktinfo, which ipv6.go marshals and parses, and zone.go resolves IPv6 zones,
// i.e. the names of the interfaces. IPv4 packet info differs:
//
//	Platforms                            Turned on with                    Sent from                    Files
//	linux                                IP_PKTINFO                        ipi_spec_dst, with an index  linux.go, ipv4_pktinfo.go
//	darwin, illumos, solaris             IP_RECVPKTINFO                    ipi_spec_dst, with an index  darwin.go, ipv4_pktinfo.go
//	zos                                  IP_RECVPKTINFO                    ipi_addr, with an index      zos.go, ipv4_pktinfo.go
//	windows                              IP_PKTINFO                        ipi_addr, with an index (*)  windows.go, ipv4_pktinfo.go
//	dragonfly, freebsd, netbsd, openbsd  IP_RECVDSTADDR, IP_RECVIF         IP_SENDSRCADDR, no index     bsd.go, ipv4_dstaddr.go
//	aix                                  IP_RECVDSTADDR, IP_RECVINTERFACE  IP_SENDSRCADDR, no index     aix.go, ipv4_dstaddr.go
//	others, e.g. js, plan9, wasip1       unsupported                                                    other.go
//
// (*) Windows sends packet info without a source address from 0.0.0.0 or ::, see [ChoosesSource].
//
// Received IPv4 packet info carries the destination address in ipi_addr, or in IP_RECVDSTADDR
// with the index in a separate link-level address. All platforms but windows and the others
// share posix.go for socket options, and all but the others share cmsg.go, which walks
// control messages and marshals either IP version.
package pktinfo
