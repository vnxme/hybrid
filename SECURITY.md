# Security policy

## Supported versions

Until v1.0.0, only the latest minor version receives security fixes, as a patch release.

## Reporting a vulnerability

Please report a vulnerability privately, via
[GitHub private vulnerability reporting](https://github.com/vnxme/hybrid/security/advisories/new),
rather than in a public issue. Include the affected version, the platform, and a way to reproduce it,
e.g. a test or a sequence of packets.

Reports are acknowledged as soon as possible, and fixed in a patch release along with a GitHub security
advisory, crediting the reporter unless asked otherwise.

## Scope

As the module handles packets from untrusted remote hosts, these are notable examples of vulnerabilities:

- a panic, a deadlock, or unbounded memory or goroutine growth caused by received packets or control messages,
  e.g. the flow table growing past `MaxFlows`;
- a packet delivered to the wrong flow, or a reply sent from the wrong address or through the wrong interface;
- data of one packet or flow exposed to another, e.g. through a pooled buffer.

Vulnerabilities in Go or in the `golang.org/x` modules should be reported to the
[Go security team](https://go.dev/doc/security/policy) instead.
