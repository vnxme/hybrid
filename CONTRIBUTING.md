# Contributing

Thank you for helping to improve hybrid. Bug reports, fixes, tests and documentation are all welcome.

## Before you start

- Report a vulnerability privately, as [the security policy](SECURITY.md) describes, rather than in an issue.
- Report a bug or propose a feature in [an issue](https://github.com/vnxme/hybrid/issues/new/choose) first,
  unless the change is small, e.g. a typo, so that the approach is agreed on before you spend time on it.
- Keep the API minimal: until v1.0.0 it may change, but every exported name has to earn its place.

## Development

Use the Go version of `go.mod` or later, and run what CI runs before you push:

```sh
go test -race -count=1 ./...  # On Windows, without -race, unless cgo is set up
golangci-lint run ./...
GOOS=freebsd go vet ./...     # Or any other platform with code of its own that you've changed
```

The README describes [the tests](README.md#testing), including the benchmarks and fuzz tests, and
[the platforms](README.md#platform-support) with code of their own. A few things to keep in mind:

- Test every change, with its edge cases: empty and maximum payloads, IPv4, IPv6 and dual-stack sockets,
  closing and deadlines while reading or writing, and anything that may race.
- Tests run in parallel on busy CI machines, so wait for conditions rather than for fixed times,
  and leave generous margins wherever timing matters.
- Use modern Go features where they fit, and no dependencies beyond the standard library and `golang.org/x`.
- Write comments that explain why, not what, in full sentences.

## Commits and pull requests

- Make each commit one logical change, with a subject in the form `area: lowercase description`,
  where the area is a package, a file or a topic, e.g. `split`, `pktinfo`, `trunk`, `test`, `ci` or `readme`.
- Update the README along with any change of behavior or API.
- Make sure CI passes. It lists any API change since the latest release, which a pull request should explain.

By contributing, you agree that your contributions are licensed under [the Apache License 2.0](LICENSE),
as the project is.
