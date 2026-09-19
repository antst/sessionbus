# Releasing Sessionbus

Preparing a release branch is separate from publishing it. Obtain the owner's
explicit release authorization before creating or pushing a release tag,
publishing to npm, or changing a registry dist-tag.

Starting with v0.5.5, one stable root tag `vX.Y.Z` identifies the source for:

- Sessionbus host and hub binary archives and their `SOURCE.txt`.
- `@sessionbus/kit@X.Y.Z`, with npm provenance.
- Go module `github.com/antst/sessionbus/bus/sdk/go` at `vX.Y.Z`, published by
  creating `bus/sdk/go/vX.Y.Z` at the same source commit.

The peers repository keeps its own release version. Existing `kit-v*` and Go
module tags are historical releases; do not move or delete them. New `kit-v*`
tags do not trigger npm publication.

Before tagging, set `bus/package.json` to the intended version, run the Go and
JavaScript tests and DTO checks, review the release diff, and require green CI
at the approved source commit. Both publication workflows reject a root tag
that does not match the package version. The Go tag step refuses an existing
module tag that resolves to different source. The release-check helper uses
Python 3 from the CI runner; it adds no dependency to installed binaries or SDKs.

After authorization, push the stable root tag. The binary release workflow
publishes tested archives, then creates the matching Go module tag. The npm
workflow independently tests and publishes the kit. These workflows are not an
atomic transaction: do not announce completion until all three surfaces are
verified. If a step fails, preserve the failure and finish only the missing
publication from the same approved source; never retag a published version.

Verify the binary `SOURCE.txt`, npm version and provenance, and the peeled Go
module tag against the approved commit. Test an ordinary Go consumer through
the public module proxy and an ordinary npm consumer of the published kit.
Removing the kit's previous `publishConfig.tag: pre` makes npm's default
publication tag `latest`; verify the resulting dist-tag. Any manual
`npm dist-tag add @sessionbus/kit@0.5.5 latest` repair remains a separately
authorized owner action, not an automatic step in this runbook.
