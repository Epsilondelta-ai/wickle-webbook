# Release archives

Wickle is distributed under MIT through GitHub Releases. The source archive
contains the workspace, lockfile, library sources, documentation and consumer
examples. It is not a server executable and does not include credentials.

## Download and verify

Download `wickle-0.2.0.tar.gz`, `release-manifest.json` and `SHA256SUMS` from the
[v0.2.0 release](https://github.com/Epsilondelta-ai/wickle/releases/tag/v0.2.0).
In the download directory, verify the files before extraction:

```sh
shasum -a 256 -c SHA256SUMS
tar -xzf wickle-0.2.0.tar.gz
cd wickle-0.2.0
python3 scripts/check-package.py --consumer agent
```

On systems with GNU coreutils, `sha256sum -c SHA256SUMS` performs the same check.
The manifest records the source commit/tree, version, license, minimum Rust
version and archive digest. Compare the commit with the release tag when checking
provenance. A matching checksum checks file integrity; obtain these files from
the intended repository's release.

For application dependencies, the [Git-tag installation](installation.md) keeps
core and adapter versions aligned. The extracted source can also be used with
local Cargo path dependencies. Read the [migration guide](migration-v0.2.md)
before attaching a newer runtime to existing persistent data.

## Reproduce the source archive

Maintainers build from the clean, committed tree that passed review and CI.
Use the same source commit when reproducing an archive; `git archive` excludes
untracked files. The gzip timestamp is omitted for reproducibility.

```sh
release_dir=$(mktemp -d)
git archive --format=tar --prefix=wickle-0.2.0/ \
  --output="$release_dir/wickle-0.2.0.tar" HEAD
gzip -n "$release_dir/wickle-0.2.0.tar"
```

Write a manifest for that exact commit and archive, then calculate SHA-256 for
both files into `SHA256SUMS`. Validate an extracted archive and an independent
application before tagging. Publish the tag at the reviewed commit, upload the
verified files, download them again, and repeat checksum and consumer validation.
The complete pre-release package gate is `python3 scripts/check-package.py`
without a consumer selector; Linux/macOS and the declared minimum Rust version
remain part of the CI gate.

Do not change the contents of an existing release tag to deliver a code fix.
Publish a new version with its own artifacts and compatibility notes.
