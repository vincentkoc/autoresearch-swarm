# Releasing

The release workflow builds one source distribution and a wheel from that source
distribution. It verifies their contents, records SHA-256 checksums, and installs
the same wheel on Linux, macOS and Windows. The Linux matrix also tests Python
3.11, the minimum supported version.

The installed package must run a native CPU experiment, expose its evidence over
MCP, complete the campaign, and resume without another execution. Optional Torch,
MLX and Temporal dependencies must remain absent from a core installation.

## Preview

Run the **Release** workflow manually at the desired revision. Manual dispatch
always produces a preview, including when the selected ref is a tag. It cannot
enter the publication job.

Download the `distributions-<commit>` artifact to inspect:

- one `autoresearch_swarm-<version>-py3-none-any.whl`;
- one `autoresearch_swarm-<version>.tar.gz`;
- `manifest.json`, containing the source commit, names, sizes and hashes;
- `SHA256SUMS`.

Local equivalents:

~~~sh
uv sync --locked --group dev --group release
uv run --no-sync python -m build --no-isolation
uv run --no-sync python -m twine check --strict dist/*.whl dist/*.tar.gz
uv run --no-sync python scripts/release.py dist --write
~~~

Start with an empty task-owned `dist` directory. Unexpected distributions are
rejected. Commit source changes before writing the manifest; the checker compares
package contents with committed Git bytes. The release dependency group and build backend are pinned; CI also
pins uv. Dependency installation uses the lock, which does not prove compatibility
with every version allowed by the package's dependency ranges.

For an existing wheel, set `RESEARCH_PACKAGE_TESTS=1`,
`RESEARCH_PACKAGE_WHEEL` to its absolute path, and
`RESEARCH_PACKAGE_WHEEL_SHA256` to its lowercase SHA-256. Then run
`tests/integration/test_packaged_release.py`. Invalid supplied artifacts fail;
the test does not replace them with a fresh build.

## One-time publisher setup

Create a GitHub environment named `pypi`. Restrict it to release tags and apply
the repository's maintainer approval policy. Configure this PyPI Trusted Publisher:

| Field | Value |
| --- | --- |
| PyPI project | `autoresearch-swarm` |
| GitHub owner | `vincentkoc` |
| Repository | `autoresearch-swarm` |
| Workflow | `release.yml` |
| Environment | `pypi` |

Use a pending publisher if this is the first upload. This account configuration
must exist before publication; a passing preview does not establish it. See
[PyPI's setup instructions](https://docs.pypi.org/trusted-publishers/adding-a-publisher/)
and [first-project publishing](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/).
No long-lived PyPI token is needed. Only the publication job receives OIDC
permission, and it neither checks out source nor rebuilds distributions.

Enable immutable GitHub releases if available for the repository. The workflow
creates a draft, attaches the qualified distributions and checksums, then
publishes it. The separate release-drafter workflow maintains a `next` draft for
editorial notes; it does not choose the package version or publish a release.

## Publish a version

1. Update the version in `pyproject.toml` and
   `src/research_swarm/__init__.py`, then run `uv lock`.
2. Add a `## <version> - YYYY-MM-DD` section to `CHANGELOG.md`. Keep a new
   Unreleased section for subsequent changes.
3. Commit and push to `main`. Require successful CI for that exact commit and
   a successful manual release preview.
4. Create and push a signed `v<version>` tag at that commit. Use canonical
   PEP 440 spelling. Development versions are preview-only.
5. The tag workflow checks version, lock, changelog and source CI, builds and
   tests the distributions, then uses the protected `pypi` environment to publish
   them with [PyPI attestations](https://docs.pypi.org/attestations/producing-attestations/).
   It then attaches those same files to the GitHub release.

Publication is enabled only for tag push events. Do not move or recreate a
published tag. Do not rebuild a different artifact under an existing version.

## Recover a partial release

If PyPI rejects the publisher identity before upload, correct the account
configuration and rerun the failed job using the existing qualified artifact.
Artifacts are retained for 14 days. An expired artifact requires a new preview
and an explicit recovery decision.

If PyPI accepted the distributions but GitHub release creation failed, finish
only the GitHub release job. An existing draft or asset requires inspection:
compare it with the manifest and checksums before completing it. The automation
does not overwrite assets or skip existing PyPI files to hide partial success.

Hardware and hosted-service qualification are separate from package release.
Keep the limitations in [implementation status](implementation.md) visible.
