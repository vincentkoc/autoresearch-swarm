# Cross-language identity conformance

The protocol has a Python implementation, but its persisted identities are not
Python object hashes. A second implementation must produce the same UTF-8 bytes
and SHA-256 digests before it can participate in stored state or signed dispatch.

## Frozen corpus

`tests/conformance/canonical-v1.json` supplies 17 synthetic golden cases. Each has
a domain, original JSON text, canonical UTF-8 text, content digest, and structured
record digest. The corpus preserves source text so a consumer can distinguish
the original numeric token from the canonical output.

Cases cover:

- UTF-16 key ordering, supplementary Unicode, and integer-looking object keys.
- Composed and decomposed text without Unicode normalization.
- Negative zero, decimal/exponent boundaries, subnormal and maximum finite floats.
- Safe-range integers and exact decimal strings for larger clocks and budget values.
- Control characters, quoting, nested ordered arrays, fixed-precision timestamps,
  and literal keys such as `__proto__` and `toJSON`.
- Absent fields, explicit null, and different digest domains.

Canonical encoding follows [RFC 8785](https://www.rfc-editor.org/rfc/rfc8785.html).
The version-1 structured preimage is an object with `domain`,
`hash_format_version="1"`, and `payload`. A stored digest is outside its own
preimage. Ordered arrays remain ordered. Schema-defined large counters and precise
budget quantities remain decimal strings; never coerce them through JavaScript
numbers.

The Python tests use the production serializer. The independent ECMAScript
fixture program uses native number/string encoding, explicit UTF-16 key ordering,
and Node's SHA-256 implementation. It does not import Python or a generated Python
result at execution time. Both must match the checked-in golden bytes and hashes.
Invalid Unicode and non-finite values are independently rejected.

## Signature interoperability

The same independent process verifies a synthetic Python Ed25519 assignment
signature using only its public key. It checks the signing-preimage digest against
the existing fixed golden value and rejects a changed experiment seed. Its
signing preimage includes the assignment domain, algorithm, key ID, and complete
payload; this is distinct from the general structured-record hash envelope.

This proves cryptographic byte interoperability. It does not implement worker
admission, validate an execution lease, or authorize a process launch. A future
worker must still validate the complete schema, enrollment binding, signatures,
resource reservation, clock authority, and native custody rules.

## Running the proof

Install Node.js 22 or later for this development check. The research runtime has
no Node dependency and the verifier uses no npm packages.

```sh
RESEARCH_REQUIRE_NODE=1 uv run pytest -q tests/test_cross_language.py
```

The ordinary Linux, macOS, and Windows CI matrix installs Node 22 and requires this
proof. Local Python-only development skips the JavaScript tests when Node is
absent; that skip does not count as cross-language qualification.

The JavaScript helper accepts only trusted fixture input. It is not a production
strict JSON parser: duplicate-key detection, safe integer-token admission, and
domain model validation remain separate responsibilities of any real adapter.
The production Python parser already applies those input restrictions. Matching
this corpus is a prerequisite for another implementation, not its full conformance
certificate.

Do not regenerate golden output to hide a mismatch. A deliberate identity-format
change requires a new version and explicit migration/replay analysis.
