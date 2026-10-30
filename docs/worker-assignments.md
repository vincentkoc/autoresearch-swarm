# Signed worker assignments

The controller signs the complete execution assignment. A worker receives a pinned
public key scoped to its workspace, controller, epoch, worker, and session. The
worker receives no private signing key.

`AssignmentPayload` includes the sealed experiment, dispatch and reservation
identities, source archive reference and tree digest, exact named inputs, admitted
engine profile, evaluator request, network policy, offline policy, and issue time.
The signature covers all of these fields. A valid signature for a different worker
session does not grant execution authority.

`AssignmentSigner` uses Ed25519 through the
[Cryptography Ed25519 API](https://cryptography.io/en/latest/hazmat/primitives/asymmetric/ed25519/).
Its input is RFC 8785 canonical JSON with the `research.worker-assignment` domain,
algorithm `ed25519-jcs-v1`, pinned key ID, and payload. The key ID is a separate
domain digest of the public key and algorithm. Public keys use 32-byte lowercase
hex; signatures use 64-byte lowercase hex. `tests/test_assignment_signatures.py`
provides deterministic synthetic known-answer fixtures.

Verification checks the pinned key, signature, enrollment, sealed source/spec
consistency, exact stage inputs, capabilities, issue time, and launch deadline.
The caller must supply qualified controller time. A caller cannot use this
primitive to establish clock agreement with a remote controller.

`allow_expired_observation` permits reconstruction of an expired authenticated
assignment. It grants no new launch authority. The journal and native release gate
must still enforce the original deadlines and prevent repeat execution.

Local runtimes persist a separate private `assignment.key` and pin its public key
ID in `identity.json`. Restart does not replace a missing or changed key. An older
runtime without signed enrollments creates this key once. Existing HMAC
enrollments keep their original authority; new enrollments use signatures.

Admission signs and stores the assignment in the same transaction as its attempt,
budget reservation, and dispatch event. A changed or absent signing key rolls back
admission. Command replay returns the original signature and grant.

The native worker verifies the signature and all presented assignment fields
before journal admission. The journal persists the full signed assignment and
includes it in duplicate-delivery identity. Preparation must match its input
digests and execution profile. Restart checks the same assignment again; result
publication must preserve it. A signed enrollment cannot fall back to HMAC.

Local execution uses the controller host's clock. Restricted remote enrollment,
polling, clock qualification, and remote execution remain pending. Signatures
alone do not qualify a remote worker or prove native process closure.
