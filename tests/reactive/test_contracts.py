from __future__ import annotations

import pytest
from pydantic import ValidationError

from research_swarm.reactive.contracts import (
    ApplicationReceipt,
    DecisionProposal,
    DecisionRequest,
    DiagnosticDefinition,
    NumericControl,
    NumericValue,
    Observation,
    ReactiveContract,
    RunBinding,
    proposed_controls,
    validate_request,
)

DIGEST = "sha256:" + "a" * 64
IDENTIFIER = "00000000-0000-0000-0000-000000000001"


@pytest.fixture
def contract() -> ReactiveContract:
    return ReactiveContract(
        policy_name="fixture-v1",
        policy_digest=DIGEST,
        controls=(NumericControl(name="lr", minimum=0.0, maximum=1.0, initial=0.1),),
        diagnostics=(DiagnosticDefinition(name="loss", source="train"),),
        interval_steps=5,
        max_lag_steps=10,
    )


@pytest.fixture
def decision_request(contract: ReactiveContract) -> DecisionRequest:
    return DecisionRequest(
        id=IDENTIFIER,
        sequence=1,
        binding=RunBinding(
            trial_id=IDENTIFIER,
            attempt_id=IDENTIFIER,
            stage_id="train",
            spec_digest=DIGEST,
            context_digest=DIGEST,
        ),
        contract_digest=contract.digest,
        policy_digest=contract.policy_digest,
        observation=Observation(step=10, diagnostics=(NumericValue(name="loss", value=2.0),)),
        requested_step=11,
        control_revision=0,
        current_controls=contract.initial_controls(),
    )


def proposal(decision_request: DecisionRequest, **changes: object) -> DecisionProposal:
    return DecisionProposal.model_validate(
        {
            "request_id": decision_request.id,
            "request_digest": decision_request.digest,
            "base_revision": decision_request.control_revision,
            "kind": "set",
            "values": [{"name": "lr", "value": 0.2}],
            **changes,
        }
    )


def test_declared_changes_and_keep_are_pure(
    contract: ReactiveContract, decision_request: DecisionRequest
) -> None:
    validate_request(contract, decision_request)
    assert proposed_controls(contract, decision_request, proposal(decision_request), step=12) == (
        NumericValue(name="lr", value=0.2),
    )
    keep = proposal(decision_request, kind="keep", values=[])
    assert (
        proposed_controls(contract, decision_request, keep, step=12)
        == decision_request.current_controls
    )
    assert decision_request.current_controls[0].value == 0.1
    with pytest.raises(ValidationError, match="frozen"):
        decision_request.current_controls[0].value = 0.9  # type: ignore[misc]


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf"), True, "0.1"])
def test_numeric_controls_reject_nonfinite_and_coerced_values(value: object) -> None:
    with pytest.raises(ValidationError):
        NumericControl.model_validate(
            {"name": "lr", "minimum": 0.0, "maximum": 1.0, "initial": value}
        )


@pytest.mark.parametrize(
    "values", [[{"name": "other", "value": 0.2}], [{"name": "lr", "value": 1.1}]]
)
def test_proposals_cannot_expand_the_control_surface(
    contract: ReactiveContract, decision_request: DecisionRequest, values: list[dict[str, object]]
) -> None:
    with pytest.raises(ValueError, match="unknown control|out-of-bounds"):
        proposed_controls(
            contract, decision_request, proposal(decision_request, values=values), step=12
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"request_id": "00000000-0000-0000-0000-000000000002"},
        {"request_digest": "sha256:" + "b" * 64},
        {"base_revision": 1},
    ],
)
def test_proposal_identity_is_bound_to_the_original_request(
    contract: ReactiveContract, decision_request: DecisionRequest, changes: dict[str, object]
) -> None:
    with pytest.raises(ValueError, match="differs from its request"):
        proposed_controls(
            contract, decision_request, proposal(decision_request, **changes), step=12
        )


@pytest.mark.parametrize("step", [10, 21, True, 12.0, 9_007_199_254_740_992])
def test_application_requires_a_fresh_integer_boundary(
    contract: ReactiveContract, decision_request: DecisionRequest, step: object
) -> None:
    with pytest.raises(ValueError, match="integer step|stale"):
        proposed_controls(contract, decision_request, proposal(decision_request), step=step)  # type: ignore[arg-type]


def test_cross_attempt_context_changes_request_identity(decision_request: DecisionRequest) -> None:
    changed = decision_request.model_copy(
        update={
            "binding": decision_request.binding.model_copy(
                update={"context_digest": "sha256:" + "b" * 64}
            )
        }
    )
    assert changed.digest != decision_request.digest


def test_unknown_diagnostics_are_rejected(
    contract: ReactiveContract, decision_request: DecisionRequest
) -> None:
    changed = decision_request.model_copy(
        update={
            "observation": Observation(
                step=10, diagnostics=(NumericValue(name="test_loss", value=1.0),)
            )
        }
    )
    with pytest.raises(ValueError, match="undeclared diagnostic"):
        validate_request(contract, changed)


def test_histories_and_names_must_be_unambiguous(decision_request: DecisionRequest) -> None:
    with pytest.raises(ValidationError, match="unique"):
        Observation(step=1, diagnostics=(NumericValue(name="loss", value=1.0),) * 2)
    with pytest.raises(ValidationError, match="strictly increasing"):
        DecisionRequest.model_validate(
            decision_request.model_dump() | {"history": (decision_request.observation,)}
        )
    with pytest.raises(ValidationError, match="Keep requires"):
        proposal(decision_request, kind="keep")


def test_response_bytes_are_bounded(
    contract: ReactiveContract, decision_request: DecisionRequest
) -> None:
    small = contract.model_copy(update={"max_response_bytes": 256})
    bound = decision_request.model_copy(update={"contract_digest": small.digest})
    with pytest.raises(ValueError, match="response byte limit"):
        proposed_controls(small, bound, proposal(bound, rationale="x" * 1000), step=12)


def test_request_count_and_context_bytes_are_bounded(
    contract: ReactiveContract, decision_request: DecisionRequest
) -> None:
    with pytest.raises(ValueError, match="decision count"):
        validate_request(
            contract, decision_request.model_copy(update={"sequence": contract.max_decisions + 1})
        )
    small = contract.model_copy(update={"max_prompt_bytes": 1024})
    large = decision_request.model_copy(
        update={
            "contract_digest": small.digest,
            "history": tuple(
                Observation(step=step, diagnostics=(NumericValue(name="loss", value=2.0),))
                for step in range(10)
            ),
        }
    )
    with pytest.raises(ValueError, match="prompt byte limit"):
        validate_request(small, large)


def test_contract_rejects_ambiguous_controls_and_test_observations(
    contract: ReactiveContract,
) -> None:
    with pytest.raises(ValidationError, match="unique names"):
        ReactiveContract.model_validate(contract.model_dump() | {"controls": contract.controls * 2})
    with pytest.raises(ValidationError, match="within its declared bounds"):
        NumericControl(name="lr", minimum=1.0, maximum=0.0, initial=0.5)
    with pytest.raises(ValidationError):
        DiagnosticDefinition.model_validate({"name": "loss", "source": "test"})


def test_application_receipt_cannot_assert_unknown_state(decision_request: DecisionRequest) -> None:
    fields = {
        "binding": decision_request.binding,
        "request_id": decision_request.id,
        "request_digest": decision_request.digest,
        "decision_digest": proposal(decision_request).digest,
        "observed_step": 10,
        "requested_step": 11,
        "applied_step": 12,
        "base_revision": 0,
        "new_revision": None,
        "outcome": "uncertain",
        "before": decision_request.current_controls,
        "after": None,
    }
    receipt = ApplicationReceipt.model_validate(fields)
    assert receipt.after is None and receipt.new_revision is None
    with pytest.raises(ValidationError, match="cannot assert"):
        ApplicationReceipt.model_validate(fields | {"after": decision_request.current_controls})
    with pytest.raises(ValidationError, match="exactly one revision"):
        ApplicationReceipt.model_validate(
            fields
            | {"outcome": "applied", "new_revision": 2, "after": decision_request.current_controls}
        )
    with pytest.raises(ValidationError, match="cannot change"):
        ApplicationReceipt.model_validate(
            fields
            | {
                "outcome": "kept",
                "new_revision": 1,
                "after": (NumericValue(name="lr", value=0.2),),
            }
        )


def test_contract_and_request_have_distinct_stable_domains(
    contract: ReactiveContract, decision_request: DecisionRequest
) -> None:
    assert (
        contract.digest == "sha256:18c8e0ca7a2f85dd07a85e52feebe0d049daecbf256e2eec1f05caab0a3df2af"
    )
    assert (
        decision_request.digest
        == "sha256:857baf95ef366e1cd131641eb423af84d0498978f96ebb678335d4c4d3bec19b"
    )
    assert (
        DecisionRequest.model_validate_json(decision_request.model_dump_json()).digest
        == decision_request.digest
    )
    assert contract.digest != decision_request.digest
