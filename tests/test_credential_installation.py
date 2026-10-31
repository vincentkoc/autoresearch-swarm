"""Credential installation composes with an owner's atomic provisioning transaction."""

import pytest

from research_swarm.control.auth import CredentialStore
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id


def test_initial_schema_and_credential_roster_roll_back_together(database, principal):
    identifiers = (new_id(), new_id())
    tokens = ("rsw_" + "A" * 43, "rsw_" + "B" * 43)

    def install(sql):
        CredentialStore.initialize_in(sql)
        for identifier, token in zip(identifiers, tokens, strict=True):
            CredentialStore.install_in(
                sql, principal, principal, identifier=identifier, token=token
            )
        raise RuntimeError("Synthetic owner transaction interruption")

    with pytest.raises(RuntimeError, match="owner transaction"):
        database.store.write(install)
    assert (
        database.store.read(
            lambda sql: sql.execute(
                "SELECT name FROM sqlite_master WHERE name='credentials'"
            ).fetchone()
        )
        is None
    )
    credentials = CredentialStore(database)
    first = credentials.install(principal, principal, identifier=identifiers[0], token=tokens[0])
    before = database.store.read(lambda sql: dict(sql.execute("SELECT * FROM credentials").one()))
    assert (
        database.store.write(
            lambda sql: CredentialStore.install_in(
                sql, principal, principal, identifier=identifiers[0], token=tokens[0]
            )
        )
        == first
    )
    assert (
        database.store.read(lambda sql: dict(sql.execute("SELECT * FROM credentials").one()))
        == before
    )


@pytest.mark.parametrize("refusal", ["revoked", "conflict", "scope"])
def test_failed_roster_install_keeps_existing_credentials_and_rolls_back_new_members(
    database, principal, refusal
):
    credentials = CredentialStore(database)
    existing = credentials.issue(principal, principal)
    if refusal == "revoked":
        credentials.revoke(principal, existing.id)
    before = database.store.read(lambda sql: dict(sql.execute("SELECT * FROM credentials").one()))

    def install(sql):
        CredentialStore.install_in(
            sql, principal, principal, identifier=new_id(), token="rsw_" + "N" * 43
        )
        issuer = (
            principal.model_copy(update={"scopes": {"read"}}) if refusal == "scope" else principal
        )
        return CredentialStore.install_in(
            sql,
            issuer,
            principal,
            identifier=existing.id,
            token="rsw_" + "C" * 43 if refusal == "conflict" else existing.token,
        )

    with pytest.raises(ResearchError) as refused:
        database.store.write(install)
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert (
        database.store.read(lambda sql: dict(sql.execute("SELECT * FROM credentials").one()))
        == before
    )
