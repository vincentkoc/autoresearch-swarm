"""Single-process native witness service, without controller state or provisioning."""

from __future__ import annotations

import ssl
from collections.abc import Callable
from ipaddress import ip_address
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import uvicorn
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from pydantic import Field, model_validator

from research_swarm.api.time_witnesses import create_time_witness_app
from research_swarm.control.native_time_witnesses import NativeWitnessSource
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import WitnessAuthorityScope, WitnessSource
from research_swarm.domain.types import Digest, VersionedRecord
from research_swarm.state.private_files import private_state_directory, read_private, write_private

MAX_CONFIGURATION_BYTES = 16 * 1024


def fixed_tls_context(certificate_file: Path, key_file: Path) -> ssl.SSLContext:
    """Snapshot admitted bytes once; never give the server the mutable source paths."""
    key_path = key_file.expanduser().absolute()
    try:
        key = read_private(key_path.parent, key_path.name, limit=64 * 1024)
        with certificate_file.expanduser().open("rb") as stream:
            certificate = stream.read(64 * 1024 + 1)
        if len(certificate) > 64 * 1024:
            raise ValueError("Oversized TLS certificate")
        # Refuse encrypted keys without ever invoking OpenSSL's interactive prompt.
        load_pem_private_key(key, password=None)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        # Python's SSL API accepts file paths for certificate chains. These are
        # fresh private copies of the admitted bytes, removed before listening.
        with TemporaryDirectory(prefix="research-witness-tls-") as directory:
            root = Path(directory) / "inputs"
            with private_state_directory(root, exclusive=True):
                write_private(root / "certificate.pem", certificate)
                write_private(root / "key.pem", key)
                context.load_cert_chain(
                    root / "certificate.pem", root / "key.pem", password=lambda: ""
                )
        return context
    except ResearchError:
        raise
    except (ValueError, TypeError, OSError, UnsupportedAlgorithm):
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid native witness TLS input"
        ) from None


class WitnessServerConfiguration(VersionedRecord):
    authority: WitnessAuthorityScope
    source: WitnessSource
    consumer_digest: Digest
    host: str = "127.0.0.1"
    port: int = Field(default=8788, strict=True, ge=1, le=65535)
    transport: Literal["loopback-http", "tls", "private-http"] = "loopback-http"
    body_timeout_seconds: float = Field(default=1.0, strict=True, gt=0, le=10)

    @model_validator(mode="after")
    def listener(self) -> WitnessServerConfiguration:
        address = ip_address(self.host)
        if self.transport == "loopback-http" and not address.is_loopback:
            raise ValueError("Loopback transport requires a loopback listener")
        return self


def load_configuration(path: Path) -> WitnessServerConfiguration:
    try:
        with path.expanduser().open("rb") as stream:
            content = stream.read(MAX_CONFIGURATION_BYTES + 1)
        if len(content) > MAX_CONFIGURATION_BYTES:
            raise ValueError("Oversized witness configuration")
        return WitnessServerConfiguration.model_validate(strict_json_loads(content))
    except (ValueError, OSError, RecursionError):
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid native witness configuration"
        ) from None


def configured_source(
    configuration: WitnessServerConfiguration, signing_key_file: Path
) -> NativeWitnessSource:
    path = signing_key_file.expanduser().absolute()
    try:
        seed = read_private(path.parent, path.name, limit=32)
        if len(seed) != 32:
            raise ValueError("Invalid signing key length")
        source = NativeWitnessSource(
            configuration.authority,
            configuration.source.id,
            seed,
            policy=configuration.source.policy,
        )
    except ResearchError:
        raise
    except (ValueError, OSError):
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid native witness signing input"
        ) from None
    if source.source != configuration.source:
        raise ResearchError(
            ErrorCode.STALE_FENCE, "Native witness source differs from configuration"
        )
    return source


def serve_witness(
    config_file: Path,
    signing_key_file: Path,
    *,
    tls_certificate_file: Path | None = None,
    tls_key_file: Path | None = None,
) -> None:
    configuration = load_configuration(config_file)
    tls = configuration.transport == "tls"
    if (tls_certificate_file is not None, tls_key_file is not None) != (tls, tls):
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "TLS files must match the witness transport"
        )
    context = (
        fixed_tls_context(tls_certificate_file, tls_key_file)
        if tls_certificate_file is not None and tls_key_file is not None
        else None
    )
    source = configured_source(configuration, signing_key_file)
    app = create_time_witness_app(
        source,
        configuration.consumer_digest,
        body_timeout_seconds=configuration.body_timeout_seconds,
    )

    def tls_factory(
        config: uvicorn.Config, default: Callable[[], ssl.SSLContext]
    ) -> ssl.SSLContext:
        assert context is not None
        return context

    # One source is created in this serving process. No worker fork, reload,
    # request-time source replacement, controller initialization or requalification.
    uvicorn.run(
        app,
        host=configuration.host,
        port=configuration.port,
        workers=1,
        reload=False,
        proxy_headers=False,
        access_log=False,
        log_level="warning",
        lifespan="off",
        ssl_context_factory=tls_factory if context is not None else None,
    )
