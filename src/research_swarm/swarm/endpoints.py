"""Controller destination policy without transport, runtime or provider imports."""

import ipaddress
from urllib.parse import urlsplit


def controller_url(url: str) -> str:
    """Validate a controller destination without opening a client or doing network I/O."""
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or not parsed.hostname
        or (parsed.port is not None and not 1 <= parsed.port <= 65535)
        or any(character.isspace() for character in url)
    ):
        raise ValueError("Controller URL must be HTTP(S), without credentials, query, or fragment")
    if parsed.scheme == "http":
        try:
            loopback = ipaddress.ip_address(parsed.hostname).is_loopback
        except ValueError:
            loopback = parsed.hostname == "localhost"
        if not loopback or "%" in parsed.hostname:
            raise ValueError("Non-loopback controller connections require HTTPS")
    return url.rstrip("/")
