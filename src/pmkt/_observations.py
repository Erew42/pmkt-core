"""Conservative source classification and request-observation sanitation."""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
import json
import math
from urllib.parse import SplitResult, urlsplit

from pmkt.records import DataScope, TransportOrigin


_QUALIFIED_ENDPOINTS: dict[tuple[str, str, str], DataScope] = {
    ("gamma", "gamma-api.polymarket.com", ""): "production",
    ("clob", "clob.polymarket.com", ""): "production",
    ("data", "data-api.polymarket.com", ""): "production",
    ("kalshi", "external-api.kalshi.com", "/trade-api/v2"): "production",
    ("kalshi", "external-api.demo.kalshi.co", "/trade-api/v2"): "demo",
}
_SENSITIVE_PARAMETER_FRAGMENTS = (
    "access_token",
    "api-key",
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "password",
    "passphrase",
    "private",
    "secret",
    "signature",
)


@dataclass(frozen=True)
class RequestSource:
    venue: str
    service: str
    data_scope: DataScope
    transport_origin: TransportOrigin
    origin: str


def classify_request_source(
    base_url: str,
    *,
    venue: str,
    service: str,
    transport_supplied: bool,
) -> RequestSource:
    parsed = urlsplit(base_url)
    port = _parsed_port(parsed)
    origin = _sanitized_origin(parsed.scheme, parsed.hostname, port)
    transport_origin: TransportOrigin = (
        "caller_supplied" if transport_supplied else "library_default"
    )
    path = parsed.path.rstrip("/")
    qualified = (
        not transport_supplied
        and _secure_default_origin(parsed, port)
        and parsed.query == ""
    )
    scope = (
        _QUALIFIED_ENDPOINTS.get((service, (parsed.hostname or "").lower(), path), "unknown")
        if qualified
        else "unknown"
    )
    return RequestSource(
        venue=venue,
        service=service,
        data_scope=scope,
        transport_origin=transport_origin,
        origin=origin,
    )


def source_after_response(source: RequestSource, response_url: str) -> RequestSource:
    parsed = urlsplit(response_url)
    port = _parsed_port(parsed)
    response_origin = _sanitized_origin(parsed.scheme, parsed.hostname, port)
    if (
        source.data_scope != "unknown"
        and source.transport_origin == "library_default"
        and _secure_default_origin(
        parsed, port
        )
    ):
        path = parsed.path.rstrip("/")
        if source.service in {"gamma", "clob", "data"}:
            service_path = ""
        elif source.service == "kalshi" and (
            path == "/trade-api/v2" or path.startswith("/trade-api/v2/")
        ):
            service_path = "/trade-api/v2"
        else:
            service_path = path
        scope = _QUALIFIED_ENDPOINTS.get(
            (source.service, (parsed.hostname or "").lower(), service_path),
            "unknown",
        )
    else:
        scope = "unknown"
    return RequestSource(
        venue=source.venue,
        service=source.service,
        data_scope=scope,
        transport_origin=source.transport_origin,
        origin=response_origin,
    )


def sanitize_endpoint_template(endpoint_template: str) -> str:
    if not isinstance(endpoint_template, str):
        raise TypeError("endpoint_template must be a string")
    parsed = urlsplit(endpoint_template)
    if (
        parsed.scheme
        or parsed.netloc
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith("/")
    ):
        raise ValueError("endpoint_template must be an explicit relative path template")
    return parsed.path


def sanitize_effective_parameters(
    parameters: Mapping[str, object] | None,
    *,
    allowlist: Collection[str],
) -> tuple[tuple[str, str], ...]:
    allowed = frozenset(allowlist)
    sanitized: list[tuple[str, str]] = []
    for raw_name, raw_value in (parameters or {}).items():
        name = str(raw_name).strip()
        lowered = name.lower()
        if not name:
            raise ValueError("effective parameter names must not be empty")
        if name not in allowed:
            raise ValueError(f"effective parameter is not allowlisted: {name}")
        if any(fragment in lowered for fragment in _SENSITIVE_PARAMETER_FRAGMENTS):
            raise ValueError(f"sensitive effective parameter is not observable: {name}")
        sanitized.append((name, _parameter_text(raw_value)))
    return tuple(sorted(sanitized))


def _parameter_text(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("effective parameter floats must be finite")
        return str(value)
    if isinstance(value, (str, int)):
        return str(value)
    if isinstance(value, (list, tuple)) and all(
        isinstance(item, (str, int, float, bool)) for item in value
    ):
        for item in value:
            if isinstance(item, float) and not math.isfinite(item):
                raise ValueError("effective parameter floats must be finite")
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"))
    raise TypeError("effective parameter values must be scalar or scalar sequences")


def _secure_default_origin(parsed: SplitResult, port: int | None) -> bool:
    return (
        parsed.scheme.lower() == "https"
        and parsed.username is None
        and parsed.password is None
        and parsed.fragment == ""
        and port in (None, 443)
        and port != -1
    )


def _parsed_port(parsed: SplitResult) -> int | None:
    try:
        return parsed.port
    except ValueError:
        return -1


def _sanitized_origin(scheme: str, hostname: str | None, port: int | None) -> str:
    normalized_scheme = scheme.lower() or "unknown"
    normalized_host = (hostname or "unknown").lower()
    if ":" in normalized_host and not normalized_host.startswith("["):
        normalized_host = f"[{normalized_host}]"
    default_port = (normalized_scheme == "https" and port == 443) or (
        normalized_scheme == "http" and port == 80
    )
    suffix = "" if port in (None, -1) or default_port else f":{port}"
    return f"{normalized_scheme}://{normalized_host}{suffix}"


__all__ = [
    "RequestSource",
    "classify_request_source",
    "sanitize_effective_parameters",
    "sanitize_endpoint_template",
    "source_after_response",
]
