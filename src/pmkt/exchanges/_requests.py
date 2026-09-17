"""Venue request observations, composed around the HTTP transport."""

from __future__ import annotations

import asyncio
from datetime import datetime
from json import JSONDecodeError
from typing import Any, Callable, Mapping, Sequence

import httpx

from pmkt._http import HttpClient, _RequestTrace
from pmkt._observations import (
    classify_request_source,
    sanitize_effective_parameters,
    sanitize_endpoint_template,
    source_after_response,
)
from pmkt.runtime import OperationExpiry
from pmkt.errors import InvalidDataError, OperationTimeoutError
from pmkt.records import RequestObservation, RequestOutcome


class VenueRequests:
    """Attach venue meaning to transport facts without owning its lifetime."""

    def __init__(self, http: HttpClient, *, venue: str, service: str) -> None:
        self.http = http
        self.source = classify_request_source(
            http.base_url,
            venue=venue,
            service=service,
            transport_supplied=http._transport is not None,
        )

    async def request_json_observed(
        self,
        method: str,
        path: str,
        *,
        request_id: str,
        endpoint_template: str,
        effective_parameters: Mapping[str, object] | None = None,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
        expiry: OperationExpiry | None = None,
        response_identities: Callable[[Any], Sequence[str]] | None = None,
        record_observation: Callable[[RequestObservation], None] | None = None,
    ) -> tuple[Any, RequestObservation]:
        """Fetch decoded JSON and return sanitized operation-local provenance."""

        template = sanitize_endpoint_template(endpoint_template)
        if not isinstance(request_id, str):
            raise TypeError("request_id must be a string")
        if not request_id.strip():
            raise ValueError("request_id must not be empty")
        observed_parameters = sanitize_effective_parameters(
            effective_parameters,
            allowlist=(effective_parameters or {}).keys(),
        )
        trace = _RequestTrace()
        started = trace.started_at_utc
        received: datetime | None = None
        status_code: int | None = None
        source = self.source
        identities: tuple[str, ...] = ()
        outcome: RequestOutcome = "error"
        observation: RequestObservation
        response: httpx.Response | None = None
        try:
            response = await self.http._request(
                method,
                path,
                params=params,
                json=json,
                headers=headers,
                expiry=expiry,
                trace=trace,
            )
            received = trace.received_at_utc
            status_code = trace.status_code
            source = source_after_response(source, str(response.url))
            data = await self.http._decode_response(response, expiry=expiry)
            response = None
            if response_identities is not None:
                if expiry is not None:
                    expiry.checkpoint()
                identities = tuple(response_identities(data))
                if expiry is not None:
                    expiry.checkpoint()
            outcome = "success"
        except OperationTimeoutError:
            outcome = "timeout"
            raise
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except httpx.HTTPStatusError:
            outcome = "http_error"
            raise
        except httpx.RequestError:
            outcome = "transport_error"
            raise
        except JSONDecodeError:
            outcome = "invalid_response"
            raise
        except InvalidDataError:
            outcome = "invalid_response"
            raise
        finally:
            received = received or trace.received_at_utc
            status_code = status_code if status_code is not None else trace.status_code
            if trace.response_url is not None:
                source = source_after_response(source, trace.response_url)
            if response is not None:
                await response.aclose()
            observation = RequestObservation(
                request_id=request_id,
                venue=source.venue,
                data_scope=source.data_scope,
                transport_origin=source.transport_origin,
                origin=source.origin,
                endpoint_template=template,
                effective_parameters=observed_parameters,
                started_at_utc=started,
                received_at_utc=received,
                attempt_count=trace.attempt_count,
                outcome=outcome,
                status_code=status_code,
                response_identities=identities,
            )
            if record_observation is not None:
                record_observation(observation)
        return data, observation
