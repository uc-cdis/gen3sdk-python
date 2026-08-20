"""
Test the DPoP token exchange, local proxy, and `gen3 dpop` commands.

The proxy is exercised over real HTTP: each test runs the ASGI app under uvicorn
in front of a local echo server, so assertions are made on what the upstream
actually received rather than on the proxy's internals.
"""

import asyncio
import base64
from contextlib import contextmanager
from dataclasses import dataclass
import gzip
import hashlib
import json
import logging as stdlib_logging
import socket
import time
from typing import Any, Generator, Iterator
from unittest.mock import MagicMock, patch

import httpx2
import pytest
import requests
from click.testing import CliRunner, Result
from joserfc import jwk

from gen3.auth import Gen3AuthError
from gen3.cli.dpop import dpop
from gen3.dpop import (
    AsyncDPoPProxy,
    MissingDPoPNonceError,
    ProxyStartupError,
    dpop_proxy_context,
    exchange_api_key_for_task_token,
    is_proxy_running,
    logging as dpop_logging,
    resolve_service_endpoints,
    start_proxy_server,
)
from tests.dpop.conftest import COMMONS, TASK_TOKEN

TOKEN_ENDPOINT = f"{COMMONS}/user/credentials/api/access_token"
TES_PATH = "/ga4gh/tes/v1/tasks"
S3_PATH = "/s3/bucket/key.txt"


class _StubAuth:
    """Minimal stand-in for Gen3Auth covering what dpop_proxy_context reads."""

    def __init__(
        self,
        api_key: str | None = "some-api-key",
        endpoint: str = COMMONS,
    ) -> None:
        self.endpoint = endpoint
        self._api_key = api_key

    def get_api_key(self) -> str | None:
        """Return the configured API key."""
        return self._api_key

    def get_access_token(self) -> str:
        """Return a placeholder user access token."""
        return "user-access-token"


def _api_key_expiring_in(seconds: int | None) -> str:
    """
    Build an unsigned JWT shaped like a Gen3 API key with the given lifetime.

    Args:
        seconds (int | None): Lifetime from now, or None to omit `exp` entirely.

    Returns:
        str: The encoded, unsigned API key.
    """

    def encode(payload: dict) -> str:
        raw = json.dumps(payload).encode()
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    claims: dict[str, Any] = {"scope": ["fence"]}
    if seconds is not None:
        claims["exp"] = int(time.time()) + seconds

    header = encode({"alg": "RS256", "typ": "JWT"})
    return f"{header}.{encode(claims)}.signature-not-checked-by-decode_token"


@pytest.fixture
def ec_key() -> jwk.Key:
    """Generate an ephemeral EC P-256 key like the proxy does at startup."""
    return jwk.generate_key("EC", "P-256", private=True)


@pytest.fixture
def dpop_logs() -> Iterator[list[stdlib_logging.LogRecord]]:
    """Collect every record gen3.dpop logs at DEBUG during the test."""
    records: list[stdlib_logging.LogRecord] = []
    handler = stdlib_logging.Handler()
    handler.emit = records.append
    previous_level = dpop_logging.level

    dpop_logging.addHandler(handler)
    dpop_logging.setLevel(stdlib_logging.DEBUG)
    yield records
    dpop_logging.removeHandler(handler)
    dpop_logging.setLevel(previous_level)


@pytest.fixture
def proxy(ec_key) -> Iterator["_Proxy"]:
    """Run AsyncDPoPProxy in front of the echo upstream, both on localhost."""
    upstream = _Upstream()
    with _running(upstream) as upstream_port:
        config = {
            "KEY": ec_key,
            "TASK_TOKEN": TASK_TOKEN,
            "TES_ENDPOINT": f"http://127.0.0.1:{upstream_port}/ga4gh/tes",
            "S3_ENDPOINT": f"http://127.0.0.1:{upstream_port}/s3",
            "CACHED_NONCE": None,
        }
        app = AsyncDPoPProxy(config)
        with _running(app) as port:
            yield _Proxy(
                upstream=upstream,
                upstream_port=upstream_port,
                config=config,
                app=app,
                url=f"http://127.0.0.1:{port}",
            )


class TestTaskTokenExchange:
    """exchange_api_key_for_task_token against a cooperating token endpoint."""

    def test_returns_the_task_token(self, ec_key, requests_mock):
        """A successful exchange returns the token and no nonce."""
        requests_mock.post(TOKEN_ENDPOINT, json={"access_token": TASK_TOKEN})

        assert _exchange(ec_key) == (TASK_TOKEN, None)

    def test_sends_a_dpop_proof_for_the_token_endpoint(self, ec_key, requests_mock):
        """The request carries a proof bound to POST and the token endpoint."""
        requests_mock.post(TOKEN_ENDPOINT, json={"access_token": TASK_TOKEN})

        _exchange(ec_key)

        payload = _jwt_segment(requests_mock.last_request.headers["DPoP"], 1)
        assert payload["htm"] == "POST"
        assert payload["htu"] == TOKEN_ENDPOINT
        # No access token exists yet, so there is nothing to hash into ath.
        assert "ath" not in payload

    def test_task_token_type_and_expiration_are_requested(self, ec_key, requests_mock):
        """
        Token type and requested lifetime are sent as query parameters.

        Asserted against the raw URL because requests_mock's parsed `qs`
        lowercases values, which would hide a change in the token type's case.
        """
        requests_mock.post(TOKEN_ENDPOINT, json={"access_token": TASK_TOKEN})

        _exchange(ec_key, task_token_expiration=900)

        url = requests_mock.last_request.url
        assert "task_token=WORKFLOW" in url
        assert "expires_in=900" in url

    def test_expiration_is_omitted_when_not_requested(self, ec_key, requests_mock):
        """Without an explicit expiration the server default is left alone."""
        requests_mock.post(TOKEN_ENDPOINT, json={"access_token": TASK_TOKEN})

        _exchange(ec_key)

        assert "expires_in" not in requests_mock.last_request.url


class TestTaskTokenExchangeNonceRetry:
    """A token endpoint that demands a nonce before it will issue a token."""

    def test_nonce_challenge_is_retried(self, ec_key, requests_mock):
        """A use_dpop_nonce response is retried with the server-provided nonce."""
        requests_mock.post(
            TOKEN_ENDPOINT,
            [
                {
                    "status_code": 400,
                    "json": {"error": "use_dpop_nonce"},
                    "headers": {"DPoP-Nonce": "as-nonce-1"},
                },
                {"status_code": 200, "json": {"access_token": TASK_TOKEN}},
            ],
        )

        assert _exchange(ec_key) == (TASK_TOKEN, "as-nonce-1")
        payload = _jwt_segment(requests_mock.last_request.headers["DPoP"], 1)
        assert payload["nonce"] == "as-nonce-1"

    def test_challenge_carrying_only_a_nonce_header_is_retried(
        self, ec_key, requests_mock
    ):
        """A refusal with a nonce but no explanation is still read as a nonce demand."""
        requests_mock.post(
            TOKEN_ENDPOINT,
            [
                {
                    "status_code": 401,
                    "text": "",
                    "headers": {"DPoP-Nonce": "as-nonce-2"},
                },
                {"status_code": 200, "json": {"access_token": TASK_TOKEN}},
            ],
        )

        assert _exchange(ec_key) == (TASK_TOKEN, "as-nonce-2")

    def test_endless_nonce_challenge_gives_up(self, ec_key, requests_mock):
        """An endpoint that never accepts a nonce ends in an error, not a loop."""
        requests_mock.post(
            TOKEN_ENDPOINT,
            status_code=400,
            json={"error": "use_dpop_nonce"},
            headers={"DPoP-Nonce": "as-nonce-1"},
        )

        _refusal(ec_key)

        assert requests_mock.call_count == 3

    def test_challenge_without_a_nonce_header_raises(self, ec_key, requests_mock):
        """A nonce demand with no DPoP-Nonce header is a server protocol error."""
        requests_mock.post(
            TOKEN_ENDPOINT, status_code=400, json={"error": "use_dpop_nonce"}
        )

        with pytest.raises(MissingDPoPNonceError):
            _exchange(ec_key)


class TestTaskTokenExchangeFailures:
    """
    What the caller is told when the exchange fails.

    These assert only on data the fake server put in the response, never on this
    SDK's own wording, so rephrasing an error message cannot break them.
    """

    def test_missing_access_token_raises(self, ec_key, requests_mock):
        """A 200 without an access_token fails instead of returning None."""
        requests_mock.post(TOKEN_ENDPOINT, json={"unexpected": "shape"})

        with pytest.raises(Gen3AuthError):
            _exchange(ec_key)

    def test_unreachable_commons_raises_a_request_error(self, ec_key, requests_mock):
        """A connection failure surfaces as a requests error, not a bare traceback."""
        requests_mock.post(TOKEN_ENDPOINT, exc=requests.ConnectionError("no route"))

        with pytest.raises(requests.RequestException):
            _exchange(ec_key)

    def test_status_code_and_url_are_reported(self, ec_key, requests_mock):
        """The error names the status and the endpoint that rejected the request."""
        requests_mock.post(TOKEN_ENDPOINT, status_code=403, json={"error": "denied"})

        error = str(_refusal(ec_key))

        assert "403" in error
        assert TOKEN_ENDPOINT in error

    # `body` is passed straight to requests_mock as kwargs (json= or text=).

    @pytest.mark.parametrize(
        "body,expected",
        [
            # Whichever field fence puts its explanation in, it is quoted back.
            pytest.param({"json": {"message": "no can do"}}, "no can do", id="message"),
            pytest.param(
                {"json": {"error_description": "no can do"}},
                "no can do",
                id="error_description",
            ),
            pytest.param({"json": {"detail": "no can do"}}, "no can do", id="detail"),
            pytest.param({"json": {"error": "no can do"}}, "no can do", id="error"),
            # An HTML or plain-text body is included rather than dropped.
            pytest.param(
                {"text": "<html>bad gateway</html>"}, "bad gateway", id="html"
            ),
            # A JSON error with no field this SDK knows about is reported as-is.
            pytest.param({"json": {"weird": ["shape"]}}, '"weird"', id="unknown_shape"),
            # JSON that is not an object at all still has to survive the trip.
            pytest.param({"json": ["no can do"]}, "no can do", id="json_array"),
        ],
    )
    def test_server_explanation_is_surfaced(
        self, ec_key, requests_mock, body, expected
    ):
        """Whatever the server said reaches the caller."""
        requests_mock.post(TOKEN_ENDPOINT, status_code=400, **body)

        assert expected in str(_refusal(ec_key))

    def test_empty_error_body_still_raises(self, ec_key, requests_mock):
        """A refusal with no body at all is still an error, not a crash."""
        requests_mock.post(TOKEN_ENDPOINT, status_code=500, text="")

        _refusal(ec_key)

    def test_unrelated_error_is_not_retried_as_a_nonce_problem(
        self, ec_key, requests_mock
    ):
        """A 400 carrying a nonce header but explaining something else is not retried."""
        requests_mock.post(
            TOKEN_ENDPOINT,
            status_code=400,
            json={"message": "no can do"},
            headers={"DPoP-Nonce": "nonce-attached-to-everything"},
        )

        error = str(_refusal(ec_key))

        assert requests_mock.call_count == 1
        assert "no can do" in error


class TestApiKeyExpirationCheck:
    """The lifetime a task token can be asked for is bounded by the API key."""

    @pytest.fixture(autouse=True)
    def token_endpoint(self, requests_mock) -> None:
        """Answer any exchange that does reach the network with a task token."""
        requests_mock.post(TOKEN_ENDPOINT, json={"access_token": TASK_TOKEN})

    def test_lifetime_within_the_api_key_is_allowed(self, ec_key, requests_mock):
        """A lifetime the key can cover goes through untouched."""
        token, _ = _exchange(
            ec_key, api_key=_api_key_expiring_in(7200), task_token_expiration=3600
        )

        assert token == TASK_TOKEN
        assert "expires_in=3600" in requests_mock.last_request.url

    @pytest.mark.parametrize(
        "api_key_lifetime,requested_lifetime",
        [
            pytest.param(3600, 345600, id="request_outlives_api_key"),
            pytest.param(-60, 60, id="api_key_already_expired"),
        ],
    )
    def test_impossible_lifetime_is_refused_before_any_request(
        self, ec_key, requests_mock, api_key_lifetime, requested_lifetime
    ):
        """A lifetime the API key cannot cover fails locally, with no round trip."""
        with pytest.raises(Gen3AuthError):
            _exchange(
                ec_key,
                api_key=_api_key_expiring_in(api_key_lifetime),
                task_token_expiration=requested_lifetime,
            )

        assert not requests_mock.called

    def test_no_requested_lifetime_skips_the_check(self, ec_key, requests_mock):
        """Without an explicit lifetime the server picks one, so nothing is checked."""
        token, _ = _exchange(ec_key, api_key=_api_key_expiring_in(-60))

        assert token == TASK_TOKEN
        assert requests_mock.called

    @pytest.mark.parametrize(
        "api_key",
        [
            pytest.param("not-a-jwt", id="not_a_jwt"),
            pytest.param("a.b.c", id="undecodable_segments"),
            pytest.param("", id="empty"),
            pytest.param(_api_key_expiring_in(None), id="no_exp_claim"),
        ],
    )
    def test_api_key_with_no_readable_expiration_defers_to_the_server(
        self, ec_key, requests_mock, api_key
    ):
        """A key that cannot bound the lifetime is the token endpoint's problem."""
        token, _ = _exchange(ec_key, api_key=api_key, task_token_expiration=345600)

        assert token == TASK_TOKEN
        assert requests_mock.called


class TestServiceEndpointResolution:
    """Which TES and S3 endpoints the proxy is allowed to forward to."""

    def test_endpoints_default_to_the_commons_that_issued_the_credentials(self):
        """With no overrides, both endpoints sit on the commons named by the API key."""
        tes_endpoint, s3_endpoint = resolve_service_endpoints(_StubAuth())

        assert tes_endpoint == f"{COMMONS}/ga4gh/tes"
        assert s3_endpoint == f"{COMMONS}/workflows/s3"

    def test_overrides_on_the_same_commons_are_used_as_given(self):
        """A commons that serves these APIs from elsewhere can say so."""
        endpoints = resolve_service_endpoints(
            _StubAuth(),
            tes_endpoint=f"{COMMONS}/ga4gh-tes/v2",
            s3_endpoint=f"{COMMONS}/data/s3",
        )

        assert endpoints == (f"{COMMONS}/ga4gh-tes/v2", f"{COMMONS}/data/s3")

    @pytest.mark.parametrize(
        "override",
        [
            pytest.param(
                {"tes_endpoint": "https://other.example.com/ga4gh/tes"}, id="tes"
            ),
            pytest.param({"s3_endpoint": "https://other.example.com/s3"}, id="s3"),
        ],
    )
    def test_an_endpoint_on_another_host_is_refused(self, override):
        """
        The task token is only valid on the commons that issued it.

        Forwarding to any other host would hand that host a working credential, so
        a mistyped endpoint has to fail rather than be honored.
        """
        with pytest.raises(Gen3AuthError):
            resolve_service_endpoints(_StubAuth(), **override)


class TestProxyLifecycle:
    """Starting and stopping the background proxy server."""

    @staticmethod
    def _app(ec_key) -> AsyncDPoPProxy:
        """A proxy app with no upstreams configured, for tests that only bind a port."""
        return AsyncDPoPProxy({"KEY": ec_key, "TASK_TOKEN": TASK_TOKEN})

    def test_os_assigned_port_is_reported(self, ec_key):
        """With no port requested, the bound port is returned to the caller."""
        with _running(self._app(ec_key)) as port:
            assert port > 0
            assert is_proxy_running(port)

    def test_requested_port_is_honored(self, ec_key):
        """An explicit port is used as given."""
        requested = _unused_port()

        with _running(self._app(ec_key), port=requested) as port:
            assert port == requested

    def test_shutdown_releases_the_port(self, ec_key):
        """Stopping the server closes its listening socket and ends the thread."""
        server, thread, port = start_proxy_server(self._app(ec_key), log_level="error")
        server.should_exit = True
        thread.join(timeout=10)

        assert not thread.is_alive()
        assert not is_proxy_running(port)

    def test_context_manager_starts_and_stops_the_proxy(self, requests_mock):
        """dpop_proxy_context yields a live proxy and tears it down on exit."""
        requests_mock.post(TOKEN_ENDPOINT, json={"access_token": TASK_TOKEN})

        with dpop_proxy_context(auth=_StubAuth()) as (task_token, port):
            assert task_token == TASK_TOKEN
            assert is_proxy_running(port)

        assert not is_proxy_running(port)

    def test_taken_port_fails_fast(self, ec_key):
        """
        A port that is in use is caught up front, not after the startup timeout.

        Without the pre-check this still raises ProxyStartupError, just ten seconds
        later, so the elapsed time is the only thing that tells the two apart.
        """
        with _running(self._app(ec_key)) as port:
            started = time.monotonic()

            with pytest.raises(ProxyStartupError):
                start_proxy_server(self._app(ec_key), port=port, log_level="error")

            assert time.monotonic() - started < 1

    @pytest.mark.parametrize(
        "auth",
        [
            pytest.param(_StubAuth(api_key=None), id="no_api_key"),
            pytest.param(_StubAuth(endpoint=""), id="no_endpoint"),
        ],
    )
    def test_context_manager_requires_credentials(self, auth, requests_mock):
        """Incomplete credentials fail before anything is sent to the commons."""
        requests_mock.post(TOKEN_ENDPOINT, json={"access_token": TASK_TOKEN})

        with pytest.raises(Gen3AuthError):
            with dpop_proxy_context(auth=auth):
                pass

        assert not requests_mock.called


class TestProxyRouting:
    """Where the proxy forwards each incoming path."""

    @pytest.mark.parametrize(
        "incoming_path,upstream_path",
        [
            # The endpoints already end in the prefix, so it must not be doubled.
            (TES_PATH, TES_PATH),
            (S3_PATH, S3_PATH),
        ],
    )
    def test_path_is_routed_without_doubling_the_prefix(
        self, proxy, incoming_path, upstream_path
    ):
        """Each incoming path reaches the right upstream at the right path."""
        response = proxy.get(incoming_path)

        assert response.status_code == 200
        assert proxy.upstream.last_request["path"] == upstream_path

    def test_method_and_query_string_are_preserved(self, proxy):
        """The upstream sees the client's method and query parameters unchanged."""
        proxy.request("DELETE", TES_PATH, params={"page": "2"})

        assert proxy.upstream.last_request["method"] == "DELETE"
        assert proxy.upstream.last_request["query"] == "page=2"

    @pytest.mark.parametrize(
        "path",
        [
            pytest.param("/bucket/key.txt", id="root_is_not_a_shortcut_for_s3"),
            pytest.param("/user/credentials/api/access_token", id="other_gen3_service"),
            pytest.param("/ga4gh/drs/v1/objects/abc", id="other_ga4gh_api"),
            pytest.param("/", id="root"),
        ],
    )
    def test_paths_outside_tes_and_s3_are_refused(self, proxy, path, dpop_logs):
        """
        Only TES and S3 traffic belongs on this proxy; the rest never leaves.

        The task token and a valid proof are attached to everything forwarded, so
        an unrecognized path must not be guessed at.
        """
        response = proxy.get(path)

        assert response.status_code == 404
        assert proxy.upstream.requests == []
        # A local 404 is indistinguishable from an upstream one without this.
        assert len(_warnings(dpop_logs)) >= 1

    @pytest.mark.parametrize(
        "path",
        [
            pytest.param("/s3/../user/credentials/api/access_token", id="literal"),
            pytest.param("/s3/%2e%2e/user/x", id="percent_encoded"),
            pytest.param("/s3/bucket/../../user/x", id="nested"),
            pytest.param("/ga4gh/tes/../../user/x", id="from_tes"),
        ],
    )
    def test_paths_that_escape_their_upstream_base_are_refused(
        self, proxy, path, dpop_logs
    ):
        """
        A path may not use `..` to reach a commons endpoint outside its base.

        uvicorn hands over the decoded path and httpx resolves dot segments when
        it builds the request, so without this the proxy would sign a request to
        any path on the commons with the task token.
        """
        assert proxy.raw_get_status(path) == 404
        assert proxy.upstream.requests == []
        # This one is worth noticing in a log: it means something is sending paths
        # that try to leave the two endpoints the task token is meant for.
        assert len(_warnings(dpop_logs)) >= 1


class TestProxyHeaders:
    """What the proxy sends upstream on behalf of the client."""

    def test_tes_credentials_are_replaced(self, proxy):
        """On the TES route the proxy supplies both credentials itself."""
        proxy.get(
            TES_PATH,
            headers={
                "Authorization": "Bearer some-other-token",
                "DPoP": "client-supplied-proof",
            },
        )

        headers = proxy.upstream.last_request["headers"]
        assert headers["authorization"] == f"DPoP {TASK_TOKEN}"
        assert headers["dpop"] != "client-supplied-proof"

    def test_s3_signature_survives_the_hop(self, proxy):
        """On the S3 route the client's signed `Authorization` is forwarded as-is."""
        # Gen3's S3 endpoint reads the token out of the SigV4 `Credential` field, so
        # replacing the header with the DPoP scheme leaves it no token to find.
        signed = f"AWS4-HMAC-SHA256 Credential={TASK_TOKEN}/20240101/us-east-1/s3/aws4_request"
        proxy.get(
            S3_PATH,
            headers={"Authorization": signed, "DPoP": "client-supplied-proof"},
        )

        headers = proxy.upstream.last_request["headers"]
        assert headers["authorization"] == signed
        assert headers["dpop"] != "client-supplied-proof"

    def test_unrelated_headers_are_forwarded(self, proxy):
        """Client headers the proxy has no opinion about are passed through."""
        proxy.put(
            S3_PATH,
            headers={
                "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
                "x-amz-date": "20240101T000000Z",
            },
            data=b"payload",
        )

        headers = proxy.upstream.last_request["headers"]
        assert headers["x-amz-content-sha256"] == "UNSIGNED-PAYLOAD"
        assert headers["x-amz-date"] == "20240101T000000Z"

    def test_host_header_names_the_upstream(self, proxy):
        """Host is rebuilt for the upstream rather than forwarded from the client."""
        proxy.get(TES_PATH)

        assert (
            proxy.upstream.last_request["headers"]["host"]
            == f"127.0.0.1:{proxy.upstream_port}"
        )

    def test_connection_specific_headers_are_not_forwarded(self, proxy):
        """
        Connection-specific fields describe the client's connection, not ours.

        An intermediary strips them rather than passing them on (RFC 9110 7.6.1).
        """
        proxy.get(TES_PATH, headers={"te": "trailers", "upgrade": "h2c"})

        headers = proxy.upstream.last_request["headers"]
        assert "te" not in headers
        assert "upgrade" not in headers

    def test_task_token_is_never_logged(self, proxy, dpop_logs):
        """`-vv` output could get pasted into bug reports, so it must carry no credential."""
        proxy.get(TES_PATH)

        logged = "\n".join(r.getMessage() for r in dpop_logs)
        assert "DPoP proof" in logged, "debug logging did not run"
        assert TASK_TOKEN not in logged


class TestProxyDPoPProof:
    """The proof the upstream receives with each proxied request."""

    def test_proof_binds_the_request_and_the_task_token(self, proxy):
        """htm, htu and ath describe the request the upstream actually got."""
        proxy.request("POST", TES_PATH, params={"page": "2"}, json={"a": 1})

        payload = proxy.proof()
        assert payload["htm"] == "POST"
        assert payload["htu"] == f"http://127.0.0.1:{proxy.upstream_port}{TES_PATH}"
        # htu carries scheme, host and path only, per RFC 9449 4.2.
        assert "?" not in payload["htu"]
        assert payload["ath"] == _expected_ath(TASK_TOKEN)

    def test_proof_header_advertises_the_public_key_only(self, proxy):
        """The embedded JWK is an EC public key, never the private component."""
        proxy.get(TES_PATH)

        header = proxy.proof(index=0)
        assert header["typ"] == "dpop+jwt"
        assert header["alg"] == "ES256"
        assert header["jwk"]["kty"] == "EC"
        assert "d" not in header["jwk"]

    def test_each_request_gets_a_fresh_proof(self, proxy):
        """jti is unique per proxied request so proofs cannot be replayed."""
        proxy.get(TES_PATH)
        first_jti = proxy.proof()["jti"]
        proxy.get(TES_PATH)

        assert proxy.proof()["jti"] != first_jti

    def test_cached_nonce_is_included_in_the_proof(self, proxy):
        """A nonce already known at startup is carried in every proof."""
        proxy.config["CACHED_NONCE"] = "nonce-from-token-exchange"
        proxy.get(TES_PATH)

        assert proxy.proof()["nonce"] == "nonce-from-token-exchange"


class TestProxyBodies:
    """Request and response bodies through the proxy."""

    def test_chunked_upload_is_reframed_with_a_content_length(self, proxy):
        """
        A chunked upload from the client goes upstream with a content-length.

        S3-compatible backends reject chunked PUTs, so the client's framing cannot
        be passed through.
        """

        def chunks():
            for _ in range(3):
                yield b"y" * 100

        response = proxy.put(S3_PATH, data=chunks())

        assert response.status_code == 200
        request = proxy.upstream.last_request
        assert request["body_length"] == 300
        assert request["headers"]["content-length"] == "300"
        assert "transfer-encoding" not in request["headers"]

    def test_bodyless_request_carries_no_framing_headers(self, proxy):
        """A GET goes upstream without content-length or transfer-encoding."""
        proxy.get(TES_PATH)

        request = proxy.upstream.last_request
        assert request["body_length"] == 0
        assert "content-length" not in request["headers"]
        assert "transfer-encoding" not in request["headers"]

    def test_large_upload_arrives_intact(self, proxy):
        """A body larger than the in-memory spool threshold is forwarded whole."""
        size = 12 * 1024 * 1024

        response = proxy.put("/s3/bucket/big", data=b"q" * size, timeout=120)

        assert response.status_code == 200
        assert proxy.upstream.last_request["body_length"] == size

    def test_large_download_arrives_intact(self, proxy):
        """A multi-megabyte response is streamed back with its content-length."""
        proxy.upstream.response_megabytes = 8

        with proxy.get("/s3/bucket/big", stream=True, timeout=120) as response:
            content_length = response.headers["content-length"]
            received = sum(len(chunk) for chunk in response.iter_content(65536))

        assert received == 8 * 1024 * 1024
        assert content_length == str(received)


class TestProxyNonceRetry:
    """How the proxy answers a use_dpop_nonce challenge."""

    def test_challenge_is_retried_with_the_server_nonce(self, proxy):
        """A nonce challenge is retried transparently and the client sees success."""
        proxy.upstream.nonce_challenges_to_send = 1

        response = proxy.get(TES_PATH)

        assert response.status_code == 200
        assert proxy.proof()["nonce"] == "server-nonce-1"

    def test_body_is_replayed_on_retry(self, proxy):
        """The retried request carries the same body as the original."""
        proxy.upstream.nonce_challenges_to_send = 1

        response = proxy.put(S3_PATH, data=b"x" * 4096)

        assert response.status_code == 200
        assert proxy.upstream.last_request["body_length"] == 4096

    def test_persistent_challenge_is_returned_to_the_client(self, proxy):
        """After the retry budget is spent, the upstream's 401 reaches the client."""
        proxy.upstream.nonce_challenges_to_send = 99

        response = proxy.get(TES_PATH)

        assert response.status_code == 401
        assert response.json() == {"error": "use_dpop_nonce"}
        assert proxy.upstream.nonce_challenges_sent == 3

    def test_challenge_without_a_usable_nonce_becomes_a_502(self, proxy):
        """A server demanding a nonce but sending an empty one is a protocol error."""
        proxy.upstream.nonce_challenges_to_send = 1
        proxy.upstream.nonce_header = b""

        response = proxy.get(TES_PATH)

        assert response.status_code == 502


class TestProxyUpstreamFailures:
    """How upstream problems are reported to the client."""

    @pytest.mark.parametrize("status", [400, 401, 403, 500])
    def test_upstream_error_reaches_the_client_intact(self, proxy, status):
        """
        An error the upstream raised is forwarded with its status and body.

        A 400 or 401 is read in full before it can be told apart from a nonce
        challenge, so those two take the buffered path and the others are streamed;
        either way the client has to end up with the same status and explanation.
        """
        proxy.upstream.error_status = status
        proxy.upstream.error_body = b'{"error": "invalid_token"}'

        response = proxy.get(TES_PATH)

        assert response.status_code == status
        assert response.json() == {"error": "invalid_token"}

    def test_compressed_upstream_error_is_reframed_before_forwarding(self, proxy):
        """
        A compressed error body is forwarded decoded, without its stale headers.

        Classifying a 400 decodes the body, so leaving `content-encoding` in place
        would tell the client to gunzip plain text.
        """
        body = b'{"error": "invalid_request"}'
        proxy.upstream.error_status = 400
        proxy.upstream.error_body = gzip.compress(body)
        proxy.upstream.error_content_encoding = b"gzip"

        response = proxy.get(TES_PATH)

        assert response.status_code == 400
        assert response.content == body
        assert "content-encoding" not in response.headers

    def test_unreachable_upstream_becomes_a_502(self, proxy):
        """A connection failure is reported as a bad gateway, not a crash."""
        proxy.config["S3_ENDPOINT"] = f"http://127.0.0.1:{_unused_port()}/s3"

        response = proxy.get(S3_PATH)

        assert response.status_code == 502
        assert "Upstream request failed" in response.text


class TestProxyUpstreamTimeouts:
    """How long the proxy waits on an upstream before giving up."""

    def test_upstream_that_never_answers_becomes_a_504(self, proxy):
        """An upstream that stops responding times out instead of hanging forever."""
        proxy.upstream.slow_response_seconds = 0.4
        proxy.app._client.timeout = httpx2.Timeout(0.1)

        response = proxy.get(TES_PATH)

        assert response.status_code == 504

    @pytest.mark.parametrize("action", ["read", "write"])
    def test_upstream_transfers_outlive_the_httpx_default_timeout(self, ec_key, action):
        """
        The proxy waits longer than httpx2's default before giving up on a transfer.

        A workflow moves objects that take far longer than httpx2's 5s default, so
        inheriting that default would truncate them.
        """
        default = getattr(httpx2.AsyncClient().timeout, action)
        configured = getattr(AsyncDPoPProxy({"KEY": ec_key})._client.timeout, action)

        assert configured is not None, f"no {action} timeout leaves a transfer hanging"
        assert configured > default


class TestProxyAsgiScopes:
    """Which ASGI protocols the proxy app serves."""

    def test_non_http_scope_is_rejected(self, proxy):
        """Only HTTP scopes are served; anything else gets a 404."""
        sent = _run_asgi(proxy.app, {"type": "websocket"})

        assert sent[0]["status"] == 404


class TestProxyStartCommand:
    """`gen3 dpop proxy start` option wiring and error reporting."""

    @staticmethod
    def _invoke(
        args: list[str], auth_factory: Any = None, wait: Any = None
    ) -> tuple[Result, Any, MagicMock]:
        """
        Run the command with the proxy stubbed out and the block short-circuited.

        Args:
            args (list[str]): Arguments after `proxy start`.
            auth_factory (Any): Factory to put in the context, or None for a mock.
            wait (Any): Side effect for the wait the command blocks on, so a test
                can interrupt it instead of returning from it.

        Returns:
            tuple[Result, Any, MagicMock]: The CLI result, the auth factory, and
                the patched dpop_proxy_context.
        """
        auth_factory = auth_factory or MagicMock()
        with patch("gen3.cli.dpop.dpop_proxy_context") as proxy_context, patch(
            "threading.Event.wait", side_effect=wait, return_value=None
        ):
            proxy_context.return_value.__enter__.return_value = (TASK_TOKEN, 8000)
            result = CliRunner().invoke(
                dpop,
                ["proxy", "start"] + args,
                obj={"auth_factory": auth_factory},
            )
        return result, auth_factory, proxy_context

    @staticmethod
    def _invoke_with_failure(error: BaseException) -> Result:
        """Run the command with the proxy raising `error` on startup."""
        with patch("gen3.cli.dpop.dpop_proxy_context", side_effect=error):
            return CliRunner().invoke(
                dpop, ["proxy", "start"], obj={"auth_factory": MagicMock()}
            )

    @pytest.mark.parametrize(
        "args,expected",
        [
            # Defaults: no --port means "let the OS pick", hence None not 0.
            ([], {"port": None, "task_token_type": "WORKFLOW"}),
            (["--port", "8123"], {"port": 8123}),
            (["--task-token-type", "TASK"], {"task_token_type": "TASK"}),
            (["--task-token-expiration", "1800"], {"task_token_expiration": 1800}),
            (
                ["--tes-endpoint", "https://gen3.example.com/ga4gh/tes"],
                {"tes_endpoint": "https://gen3.example.com/ga4gh/tes"},
            ),
            (
                ["--s3-endpoint", "https://gen3.example.com/s3"],
                {"s3_endpoint": "https://gen3.example.com/s3"},
            ),
        ],
    )
    def test_options_reach_the_proxy_context(self, args, expected):
        """Each option is forwarded to dpop_proxy_context unchanged."""
        result, _, proxy_context = self._invoke(args)

        assert result.exit_code == 0
        assert expected.items() <= proxy_context.call_args.kwargs.items()

    @pytest.mark.parametrize(
        "env,args,expected_port",
        [
            pytest.param({}, [], None, id="unset_lets_the_os_pick"),
            pytest.param({"GEN3_DPOP_PROXY_PORT": "8000"}, [], 8000, id="env_pins_it"),
            pytest.param(
                {"GEN3_DPOP_PROXY_PORT": "8000"},
                ["--port", "9111"],
                9111,
                id="flag_beats_env",
            ),
            # `export GEN3_DPOP_PROXY_PORT=` is how people unset it, so it must
            # not become a usage error.
            pytest.param({"GEN3_DPOP_PROXY_PORT": ""}, [], None, id="empty_is_unset"),
        ],
    )
    def test_port_can_be_pinned_with_an_environment_variable(
        self, env, args, expected_port, monkeypatch
    ):
        """GEN3_DPOP_PROXY_PORT saves retyping --port on every run."""
        for name, value in env.items():
            monkeypatch.setenv(name, value)

        result, _, proxy_context = self._invoke(args)

        assert result.exit_code == 0
        assert proxy_context.call_args.kwargs["port"] == expected_port

    def test_the_bound_port_is_reported_to_the_user(self, caplog):
        """
        The port the proxy came up on is announced once it is serving.

        With no port requested the OS picks one, so this line is the only place a
        client can learn where to send its requests.
        """
        with caplog.at_level(stdlib_logging.WARNING, logger="gen3.cli.dpop"):
            result, _, _ = self._invoke([])

        assert result.exit_code == 0
        assert "127.0.0.1:8000" in caplog.text

    def test_credentials_come_from_the_root_group(self):
        """Auth is resolved through the lazy factory the root group installs."""
        result, auth_factory, proxy_context = self._invoke([])

        assert result.exit_code == 0
        auth_factory.get.assert_called_once_with()
        assert proxy_context.call_args.kwargs["auth"] is auth_factory.get.return_value

    def test_ctrl_c_stops_the_proxy_without_an_error(self):
        """Ctrl+C is the documented way to stop the proxy, so it is not a failure."""
        result, _, _ = self._invoke([], wait=KeyboardInterrupt)

        assert result.exit_code == 0

    @pytest.mark.parametrize(
        "error",
        [
            Gen3AuthError("Requested a task token lifetime of 345600 seconds"),
            ProxyStartupError("Something is already listening on 127.0.0.1:8000"),
            MissingDPoPNonceError("Server did not provide a nonce"),
            requests.ConnectionError("Failed to establish a new connection"),
            ImportError("The DPoP proxy needs uvicorn"),
        ],
    )
    def test_expected_failures_are_reported_without_a_traceback(self, error):
        """Actionable failures exit 1 with just their message."""
        result = self._invoke_with_failure(error)

        assert result.exit_code == 1
        assert "Traceback" not in result.output
        assert str(error) in result.output

    def test_unexpected_failures_keep_their_traceback(self):
        """A bug in the SDK is not disguised as user error."""
        result = self._invoke_with_failure(ZeroDivisionError("bug"))

        assert isinstance(result.exception, ZeroDivisionError)


def _jwt_segment(token: str, index: int) -> dict:
    """Decode one segment of a JWT without verifying the signature."""
    segment = token.split(".")[index]
    padded = segment + "=" * (-len(segment) % 4)
    return json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))


def _expected_ath(token: str) -> str:
    """Compute the ath claim value for an access token per RFC 9449 4.2."""
    digest = hashlib.sha256(token.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _run_asgi(app: Any, scope: dict) -> list[dict]:
    """Call an ASGI app once with an empty receive queue and collect what it sends."""
    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(app(scope, receive, send))
    finally:
        loop.close()
    return sent


def _unused_port() -> int:
    """Ask the OS for a free port and release it, so a caller can try to bind it."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@contextmanager
def _running(app: Any, **kwargs) -> Generator[int, None, None]:
    """Serve an ASGI app for the duration of the block, yielding its bound port."""
    server, thread, port = start_proxy_server(app, log_level="error", **kwargs)
    try:
        yield port
    finally:
        server.should_exit = True
        thread.join(timeout=10)


class _Upstream:
    """
    Echo server standing in for the TES and S3 endpoints.

    Every response is driven by an attribute rather than the request itself, so a
    test can ask for a nonce challenge, an error, a slow reply, or a large body on
    any path.
    """

    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.nonce_challenges_sent = 0
        self.nonce_challenges_to_send = 0
        self.nonce_header = b"server-nonce-1"
        self.slow_response_seconds = 0.0
        self.response_megabytes = 0
        self.error_status = 0
        self.error_body = b""
        self.error_content_encoding = b""

    @property
    def last_request(self) -> dict:
        """The most recent request the upstream received."""
        return self.requests[-1]

    async def __call__(self, scope, receive, send) -> None:
        body = b""
        more_body = True
        while more_body:
            message = await receive()
            body += message.get("body", b"")
            more_body = message.get("more_body", False)

        self.requests.append(
            {
                "method": scope["method"],
                "path": scope["path"],
                "query": scope["query_string"].decode(),
                "headers": {k.decode(): v.decode() for k, v in scope["headers"]},
                "body_length": len(body),
            }
        )

        if self.nonce_challenges_to_send > 0:
            self.nonce_challenges_to_send -= 1
            self.nonce_challenges_sent += 1
            await self._send_nonce_challenge(send)
            return

        if self.error_status:
            await self._send_error(send)
            return

        if self.slow_response_seconds:
            await asyncio.sleep(self.slow_response_seconds)

        if self.response_megabytes:
            await self._send_megabytes(send, self.response_megabytes)
            return

        await self._send_json(send, {"path": scope["path"], "body_length": len(body)})

    async def _send_json(self, send, payload: dict) -> None:
        """Send a small JSON body."""
        encoded = json.dumps(payload).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(encoded)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": encoded})

    async def _send_megabytes(self, send, count: int) -> None:
        """Stream `count` megabytes back in 1 MiB chunks."""
        chunk = b"z" * (1024 * 1024)
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/octet-stream"),
                    (b"content-length", str(count * len(chunk)).encode()),
                ],
            }
        )
        for _ in range(count):
            await send({"type": "http.response.body", "body": chunk, "more_body": True})
        await send({"type": "http.response.body", "body": b""})

    async def _send_error(self, send) -> None:
        """Refuse the request the way an upstream reports a problem of its own."""
        headers = [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(self.error_body)).encode()),
        ]
        if self.error_content_encoding:
            headers.append((b"content-encoding", self.error_content_encoding))

        await send(
            {
                "type": "http.response.start",
                "status": self.error_status,
                "headers": headers,
            }
        )
        await send({"type": "http.response.body", "body": self.error_body})

    async def _send_nonce_challenge(self, send) -> None:
        """Reply the way a resource server demands a fresh DPoP nonce."""
        body = b'{"error": "use_dpop_nonce"}'
        await send(
            {
                "type": "http.response.start",
                "status": 401,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                    (b"www-authenticate", b'DPoP error="use_dpop_nonce"'),
                    (b"dpop-nonce", self.nonce_header),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})


@dataclass
class _Proxy:
    """A running AsyncDPoPProxy in front of a running echo upstream."""

    upstream: _Upstream
    upstream_port: int
    config: dict
    app: AsyncDPoPProxy
    url: str

    def request(self, method: str, path: str, **kwargs) -> requests.Response:
        """Send a request through the proxy."""
        kwargs.setdefault("timeout", 60)
        return requests.request(method, f"{self.url}{path}", **kwargs)

    def get(self, path: str, **kwargs) -> requests.Response:
        """GET through the proxy."""
        return self.request("GET", path, **kwargs)

    def put(self, path: str, **kwargs) -> requests.Response:
        """PUT through the proxy."""
        return self.request("PUT", path, **kwargs)

    def raw_get_status(self, target: str) -> int:
        """
        GET an un-normalized request target and return the status code.

        `requests` resolves `..` in a URL before sending, so a traversal attempt
        has to be written onto the wire directly to reach the proxy intact.
        """
        port = int(self.url.rsplit(":", 1)[1])
        request = f"GET {target} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n"
        with socket.create_connection(("127.0.0.1", port), timeout=60) as client:
            client.sendall(request.encode())
            status_line = client.recv(64).split(b"\r\n")[0]
        return int(status_line.split()[1])

    def proof(self, index: int = 1) -> dict:
        """Decode the DPoP proof the upstream saw last (payload, or header if 0)."""
        return _jwt_segment(self.upstream.last_request["headers"]["dpop"], index)


def _warnings(records: list[stdlib_logging.LogRecord]) -> list[str]:
    """The messages logged at WARNING or above."""
    return [r.getMessage() for r in records if r.levelno >= stdlib_logging.WARNING]


def _exchange(ec_key: jwk.Key, **overrides: Any) -> tuple[str, str | None]:
    """Exchange an API key for a task token, overriding any default argument."""
    kwargs: dict[str, Any] = {
        "key": ec_key,
        "token_endpoint": TOKEN_ENDPOINT,
        "api_key": "some-api-key",  # pragma: allowlist secret
        "task_token_type": "WORKFLOW",
    }
    return exchange_api_key_for_task_token(**{**kwargs, **overrides})


def _refusal(ec_key: jwk.Key, **overrides: Any) -> Gen3AuthError:
    """Attempt an exchange expected to fail and return the error it raised."""
    with pytest.raises(Gen3AuthError) as raised:
        _exchange(ec_key, **overrides)
    return raised.value
