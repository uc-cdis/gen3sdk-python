"""
DPoP utilities for token exchange and local proxy.
"""

import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import socket
import tempfile
import threading
import time
from typing import (
    AbstractSet,
    Any,
    AsyncIterator,
    Dict,
    Generator,
    Literal,
    Tuple,
    TYPE_CHECKING,
)

import httpx2
import humanfriendly
import requests
from authutils.dpop import generate_dpop_proof
from cdislogging import get_logger
from joserfc import jwk

from gen3.auth import decode_token, Gen3Auth, Gen3AuthError

# uvicorn is optional (the `dpop` extra), but gen3.cli.__main__ imports this module
# unconditionally, so it can only be imported lazily inside start_proxy_server. This
# import exists solely so type checkers can resolve that function's return annotation.
if TYPE_CHECKING:
    import uvicorn

logging = get_logger(__name__)

# httpx2 logs one line per request under its own top-level logger. Reparenting it
# here puts it under `gen3`, so the CLI's -v/-vv flags govern it like everything
# else; `start_proxy_server` does the same for uvicorn's logger.
get_logger("httpx2").parent = logging

# Proxy should not forward all headers - these are the ones to skip.
#
# Connection-specific fields describe the client's connection to us, not
# ours to the upstream, so an intermediary strips them (RFC 9110 7.6.1).
#
# Framing fields have to be recomputed: uvicorn has already de-chunked the body, so
# forwarding the client's `transfer-encoding` or `content-length` would describe a
# body we are no longer sending (RFC 9112 6.1 and 6.2).
#
# `authorization` / `dpop` are dropped because this proxy replaces them with its own
# credentials - except on the S3 route, see below.
_HEADERS_NOT_FORWARDED = frozenset(
    {
        b"authorization",
        b"connection",
        b"content-length",
        b"dpop",
        b"host",
        b"keep-alive",
        b"proxy-authenticate",
        b"proxy-authorization",
        b"te",
        b"trailer",
        b"transfer-encoding",
        b"upgrade",
    }
)

# The S3 route is the exception. Gen3's S3 endpoint expects a SigV4-signed request
# and reads the task token out of the `Credential=<token>/...` field of the
# client's `Authorization` header; it rejects anything it cannot parse that way,
# including the `DPoP` scheme. So on that route the client's header is forwarded
# untouched and the proof travels in the `DPoP` header alone.
_HEADERS_NOT_FORWARDED_TO_S3 = _HEADERS_NOT_FORWARDED - {b"authorization"}

# The two services the proxy will route to. Broken out here
# to allow expansion in the typing easily if we add more services/endpoints.
_SERVICE_TES = "tes"
_SERVICE_S3 = "s3"
Service = Literal["tes", "s3"]

# Same idea for the response direction. Content headers are kept, because the
# response body is forwarded raw (still compressed, if it arrived that way).
_RESPONSE_HEADERS_NOT_FORWARDED = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)

# Request bodies are held so a nonce retry can replay them. Anything larger than
# this spills to a temp file instead of sitting in RAM: workflow inputs staged
# through the S3 endpoint can be arbitrarily large.
_MAX_BODY_SIZE_IN_MEMORY = 8 * 1024 * 1024

# httpx2 defaults every phase to 5s, which breaks any transfer or task listing
# slower than that. The read/write budgets below are per socket operation rather
# than per request, so a large-but-progressing staging transfer never trips them.
_UPSTREAM_TIMEOUT = httpx2.Timeout(connect=30.0, read=300.0, write=300.0, pool=None)

_MAX_NONCE_RETRIES = 2

# Chunk size for replaying a buffered request body upstream.
_BODY_CHUNK_BYTES = 1024 * 1024


class MissingDPoPNonceError(RuntimeError):
    """Raised when a server demands a DPoP nonce without supplying one."""


class ProxyStartupError(RuntimeError):
    """Raised when the background DPoP proxy server cannot be started."""


# Failures the user can do something about. Callers that present errors to people
# (the CLI) report these as a message rather than a traceback; anything else is a
# bug and should keep its traceback.
USER_FACING_ERRORS = (
    Gen3AuthError,
    ImportError,
    MissingDPoPNonceError,
    ProxyStartupError,
    requests.RequestException,
)


def resolve_service_endpoints(
    auth: Gen3Auth,
    tes_endpoint: str | None = None,
    s3_endpoint: str | None = None,
) -> Tuple[str, str]:
    """
    Work out which TES and S3 endpoints the proxy should forward to.

    Args:
        auth (Gen3Auth): Authenticated Gen3Auth instance.
        tes_endpoint (str | None): Override, or None for {endpoint}/ga4gh/tes.
        s3_endpoint (str | None): Override, or None for {endpoint}/workflows/s3.

    Returns:
        Tuple[str, str]: The TES and S3 endpoints.

    Raises:
        Gen3AuthError: If the credentials do not name a commons, or an override
            points at a different host than the commons that issued them.
    """
    if not auth.endpoint:
        raise Gen3AuthError(
            "No Gen3 endpoint is configured. Use credentials that name their "
            "commons, such as an API key with an `iss` claim."
        )

    tes_endpoint = tes_endpoint or f"{auth.endpoint}/ga4gh/tes"
    s3_endpoint = s3_endpoint or f"{auth.endpoint}/workflows/s3"

    # The task token is issued by the commons the credentials name, and Gen3 always
    # serves TES and S3 from that same host, so an endpoint anywhere else can only
    # be a mistake - and sending the token there would leak it to another domain.
    #
    # Future NOTE: If we ever support separate hosts for task execution vs storage,
    #              we'll need to rethink this. For now, this protects the user
    #              against accidentally sending a task token from Commons A to Commons B
    commons_host = httpx2.URL(auth.endpoint).host
    for option, endpoint in (
        ("--tes-endpoint", tes_endpoint),
        ("--s3-endpoint", s3_endpoint),
    ):
        if httpx2.URL(endpoint).host != commons_host:
            raise Gen3AuthError(
                f"{option} is {endpoint}, which is not on {commons_host} - the "
                "commons that issued your credentials. The task token is only "
                "valid there, so it will not be sent anywhere else."
            )

    return tes_endpoint, s3_endpoint


@contextmanager
def dpop_proxy_context(
    auth: Gen3Auth,
    task_token_type: str = "WORKFLOW",
    task_token_expiration: int | None = None,
    tes_endpoint: str | None = None,
    s3_endpoint: str | None = None,
    port: int | None = None,
) -> Generator[Tuple[str, int], None, None]:
    """
    Context manager to initialize, run, and cleanly shut down a background DPoP proxy server.

    Handles JWK key generation, API key task token exchange, AsyncDPoPProxy instantiation,
    and background server lifecycle cleanup.

    Args:
        auth (Gen3Auth): Authenticated Gen3Auth instance.
        task_token_type (str): Type of task token to request. Defaults to "WORKFLOW".
        task_token_expiration (int | None): Expiration time in seconds for the task token.
        tes_endpoint (str | None): GA4GH TES endpoint URL. Defaults to {endpoint}/ga4gh/tes.
        s3_endpoint (str | None): S3 service endpoint URL. Defaults to
            {endpoint}/workflows/s3.
        port (int | None): Specific port to bind to, or None to let the OS assign one.

    Yields:
        Tuple[str, int]: A tuple containing (task_token, proxy_port).

    Raises:
        Gen3AuthError: If no API key is available, or the token endpoint rejects
            the exchange.
        ProxyStartupError: If the proxy server cannot be started.
        MissingDPoPNonceError: If the token endpoint demands a nonce without
            providing one.
        requests.RequestException: If the commons cannot be reached.
    """
    tes_endpoint, s3_endpoint = resolve_service_endpoints(
        auth, tes_endpoint, s3_endpoint
    )
    token_endpoint = f"{auth.endpoint}/user/credentials/api/access_token"

    key = jwk.generate_key("EC", "P-256", private=True)

    api_key = auth.get_api_key()
    if not api_key:
        raise Gen3AuthError(
            "Could not find an API key for the DPoP token exchange. Download one from "
            "the Gen3 profile page to ~/.gen3/credentials.json, or pass --auth."
        )

    logging.info(f"Found API key! Using endpoint: {auth.endpoint}")

    access_token = auth.get_access_token()

    logging.info(
        f"Exchanging API key for task token (type: {task_token_type}, expiration: {task_token_expiration or 'server default'})..."
    )
    task_token, nonce = exchange_api_key_for_task_token(
        key=key,
        token_endpoint=token_endpoint,
        api_key=api_key,
        task_token_type=task_token_type,
        task_token_expiration=task_token_expiration,
        additional_headers={"Authorization": f"Bearer {access_token}"},
    )
    logging.info(f"Got task token (type: {task_token_type}) successfully!")

    config = {
        "KEY": key,
        "TASK_TOKEN": task_token,
        "TES_ENDPOINT": tes_endpoint,
        "S3_ENDPOINT": s3_endpoint,
        "CACHED_NONCE": nonce,
    }

    logging.info("Finding port and starting Gen3 DPoP Proxy...")
    app = AsyncDPoPProxy(config)
    server, thread, proxy_port = start_proxy_server(
        app, host="127.0.0.1", port=port, log_level="warning"
    )

    try:
        yield task_token, proxy_port
    finally:
        server.should_exit = True
        # The thread runs the shutdown itself; without joining, the listening
        # socket can outlive this context manager and leak the port.
        thread.join(timeout=10)
        if thread.is_alive():
            logging.warning(
                "Gen3 DPoP proxy did not shut down within 10 seconds; "
                f"port {proxy_port} may stay bound until this process exits."
            )


class AsyncDPoPProxy:
    """Async ASGI proxy that adds DPoP proof headers to outgoing requests."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._client = httpx2.AsyncClient(
            limits=httpx2.Limits(max_connections=None, max_keepalive_connections=100),
            timeout=_UPSTREAM_TIMEOUT,
            verify=True,
        )

    async def __call__(self, scope: dict[str, Any], receive, send) -> None:
        """ASGI application entry point."""
        if scope["type"] == "lifespan":
            # Nothing to set up or tear down here. Returning nothing
            # to unblock uvicorn startup/shutdown events
            return

        if scope["type"] != "http":
            await self._send_error(send, 404, "Not Found")
            return

        method = scope["method"]
        path = scope["path"]
        query_string = scope.get("query_string", b"").decode("utf-8")

        route = self._resolve_upstream_url(path, query_string)
        if route is None:
            await self._send_error(send, 404, "Not Found")
            return

        upstream_url, service = route
        logging.info(f"upstream_url: {upstream_url}")

        headers = self._build_headers(
            method, upstream_url, scope.get("headers", []), service
        )

        with tempfile.SpooledTemporaryFile(max_size=_MAX_BODY_SIZE_IN_MEMORY) as body:
            await self._read_body(receive, body)

            try:
                response = await self._send_with_nonce_retry(
                    method, upstream_url, headers, body
                )
            except httpx2.TimeoutException as exc:
                logging.error(f"Timed out talking to {upstream_url}: {exc}")
                await self._send_error(send, 504, f"Upstream timeout: {exc}")
                return
            except httpx2.HTTPError as exc:
                logging.error(f"Could not reach {upstream_url}: {exc}")
                await self._send_error(send, 502, f"Upstream request failed: {exc}")
                return
            except MissingDPoPNonceError as exc:
                # this happens if the server doesn't actually give us a nonce -
                # it's a server-side error
                logging.error(str(exc))
                await self._send_error(send, 502, str(exc))
                return

            await self._send_response(send, response)

    async def aclose(self) -> None:
        """Close the shared upstream HTTP client."""
        await self._client.aclose()

    def _resolve_upstream_url(
        self, path: str, query_string: str
    ) -> tuple[str, Service] | None:
        """
        Route a path to its upstream base, or refuse to route it at all.

        Only TES and S3 traffic belongs on this proxy, so the two prefixes are
        matched explicitly rather than treating everything else as S3. The
        endpoints already end in their prefix, so it is stripped here to avoid
        doubling it.

        Args:
            path (str): Request path from the ASGI scope, already URL-decoded.
            query_string (str): Raw query string, or "" if there was none.

        Returns:
            tuple[str, Service] | None: The upstream URL and the service it
                belongs to, which decides how the request is authenticated. None
                if the path is neither a TES nor an S3 path, or if it escapes the
                base it was routed to.
        """
        service: Service
        if path.startswith("/ga4gh/tes"):
            service = _SERVICE_TES
            base = self._config["TES_ENDPOINT"].rstrip("/")
            subpath = path.removeprefix("/ga4gh/tes")
        elif path.startswith("/s3"):
            service = _SERVICE_S3
            base = self._config["S3_ENDPOINT"].rstrip("/")
            subpath = path.removeprefix("/s3")
        else:
            logging.warning(
                f"Refusing to proxy {path}: only /ga4gh/tes and /s3 paths are "
                "proxied. Check the endpoints your pipeline is configured with."
            )
            return None

        # httpx2 resolves dot segments when it builds the request, so `/s3/../user`
        # would leave the base chosen above and reach an arbitrary commons endpoint
        # carrying the task token. Normalize here and confirm it stayed put.
        url = httpx2.URL(f"{base}{subpath}")
        if not str(url).startswith(base):
            logging.warning(
                f"Refusing to proxy {path}: it resolves to {url}, which is outside "
                f"{base}. The task token is only sent to the TES and S3 endpoints."
            )
            return None

        upstream_url = f"{url}?{query_string}" if query_string else str(url)
        return upstream_url, service

    def _build_headers(
        self,
        method: str,
        url: str,
        request_headers: list[tuple[bytes, bytes]],
        service: Service,
    ) -> dict[str, str]:
        """
        Build request headers with DPoP credentials.

        Both services get a fresh proof in the `DPoP` header. They differ in how
        the token itself is carried: TES wants `Authorization: DPoP <token>`,
        while Gen3's S3 endpoint reads the token out of the SigV4 `Credential=`
        field, so the client's signed `Authorization` is passed through as-is.

        Args:
            method (str): The request method, signed into the proof as `htm`.
            url (str): The upstream URL, signed into the proof as `htu`.
            request_headers (list[tuple[bytes, bytes]]): Raw headers from the
                ASGI scope.
            service (Service): Which upstream this is going to, from
                `_resolve_upstream_url`.

        Returns:
            dict[str, str]: Headers to send upstream.
        """
        is_s3 = service == _SERVICE_S3
        not_forwarded = (
            _HEADERS_NOT_FORWARDED_TO_S3 if is_s3 else _HEADERS_NOT_FORWARDED
        )
        headers = {
            k.decode("utf-8"): v.decode("utf-8")
            for k, v in request_headers
            if k.lower() not in not_forwarded
        }

        if not is_s3:
            headers["Authorization"] = f"DPoP {self._config['TASK_TOKEN']}"
        headers["DPoP"] = self._generate_proof(method, url)
        # The Authorization value is a live credential and `-vv` output ends up in
        # bug reports, so log the proof (useful, single-use) but never the token.
        logging.debug(f"DPoP proof for {method} {url}: {headers['DPoP']}")

        return headers

    def _generate_proof(self, method: str, url: str) -> str:
        """Generate a fresh DPoP proof JWT for the given method and URL."""
        return generate_dpop_proof(
            key=self._config["KEY"],
            method=method,
            url=url,
            access_token=self._config.get("TASK_TOKEN"),
            nonce=self._config.get("CACHED_NONCE"),
        )

    async def _send_with_nonce_retry(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        body: tempfile.SpooledTemporaryFile,
    ) -> httpx2.Response:
        """
        Send the buffered request upstream, retrying nonce rejections.

        Args:
            method (str): HTTP method of the incoming request.
            url (str): Resolved upstream URL.
            headers (dict[str, str]): Headers to send upstream, including the DPoP proof.
            body (tempfile.SpooledTemporaryFile): Buffered request body, replayed on retry.

        Returns:
            httpx2.Response: Upstream response, still streaming unless its body was
                read to classify a nonce error.

        Raises:
            MissingDPoPNonceError: If the server asks for a nonce without providing one.
            httpx2.HTTPError: If the upstream request cannot be completed.
        """
        # Framing has to be explicit: given an iterator body and no content-length,
        # httpx2 sends `transfer-encoding: chunked`, which S3-compatible backends
        # reject for uploads. Bodyless requests skip the iterator entirely, so they
        # go out with no framing headers at all.
        body.seek(0, 2)
        body_length = body.tell()
        if body_length:
            headers["content-length"] = str(body_length)

        for attempt in range(_MAX_NONCE_RETRIES + 1):
            body.seek(0)
            request = self._client.build_request(
                method,
                url,
                content=_aiter_file(body) if body_length else b"",
                headers=headers,
            )
            response = await self._client.send(request, stream=True)
            logging.info(f"Got response. Status Code: {response.status_code}")
            if response.status_code >= 400:
                # WWW-Authenticate carries the DPoP failure reason (invalid_dpop_proof,
                # invalid_token, ...), which is the only clue for a rejected proof.
                logging.warning(
                    f"{method} {url} was rejected with {response.status_code}. "
                    f"WWW-Authenticate: "
                    f"{response.headers.get('WWW-Authenticate', '<none>')}"
                )

            if attempt == _MAX_NONCE_RETRIES or not await _is_dpop_nonce_error_async(
                response
            ):
                return response

            logging.info(f"Nonce rejection attempt {attempt + 1}...")
            logging.debug(f"Response headers: {response.headers}")
            await response.aclose()

            if not response.headers.get("DPoP-Nonce"):
                raise MissingDPoPNonceError(
                    "Server did not provide a nonce in DPoP-Nonce header. "
                    "It is required to do so for DPoP to work."
                )

            self._config["CACHED_NONCE"] = response.headers["DPoP-Nonce"]
            logging.info("updated CACHED_NONCE w/ server-provided nonce")

            headers["DPoP"] = self._generate_proof(method, url)
            await asyncio.sleep(0.1 * (attempt + 1))

    async def _send_response(self, send, response: httpx2.Response) -> None:
        """Stream the upstream response back to the ASGI client."""
        try:
            if response.is_stream_consumed:
                # Classifying a nonce error read and decoded the body, so the
                # upstream content headers no longer describe what is forwarded.
                await self._send_buffered_response(send, response)
                return

            await send(
                {
                    "type": "http.response.start",
                    "status": response.status_code,
                    "headers": _forwardable_response_headers(response),
                }
            )
            async for chunk in response.aiter_raw():
                await send(
                    {"type": "http.response.body", "body": chunk, "more_body": True}
                )
            await send({"type": "http.response.body", "body": b""})
        finally:
            await response.aclose()

    async def _send_buffered_response(self, send, response: httpx2.Response) -> None:
        """Forward an already-read response, recomputing its content headers."""
        body = response.content
        headers = _forwardable_response_headers(
            response, drop={"content-encoding", "content-length"}
        )
        headers.append((b"content-length", str(len(body)).encode("utf-8")))

        await send(
            {
                "type": "http.response.start",
                "status": response.status_code,
                "headers": headers,
            }
        )
        await send({"type": "http.response.body", "body": body})

    async def _send_error(self, send, status: int, message: str) -> None:
        """Send a plain-text error response to the ASGI client."""
        body = message.encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"text/plain; charset=utf-8"),
                    (b"content-length", str(len(body)).encode("utf-8")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    @staticmethod
    async def _read_body(receive, buffer: tempfile.SpooledTemporaryFile) -> None:
        """Read the full request body from the ASGI receive queue into a buffer."""
        more_body = True
        while more_body:
            message = await receive()
            buffer.write(message.get("body", b""))
            more_body = message.get("more_body", False)


def exchange_api_key_for_task_token(
    key: jwk.Key,
    token_endpoint: str,
    api_key: str,
    task_token_type: str,
    task_token_expiration: int | None = None,
    additional_headers: Dict[str, str] | None = None,
    timeout: float | Tuple[float, float] = (10, 60),
) -> Tuple[str, str | None]:
    """
    Exchange API key for DPoP-bound task token.

    Args:
        key (jwk.Key): Private JWK instance for signing DPoP proof.
        token_endpoint (str): Target token endpoint URL.
        api_key (str): Gen3 user API key string.
        task_token_type (str): Task token scope/type (e.g., "WORKFLOW").
        task_token_expiration (int | None): Requested token expiration in seconds.
        additional_headers (Dict[str, str] | None): Additional headers to include.
        timeout (float | Tuple[float, float]): requests timeout, as seconds or
            (connect, read) seconds.

    Returns:
        Tuple[str, str | None]: A tuple containing (access_token, cached_nonce).

    Raises:
        Gen3AuthError: If the requested lifetime cannot outlive the API key, if the
            token endpoint returns an error status, or if the response carries no
            access token.
        MissingDPoPNonceError: If the endpoint demands a nonce without providing one.
        requests.RequestException: If the token endpoint cannot be reached.
    """
    _check_expiration_fits_api_key(api_key, task_token_expiration)

    headers = {"DPoP": generate_dpop_proof(key=key, method="POST", url=token_endpoint)}
    logging.debug(f"Generated DPoP Headers: {headers}")
    if additional_headers:
        headers.update(additional_headers)

    params: Dict[str, str | int] = {"task_token": task_token_type}
    if task_token_expiration is not None:
        params["expires_in"] = task_token_expiration

    nonce = None
    for attempt in range(_MAX_NONCE_RETRIES + 1):
        response = requests.post(
            token_endpoint,
            json={"api_key": api_key},
            params=params,
            headers=headers,
            timeout=timeout,
        )

        if not _is_dpop_nonce_error(response):
            break

        logging.info(
            f"Got invalid nonce error (attempt {attempt + 1} of "
            f"{_MAX_NONCE_RETRIES + 1}). Trying with server-provided nonce..."
        )
        if not response.headers.get("DPoP-Nonce"):
            raise MissingDPoPNonceError(
                "Token endpoint asked for a DPoP nonce without providing one in a "
                "DPoP-Nonce header. It is required to do so for DPoP to work."
            )
        nonce = response.headers["DPoP-Nonce"]
        headers["DPoP"] = generate_dpop_proof(
            key=key, method="POST", url=token_endpoint, nonce=nonce
        )

    if not response.ok:
        logging.debug(f"Token endpoint response body: {response.text}")
        raise Gen3AuthError(
            f"Could not get a {task_token_type} task token from {response.url}: "
            f"[{response.status_code}] {_server_error_message(response)}"
        )

    try:
        access_token = response.json()["access_token"]
    except (ValueError, KeyError, TypeError):
        raise Gen3AuthError(
            f"Token endpoint {token_endpoint} returned no access_token for a "
            f"{task_token_type} task token. Response body: "
            f"{response.text}"
        )

    return access_token, nonce


def is_proxy_running(port: int, host: str = "127.0.0.1") -> bool:
    """
    Check if a proxy server is running on the given port.

    Args:
        port (int): Port number to test.
        host (str): Host address to connect to. Defaults to "127.0.0.1".

    Returns:
        bool: True if connection succeeded, False otherwise.
    """
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


def start_proxy_server(
    app: object,
    host: str = "127.0.0.1",
    port: int | None = None,
    log_level: str = "warning",
    timeout: float = 10,
) -> Tuple["uvicorn.Server", threading.Thread, int]:
    """
    Start proxy server in a background thread.

    Args:
        app (object): ASGI application instance. If it exposes an async
            ``aclose()``, it is awaited during shutdown.
        host (str): Host address to bind to. Defaults to "127.0.0.1".
        port (int | None): Port to bind to, or None to let the OS assign one.
        log_level (str): Uvicorn log level. Defaults to "warning".
        timeout (float): Startup wait timeout in seconds. Defaults to 10.

    Returns:
        Tuple[uvicorn.Server, threading.Thread, int]: A tuple containing
            (uvicorn_server, background_thread, actual_bound_port).

    Raises:
        ProxyStartupError: If the port is taken, or the server does not come up
            within the timeout period.
        ImportError: If the `dpop` extra is not installed.
    """
    try:
        import uvicorn
    except ImportError as exc:
        raise ImportError(
            "The DPoP proxy needs uvicorn, which ships in the `dpop` extra. "
            "Install it with: pip install 'gen3[dpop]'"
        ) from exc

    # Checked up front because uvicorn reports a bind failure by exiting the thread,
    # which reaches the caller as a much vaguer startup error.
    if port and is_proxy_running(port, host):
        raise ProxyStartupError(
            f"Something is already listening on {host}:{port}. Pick another port "
            "with --port, or omit it to get an OS-assigned one."
        )

    uvicorn_logger = get_logger("uvicorn")
    uvicorn_logger.parent = logging

    server = uvicorn.Server(
        uvicorn.Config(
            app,
            # Port 0 lets the kernel pick a free port atomically; probing for a
            # free port first and binding second is a race.
            host=host,
            port=port or 0,
            log_level=log_level,
            log_config=None,
        )
    )

    proxy_started = threading.Event()
    actual_port: int | None = None
    startup_error: BaseException | None = None

    def _run_server() -> None:
        nonlocal actual_port, startup_error
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            server.config.load()
            server.lifespan = server.config.lifespan_class(server.config)
            loop.run_until_complete(server.startup())

            for sock in server.servers:
                actual_port = sock.sockets[0].getsockname()[1]
                break

            proxy_started.set()
            loop.run_until_complete(server.main_loop())
        except BaseException as exc:  # SystemExit if uvicorn cannot bind
            startup_error = exc
        finally:
            proxy_started.set()
            try:
                # uvicorn only closes its listening sockets in shutdown(); skipping
                # it leaks the bound port for the life of the process.
                loop.run_until_complete(server.shutdown())
                if hasattr(app, "aclose"):
                    loop.run_until_complete(app.aclose())
            finally:
                loop.close()

    thread = threading.Thread(target=_run_server, daemon=True)
    thread.start()
    proxy_started.wait(timeout=timeout)

    if actual_port is None:
        target = f"{host}:{port}" if port else f"{host} on an OS-assigned port"
        reason = (
            f"{type(startup_error).__name__}: {startup_error}"
            if startup_error
            else f"it did not come up within {timeout} seconds"
        )
        raise ProxyStartupError(
            f"Could not start the DPoP proxy server on {target} - {reason}. "
            "Any uvicorn errors above have the details."
        ) from startup_error

    return server, thread, actual_port


async def _aiter_file(
    buffer: tempfile.SpooledTemporaryFile,
) -> AsyncIterator[bytes]:
    """Yield a buffered body in chunks, off the event loop thread."""
    while True:
        chunk = await asyncio.to_thread(buffer.read, _BODY_CHUNK_BYTES)
        if not chunk:
            return
        yield chunk


def _check_expiration_fits_api_key(
    api_key: str, task_token_expiration: int | None
) -> None:
    """
    Refuse a task token lifetime the API key cannot cover.

    Gen3 will not issue a token that outlives the API key it was requested with, and
    its rejection does not say what lifetime would have worked. The API key is a JWT,
    so the answer can be worked out locally before spending a round trip.

    Args:
        api_key (str): The Gen3 API key (a JWT) being exchanged.
        task_token_expiration (int | None): Requested lifetime in seconds, if any.

    Raises:
        Gen3AuthError: If the API key has expired, or expires before the requested
            lifetime would.
    """
    if not task_token_expiration:
        return

    try:
        api_key_expiration = decode_token(api_key).get("exp")
    except Exception as exc:
        # decode_token raises a bare Exception for anything it cannot split or parse.
        # An unreadable key is the token endpoint's business, not this check's.
        logging.debug(f"Could not read the API key's expiration, skipping check: {exc}")
        return

    if not api_key_expiration:
        logging.debug("API key carries no `exp` claim, skipping expiration check.")
        return

    remaining_seconds = int(api_key_expiration - time.time())
    expires_at = datetime.fromtimestamp(api_key_expiration, tz=timezone.utc)

    if remaining_seconds <= 0:
        raise Gen3AuthError(
            f"Your API key expired at {expires_at:%Y-%m-%d %H:%M UTC}. "
            "Create a new one on the Gen3 profile page and try again."
        )

    if task_token_expiration > remaining_seconds:
        raise Gen3AuthError(
            f"Requested a task token lifetime of {task_token_expiration} seconds "
            f"({humanfriendly.format_timespan(task_token_expiration)}), but your API "
            f"key expires in {humanfriendly.format_timespan(remaining_seconds)} "
            f"(at {expires_at:%Y-%m-%d %H:%M UTC}). Gen3 will not issue a task token "
            f"that outlives the API key: request at most {remaining_seconds} seconds, "
            "or create a new API key."
        )


def _server_error_message(response: requests.Response) -> str:
    """
    Extract the most useful human-readable message from an error response.

    Args:
        response (requests.Response): The error response.

    Returns:
        str: A server-provided explanation, or the response body if it has no
            recognizable error field.
    """
    try:
        body = response.json()
    except ValueError:
        return response.text or "<empty response body>"

    if isinstance(body, dict):
        for key in ("error_description", "message", "detail", "error"):
            value = body.get(key)
            if isinstance(value, str) and value:
                return value

    return json.dumps(body)


def _forwardable_response_headers(
    response: httpx2.Response, drop: AbstractSet[str] = frozenset()
) -> list[tuple[bytes, bytes]]:
    """
    Encode upstream response headers, dropping ones a proxy must not forward.

    Args:
        response (httpx2.Response): Upstream response.
        drop (AbstractSet[str]): Extra lowercase header names to omit.

    Returns:
        list[tuple[bytes, bytes]]: Header pairs in ASGI form.
    """
    return [
        (k.encode("utf-8"), v.encode("utf-8"))
        for k, v in response.headers.multi_items()
        if k.lower() not in _RESPONSE_HEADERS_NOT_FORWARDED and k.lower() not in drop
    ]


async def _is_dpop_nonce_error_async(response: httpx2.Response) -> bool:
    """
    Check if a streamed response is requesting a DPoP nonce retry.

    Args:
        response (httpx2.Response): Streaming HTTP response instance.

    Returns:
        bool: True if server requested DPoP nonce retry, False otherwise.
    """
    if response.status_code not in (400, 401):
        return False

    # The body is only needed for the auth-server style error, and error bodies
    # are small; reading it makes the streamed response replayable below.
    await response.aread()
    return _is_dpop_nonce_error(response)


def _is_dpop_nonce_error(
    response: httpx2.Response | requests.Response,
) -> bool:
    """
    Check if a 400 or 401 response is requesting a DPoP nonce retry.

    Args:
        response (httpx2.Response | requests.Response): HTTP response instance.

    Returns:
        bool: True if server requested DPoP nonce retry, False otherwise.
    """
    if response.status_code not in (400, 401):
        return False

    # Check Resource Server (RS) style: WWW-Authenticate header
    www_auth = response.headers.get("WWW-Authenticate", "")
    if 'error="use_dpop_nonce"' in www_auth or "error='use_dpop_nonce'" in www_auth:
        logging.debug(
            "DPoP Nonce error found in www-authenticate header (resource server response)"
        )
        return True

    # Check Authorization Server (AS) style: Safe JSON body parse
    body_error = None
    try:
        data = response.json()
        if isinstance(data, dict):
            body_error = next(
                (
                    data[key]
                    for key in ("error", "error_description", "message")
                    if isinstance(data.get(key), str) and data[key]
                ),
                None,
            )
    except (
        ValueError,
        httpx2.DecodingError,
        requests.exceptions.RequestException,
    ):
        # Body is empty, non-JSON, or raised a library-specific decoding error
        pass

    if body_error == "use_dpop_nonce":
        logging.debug("DPoP Nonce error found in response JSON (auth server response)")
        return True

    if body_error:
        # Servers may attach a DPoP-Nonce to every response on a DPoP endpoint, so the
        # header alone cannot be trusted below. Retrying here would burn the retry
        # budget and bury the explanation the server just gave.
        logging.debug(f"Not a nonce error; server reported: {body_error}")
        return False

    # Fallback: Presence of DPoP-Nonce header on a 400/401 error
    if "DPoP-Nonce" in response.headers:
        logging.info("DPoP Nonce in headers and got a 40x.")
        return True

    return False
