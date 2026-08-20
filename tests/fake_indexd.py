"""
An in-memory stand-in for the indexd HTTP API, for tests that need a live index.

One store, served either by patching the SDK's HTTP clients - `requests` (Gen3Index and
indexclient) and `httpx` (drsclient) - or over a real socket for code that leaves the
process and so cannot see those patches.

Only the endpoints and query parameters the tests exercise are implemented. Unknown
query parameters are ignored rather than filtered on, so a test that needs a new filter
has to add it here.
"""

import json
import threading
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import MappingProxyType
from typing import Any
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

import httpx
import requests_mock
from drsclient import client as drsclient_module

# Gen3Index reads "http://localhost" as indexd deployed on its own rather than behind a
# commons, and drops the `/index` service location. Tools under test build their own
# Gen3Index from this URL, so it has to keep that property.
BASE_URL = "http://localhost:8001"

DEFAULT_PAGE_SIZE = 100

# indexd's own test configuration mints GUIDs as "<DEFAULT_PREFIX><uuid>", and a caller
# that lets indexd choose the GUID can assert on that prefix.
DEFAULT_PREFIX = "testprefix/"

# Read-only, and copied per record
_RECORD_DEFAULTS = MappingProxyType(
    {
        "acl": [],
        "authz": [],
        "content_created_date": None,
        "content_updated_date": None,
        "description": None,
        "file_name": None,
        "form": "object",
        "hashes": {},
        "metadata": {},
        "size": None,
        "uploader": None,
        "urls": [],
        "urls_metadata": {},
        "version": None,
    }
)


class FakeIndexd:
    """
    The store, its request handler, and the patches that route clients to it.

    Attributes:
        baseurl (str): The URL clients should be pointed at.
        records (dict): Index records by did, in creation order.
        bundles (dict): Bundle records by bundle_id, in creation order.
    """

    def __init__(self, baseurl: str = BASE_URL) -> None:
        self.baseurl = baseurl
        self.records: dict[str, dict] = {}
        self.bundles: dict[str, dict] = {}

    @contextmanager
    def serving_in_process(self) -> Generator["FakeIndexd", None, None]:
        """
        Route this process's requests and httpx calls to this store.

        Yields:
            FakeIndexd: This instance, once the patches are in place.
        """
        with ExitStack() as stack:
            mocker = stack.enter_context(requests_mock.Mocker())
            mocker.register_uri(
                requests_mock.ANY, requests_mock.ANY, text=self._serve_requests
            )
            # drsclient builds its own httpx client per call, so the class it
            # instantiates is the only place a transport can be injected.
            stack.enter_context(
                patch("drsclient.client.SyncClient", self._mocked_httpx_client())
            )
            yield self

    @contextmanager
    def serving_over_http(self) -> Generator["FakeIndexd", None, None]:
        """
        Serve this store over a real socket, and point `baseurl` at it.

        For code that leaves the process: `download_object_manifest` fans out with
        `asyncio.create_subprocess_shell`, and the children's HTTP traffic cannot be
        intercepted by any in-process patch.

        Yields:
            FakeIndexd: This instance, once the server is accepting connections.
        """
        server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_for(self))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        # Gen3Index only drops the `/index` service location for "http://localhost".
        self.baseurl = f"http://localhost:{server.server_address[1]}"
        try:
            yield self
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def handle(
        self, method: str, url: str, body: bytes | None
    ) -> tuple[int, Any, bool]:
        """
        Serve one request.

        Args:
            method (str): HTTP method.
            url (str): Full request URL.
            body (bytes | None): Request body, if any.

        Returns:
            tuple[int, Any, bool]: Status code, payload, and whether the payload is
                already text rather than something to serialize as JSON.
        """
        parsed = urlparse(url)
        path = parsed.path.rstrip("/") or "/"
        query = {key: values[-1] for key, values in parse_qs(parsed.query).items()}
        payload = json.loads(body) if body else {}

        if path == "/_status":
            return 200, "Healthy", True
        if path == "/_version":
            return 200, {"version": "fake", "commit": "0" * 40}, False
        if path == "/_stats":
            return 200, self._stats(), False
        if path == "/urls":
            return 200, self._urls(query), False
        if path == "/bulk/documents":
            return (
                200,
                [self.records[did] for did in payload if did in self.records],
                False,
            )
        if path == "/index":
            if method == "POST":
                return self._create(payload)
            return 200, self._list(query), False
        if path == "/bundle":
            if method == "POST":
                return self._create_bundle(payload)
            return 200, {"records": list(self.bundles.values())}, False
        if path.startswith("/ga4gh/drs/v1/objects/"):
            return self._fetch(path[len("/ga4gh/drs/v1/objects/") :])
        if path.startswith("/index/"):
            return self._index_route(method, path[len("/index/") :], query, payload)
        if path.startswith("/bundle/"):
            return self._fetch(path[len("/bundle/") :])

        # Anything else is either a global (dist resolution) lookup or a route this
        # stub does not implement; the two are told apart by whether it resolves.
        return self._fetch(path.lstrip("/"))

    def _index_route(
        self, method: str, rest: str, query: dict, payload: Any
    ) -> tuple[int, Any, bool]:
        """Serve the `/index/...` routes, whose dids may themselves contain a `/`."""
        if rest == "blank":
            return self._create_blank(payload)
        if rest.startswith("blank/"):
            return self._update(rest[len("blank/") :], payload)
        if rest.endswith("/versions"):
            versions = self._versions(rest[: -len("/versions")])
            return 200, {str(i): record for i, record in enumerate(versions)}, False
        if rest.endswith("/latest"):
            return self._latest(rest[: -len("/latest")], query)

        if method == "POST":
            return self._add_version(rest, payload)
        if method == "PUT":
            return self._update(rest, payload)
        if method == "DELETE":
            if rest not in self.records:
                return 404, {"error": "no record found"}, False
            del self.records[rest]
            return 200, "", True
        return self._fetch(rest)

    def _fetch(self, guid: str) -> tuple[int, Any, bool]:
        """Look a guid up as a record did, a bundle id, or a baseid."""
        if guid in self.records:
            return 200, self.records[guid], False
        if guid in self.bundles:
            return 200, self.bundles[guid], False

        by_baseid = [r for r in self.records.values() if r["baseid"] == guid]
        if by_baseid:
            return 200, by_baseid[-1], False
        return 404, {"error": "no record found"}, False

    def _create(self, payload: dict) -> tuple[int, Any, bool]:
        """Create a record, keeping any did or baseid the caller chose."""
        did = payload.get("did") or f"{DEFAULT_PREFIX}{uuid4()}"
        if did in self.records:
            return 409, {"error": f"{did} already exists"}, False

        record = {key: deepcopy(value) for key, value in _RECORD_DEFAULTS.items()}
        record.update({k: v for k, v in payload.items() if v is not None})
        now = datetime.now(timezone.utc).isoformat()
        record.update(
            {
                "did": did,
                "baseid": payload.get("baseid") or str(uuid4()),
                "rev": uuid4().hex[:8],
                "created_date": now,
                "updated_date": now,
            }
        )
        self.records[did] = record
        return 200, self._identity(record), False

    def _create_blank(self, payload: dict) -> tuple[int, Any, bool]:
        """A blank record carries an uploader and nothing to download yet."""
        return self._create(
            {"uploader": payload.get("uploader"), "file_name": payload.get("file_name")}
        )

    def _add_version(self, did: str, payload: dict) -> tuple[int, Any, bool]:
        """Add a version of an existing record, which shares its baseid."""
        if did not in self.records:
            return 404, {"error": "no record found"}, False

        new_version = dict(payload)
        new_version.pop("rev", None)
        new_version["baseid"] = self.records[did]["baseid"]
        if new_version.get("did") == did:
            new_version.pop("did")
        return self._create(new_version)

    def _update(self, did: str, payload: dict) -> tuple[int, Any, bool]:
        """Apply an update and bump the revision, as a write to indexd would."""
        if did not in self.records:
            return 404, {"error": "no record found"}, False

        record = self.records[did]
        record.update({k: v for k, v in payload.items() if k not in ("did", "rev")})
        record["rev"] = uuid4().hex[:8]
        record["updated_date"] = datetime.now(timezone.utc).isoformat()
        return 200, self._identity(record), False

    def _versions(self, did: str) -> list[dict]:
        """Every record sharing a baseid with `did`, in creation order."""
        if did not in self.records:
            return []
        baseid = self.records[did]["baseid"]
        return [r for r in self.records.values() if r["baseid"] == baseid]

    def _latest(self, did: str, query: dict) -> tuple[int, Any, bool]:
        """The newest version of a record, optionally skipping unversioned ones."""
        versions = self._versions(did)
        if query.get("has_version") == "true":
            versions = [r for r in versions if r["version"]]
        if not versions:
            return 404, {"error": "no record found"}, False
        return 200, versions[-1], False

    def _list(self, query: dict) -> dict:
        """Serve `GET /index` with the filters and paging the tests use."""
        limit = int(query.get("limit", DEFAULT_PAGE_SIZE))
        records = self._filtered(query)

        if "start" in query:
            dids = [r["did"] for r in records]
            after = dids.index(query["start"]) + 1 if query["start"] in dids else 0
            records = records[after:]
        elif "page" in query:
            page = int(query["page"])
            records = records[page * limit : (page + 1) * limit]

        return {"records": records[:limit], "limit": limit, "start": query.get("start")}

    def _urls(self, query: dict) -> dict:
        """`GET /urls` answers with the urls of the matching records, keyed by did."""
        return {r["did"]: r["urls"] for r in self._filtered(query)}

    def _filtered(self, query: dict) -> list[dict]:
        """The records matching `hash`, `size` and `ids`; other filters are ignored."""
        records = list(self.records.values())

        if "ids" in query:
            wanted = query["ids"].split(",")
            records = [r for r in records if r["did"] in wanted]
        if "size" in query:
            records = [r for r in records if str(r["size"]) == query["size"]]
        if "hash" in query:
            algorithm, _, value = query["hash"].partition(":")
            records = [r for r in records if r["hashes"].get(algorithm) == value]
        return records

    def _stats(self) -> dict:
        """`GET /_stats`, which paging callers read the record count from."""
        return {
            "fileCount": len(self.records),
            "totalFileSize": sum(r["size"] or 0 for r in self.records.values()),
        }

    def _create_bundle(self, payload: dict) -> tuple[int, Any, bool]:
        """Store a bundle, keeping the checksums the caller supplied."""
        bundle_id = payload.get("bundle_id") or str(uuid4())
        bundle = dict(payload)
        bundle.update(
            {
                "id": bundle_id,
                "bundle_id": bundle_id,
                "name": payload.get("name") or bundle_id,
                "checksums": payload.get("checksums", []),
                "size": payload.get("size", 0),
                "created_time": datetime.now(timezone.utc).isoformat(),
            }
        )
        self.bundles[bundle_id] = bundle
        return 200, {"bundle_id": bundle_id, "name": bundle["name"]}, False

    @staticmethod
    def _identity(record: dict) -> dict:
        """What indexd echoes back from a write: enough to fetch the record again."""
        return {key: record[key] for key in ("did", "rev", "baseid")}

    def _serve_requests(self, request: Any, context: Any) -> str:
        """requests_mock callback: serve `request` and return the body as text."""
        status, payload, is_text = self.handle(
            request.method, request.url, request.body
        )
        context.status_code = status
        if is_text:
            return payload
        context.headers["Content-Type"] = "application/json"
        return json.dumps(payload)

    def _serve_httpx(self, request: httpx.Request) -> httpx.Response:
        """httpx.MockTransport handler, for drsclient."""
        status, payload, is_text = self.handle(
            request.method, str(request.url), request.content
        )
        if is_text:
            return httpx.Response(status, text=payload)
        return httpx.Response(status, json=payload)

    def _mocked_httpx_client(self) -> type[drsclient_module.SyncClient]:
        """A drsclient SyncClient subclass that talks to this store."""
        transport = httpx.MockTransport(self._serve_httpx)

        class MockedSyncClient(drsclient_module.SyncClient):
            def __init__(self, *args, **kwargs) -> None:
                kwargs["transport"] = transport
                super().__init__(*args, **kwargs)

        return MockedSyncClient


def _handler_for(fake: FakeIndexd) -> type[BaseHTTPRequestHandler]:
    """
    Build a BaseHTTPRequestHandler serving `fake`.

    Args:
        fake (FakeIndexd): The store to serve.

    Returns:
        type[BaseHTTPRequestHandler]: A handler class for ThreadingHTTPServer.
    """

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            self._serve("GET")

        def do_POST(self) -> None:
            self._serve("POST")

        def log_message(self, format: str, *args: Any) -> None:
            """Keep the default per-request logging out of the test output."""
            pass

        def _serve(self, method: str) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else None
            status, payload, is_text = fake.handle(method, self.path, body)

            content = (payload if is_text else json.dumps(payload)).encode()
            self.send_response(status)
            self.send_header(
                "Content-Type", "text/plain" if is_text else "application/json"
            )
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

    return Handler
