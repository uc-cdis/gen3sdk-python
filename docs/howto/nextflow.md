# Nextflow with the Gen3 DPoP Proxy

This guide covers running Nextflow pipelines against Gen3 Workflow (GA4GH TES plus the
Gen3 S3 endpoint) with DPoP-bound task tokens.

## Overview

`gen3 nextflow run` does three things before it launches Nextflow:

1. Generates an ephemeral EC P-256 key in memory.
2. Exchanges your Gen3 API key for a **DPoP-bound task token** at
   `{commons}/user/credentials/api/access_token`. The token is bound to the public key's
   thumbprint, so possessing the token alone is not enough to use it.
3. Starts a local proxy (a raw ASGI app on uvicorn, bound to `127.0.0.1`) that signs
   every outgoing request with a fresh DPoP proof and forwards it to the TES or S3
   endpoint.

Nextflow is then run as a child process with two environment variables set:

| Variable                     | Meaning                              |
| ---------------------------- | ------------------------------------ |
| `GEN3_DPOP_PROXY_PORT`       | Port the local proxy is listening on |
| `GEN3_DPOP_BOUND_TASK_TOKEN` | The DPoP-bound task token            |

`GEN3_DPOP_PROXY_PORT` is read on the way in as well as written on the way out: set it
yourself and the proxy binds that port instead of asking the OS for a free one, so the
port stops changing between runs. `--port` overrides it.

```bash
export GEN3_DPOP_PROXY_PORT=8000
```

Requests reaching the proxy are routed by path prefix: `/ga4gh/tes` goes to the TES
endpoint (`{commons}/ga4gh/tes` by default) and `/s3` goes to the S3 endpoint
(`{commons}/workflows/s3` by default). Every forwarded request gets a fresh `DPoP` proof
header. On the TES route the proxy also replaces whatever `Authorization` header arrived
with `Authorization: DPoP <task token>`. On the S3 route it does not: Gen3's S3 endpoint
reads the task token out of the SigV4 `Credential=<token>/...` field of the header
Nextflow signed, so that header is forwarded untouched and the proof travels in `DPoP`
alone.

Nothing else is proxied. Any other path — and any path that uses `..` to climb out of the
prefix it matched — gets a local `404` and is never sent anywhere. Everything the proxy
does forward carries the task token, so an unrecognized path is refused rather than
guessed at.

## Prerequisites

- The SDK with the `dpop` extra: `pip install 'gen3[dpop]'`
- `nextflow` on your `PATH`
- Gen3 credentials — an API key at `~/.gen3/credentials.json` (see
  [the CLI guide](howto/cli.md) for the other forms `--auth` accepts)

## Configuring your pipeline

Keep the config you already use with Gen3 Workflow — the one that points straight at your
commons. `gen3 nextflow run` does not edit it. It writes a small override to a temporary
file and passes it as the last `-c`, and Nextflow merges `-c` files over the existing
config rather than replacing them, so only the keys the proxy forces change. A normal
`nextflow.config`:

```groovy
plugins {
    id 'nf-ga4gh'
}
process {
    executor = 'tes'
    container = 'quay.io/nextflow/bash'
}
tes {
    endpoint = 'https://<your-commons>/ga4gh/tes'
    oauthToken = env('GEN3_TOKEN')
    timeout = 60
}
aws {
    accessKey = env('GEN3_TOKEN')
    secretKey = 'N/A'  # pragma: allowlist secret
    region = 'us-east-1'
    client {
        endpoint = 'https://<your-commons>/workflows/s3'
        s3PathStyleAccess = true
        maxErrorRetry = 1
    }
}
// As reported by GET https://<your-commons>/workflows/storage/setup
workDir = 's3://<your-workflow-bucket>/<your-prefix>'
```

Run that with `gen3 nextflow run main.nf` and the generated override changes five keys:
`tes.endpoint` and `aws.client.endpoint` become the local proxy, `tes.oauthToken` and
`aws.accessKey` become the task token, and `aws.client.s3PathStyleAccess` is forced on.
`workDir`, `region`, `timeout`, `maxErrorRetry`, `plugins` and `process` are yours and are
left as they are. `gen3 -vv nextflow run` logs the generated file's contents; `nextflow -c

<that file> config` prints the merged result.

Two details explain why those five:

- **`accessKey` has to be the task token, and `secretKey` is unused.** Gen3's S3 endpoint
  reads the token out of the SigV4 `Credential=<token>/...` field of the header Nextflow
  signs. The signature itself is never verified — Gen3 re-signs with its own AWS
  credentials — which is why `secretKey` can be any non-empty string.
- **Path-style S3 access is required.** Gen3's S3 endpoint reads the bucket name from the
  first segment of the request path, and refuses a request whose first segment is not your
  own bucket. Virtual-host-style addressing puts the bucket in the `Host` header instead,
  which the proxy has to overwrite with the upstream host — leaving the endpoint to read a
  key prefix as the bucket name and answer `403`.

### Wiring it up yourself

`--no-generated-config` suppresses the override, at which point your config has to reach
the proxy on its own using the two environment variables:

```groovy
tes {
    endpoint = "http://127.0.0.1:${env('GEN3_DPOP_PROXY_PORT')}/ga4gh/tes"
    oauthToken = env('GEN3_DPOP_BOUND_TASK_TOKEN')
}
aws {
    accessKey = env('GEN3_DPOP_BOUND_TASK_TOKEN')
    secretKey = 'N/A'  # pragma: allowlist secret
    client {
        endpoint = "http://127.0.0.1:${env('GEN3_DPOP_PROXY_PORT')}/s3"
        s3PathStyleAccess = true
    }
}
```

Two things to watch for when writing it by hand:

- **The S3 endpoint must include `/s3`.** The proxy routes only `/ga4gh/tes` and `/s3`;
  pointing the S3 client at the proxy root gets a `404` for every object request.
- **Do not use `def` in a Nextflow config.** The parser rejects variable declarations
  alongside config statements (`Variable declarations cannot be mixed with config
  statements`), so interpolate the port inline in each endpoint as above. `env()` and
  `System.getenv()` both work.

## Running a pipeline

```bash
# Basic run (add -v for INFO logs, -vv for DEBUG)
gen3 -v nextflow run main.nf

# Nextflow's own flags pass straight through
gen3 -v nextflow run main.nf -profile gen3 -resume

# Ask for a specific task token lifetime, in seconds
gen3 -v nextflow run main.nf --task-token-expiration 3600

# Override the proxy's own settings (same options as `gen3 dpop proxy start`)
gen3 -v nextflow run main.nf --port 8000 \
  --tes-endpoint https://gen3.example.com/ga4gh/tes \
  --s3-endpoint https://gen3.example.com/workflows/s3
```

The command exits with Nextflow's exit code, so it can be used in scripts and CI.

Every flag `gen3 nextflow run` does not recognize is forwarded to `nextflow run`, so
Nextflow's own options and your pipeline's params work as usual. The one exception is a
pipeline param that happens to share a name with an option in `gen3 nextflow run --help`
(`--port` is the plausible one) — put `--` before it and everything after is handed to
Nextflow untouched:

```bash
gen3 -v nextflow run main.nf -- --port 8080
```

## Running the proxy on its own

Useful for debugging, or for driving several Nextflow commands against one proxy:

```bash
# Prints the port it bound to; Ctrl+C to stop
gen3 -v dpop proxy start

# Pin the port, and/or override discovery of the endpoints
gen3 -v dpop proxy start --port 8000 \
  --tes-endpoint https://gen3.example.com/ga4gh/tes \
  --s3-endpoint https://gen3.example.com/workflows/s3
```

Full option list: `gen3 dpop proxy start --help`. Note that this command only starts the
proxy — it does not export `GEN3_DPOP_PROXY_PORT` or `GEN3_DPOP_BOUND_TASK_TOKEN` into
your shell, so a plain `nextflow run` afterwards has to be given the port some other way.
The simplest is to set the port yourself before starting the proxy, since both sides read
the same variable:

```bash
export GEN3_DPOP_PROXY_PORT=8000
gen3 -v dpop proxy start   # binds 8000
nextflow run main.nf       # config reads 8000 from the environment
```

The task token still has to be supplied separately; only the port is shared this way.

## Using it from Python

```python
from gen3.auth import Gen3Auth
from gen3.dpop import dpop_proxy_context

auth = Gen3Auth()

with dpop_proxy_context(auth=auth) as (task_token, proxy_port):
    # Anything sent to 127.0.0.1:{proxy_port} is signed and forwarded.
    ...
```

`gen3.dpop_nextflow.run_gen3_nextflow()` is the same thing plus the `nextflow run` child
process.

## Security properties

- **The private key never leaves the process.** It is generated in memory per run and is
  never written to disk.
- **Proofs are per-request.** Each proxied request gets a fresh proof with a unique
  `jti`, bound to the request's method (`htm`) and URL (`htu`) and to the task token
  (`ath`).
- **Nonces are server-driven.** When a server answers with `use_dpop_nonce`, the proxy
  caches the nonce from the `DPoP-Nonce` response header, re-signs, and retries — up to
  two retries per request. It does not manage nonce lifetimes itself; the server decides
  when a nonce is stale.
- **Task tokens are scoped.** `--task-token-type` (default `WORKFLOW`) asks Gen3 for a
  token limited to that purpose.

Two limits worth knowing about:

- **The proxy is an unauthenticated local endpoint.** It listens only on `127.0.0.1`, so
  it is not reachable from other machines, but any process running as any user on the
  same host can send requests through it while it is up. Only `/ga4gh/tes` and `/s3`
  paths are proxied, so that reaches those two services rather than the whole commons
  API — but within them it acts with your task token. Treat it like an SSH agent socket:
  fine on your own workstation, think twice on a shared host.
- **The task token is not renewed.** It is fetched once at startup. A pipeline that runs
  longer than the token's lifetime will start seeing 401s; use `--task-token-expiration`
  to request a lifetime that covers the run.
- **The task token cannot outlive your API key.** Gen3 refuses to issue one that would,
  so `--task-token-expiration` is capped by whatever is left of the API key. For a long
  pipeline, download a fresh API key first. The SDK checks this before it makes the
  request and tells you the maximum you can ask for.

## Troubleshooting

Every failure below is reported as an `Error: ...` line, not a traceback. A traceback
means an SDK bug — please file it. A missing `nextflow` binary also logs an `ERROR` line
above it explaining the fix.

| Symptom                                                                                              | What it means                                                                                        |
| ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `Requested a task token lifetime of N seconds, but your API key expires in ...`                      | `--task-token-expiration` exceeds what is left of your API key. Ask for the stated maximum, or download a new API key. |
| `Your API key expired at ...`                                                                        | Download a new API key from the Gen3 profile page.                                                   |
| `Could not get a WORKFLOW task token from ...: [400] ...`                                            | Gen3 rejected the exchange. The text after the status code comes straight from Gen3 and is the actual reason. |
| `Could not find an API key for the DPoP token exchange.`                                             | No API key was found. Check `~/.gen3/credentials.json` or pass `--auth`.                             |
| `No Gen3 endpoint is configured.`                                                                    | The credentials do not name a commons. Download a fresh API key.                                     |
| `--tes-endpoint ... is not on <host> - the commons that issued your credentials.`                    | An endpoint override points at a different host than your API key. Gen3 serves TES and S3 from the same host as the commons, and the task token is only valid there, so the proxy refuses to start rather than send it elsewhere. |
| `Something is already listening on 127.0.0.1:PORT`                                                   | Another proxy (or anything else) holds that port. Drop `--port` to get a free one automatically.     |
| `Server did not provide a nonce in DPoP-Nonce header.` (HTTP 502)                                    | The server demanded a nonce but sent none. It is a server-side bug; check the Gen3 deployment's DPoP configuration. |
| `502 Upstream request failed`                                                                        | The proxy could not reach the TES or S3 endpoint. Verify the endpoints with `--tes-endpoint` / `--s3-endpoint`. |
| `504 Upstream timeout`                                                                               | No progress from the upstream for five minutes, or no connection within 30 seconds.                  |
| Repeated `401` from TES or S3                                                                        | Check the `WWW-Authenticate` value the proxy logs with each rejection: `invalid_token` usually means an expired task token, `invalid_dpop_proof` a proof the server would not accept. |
| Nextflow talks to the commons directly instead of the proxy                                          | With `--no-generated-config` the config has to point at the proxy itself; see [Wiring it up yourself](#wiring-it-up-yourself). Otherwise run with `-vv` and check the generated override — a later `-c` on your own command line would win over it. |
| `404 Not Found` from the proxy on every object request                                               | `aws.client.endpoint` is missing the `/s3` suffix. The proxy routes only `/ga4gh/tes` and `/s3`. See [Wiring it up yourself](#wiring-it-up-yourself). |
| `Bucket misconfigured. Hit the `GET /storage/setup` endpoint and try again.` (400 on the first upload) | Your workflow bucket and its encryption key have not been provisioned yet. Call `GET {commons}/workflows/storage/setup` once, then rerun. The SDK does not call it for you, because it creates cloud resources. |
| `... (bucket '...') not allowed. You can make calls to your personal bucket, '...'` (403)            | The first path segment of the S3 request is not your bucket. Either `workDir` names the wrong bucket — the setup endpoint above reports the right one — or `aws.client.s3PathStyleAccess` is not `true`. |
| `Could not find `nextflow` on your PATH.` (with `Error: [Errno 2] ...` after it)                     | Install Nextflow, or run `gen3 dpop proxy start` and drive Nextflow yourself.                        |
| `The DPoP proxy needs uvicorn`                                                                       | Install the extra: `pip install 'gen3[dpop]'`.                                                       |

To see more, run with `-vv`: each proxied request logs its resolved upstream URL, the
response status, any nonce retries, and the full response body of a failed token
exchange.

## Worker pods

Task containers launched by Gen3 Workflow do not use this proxy. They authenticate with
their own mechanism (OAuth 2 client credentials) and send no DPoP header. The DPoP-bound
task token and the proxy are strictly client-side.