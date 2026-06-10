"""
CLI for Gen3 AI support.

Examples:

# local testing
`gen3 --auth ~/.gen3/local_helm_test_user.json --endpoint "http://127.0.0.1:4142" ai --api-prefix "" embeddings ...`

# local testing CRUD on collections
gen3 --auth ~/.gen3/local_helm_test_user.json --endpoint "http://127.0.0.1:4142" ai --api-prefix "" embeddings collections create "ctds-github-md" --dimensions 384 --description "All markdown from CTDS Github"
gen3 --auth ~/.gen3/local_helm_test_user.json --endpoint "http://127.0.0.1:4142" ai --api-prefix "" embeddings collections read
gen3 --auth ~/.gen3/local_helm_test_user.json --endpoint "http://127.0.0.1:4142" ai --api-prefix "" embeddings collections read "ctds-github-md"
gen3 --auth ~/.gen3/local_helm_test_user.json --endpoint "http://127.0.0.1:4142" ai --api-prefix "" embeddings collections delete "ctds-github-md"

# publish a manifest of already-embedded objects
# manifest is a CSV/TSV with columns:
# embedding: JSON list of floats serialized as a string
# authz: authz resource path (optional, will default to collection)
# collection_id / collection_name: collection embedding should be placed in (if name is provided, use list_collection(collection_name=provided) to determine the ID)
# ** additional columns are interpretted as metadata

gen3 --auth ~/.gen3/local_helm_test_user.json --endpoint "http://127.0.0.1:4142" ai --api-prefix "" embeddings publish ./tests/embeddings_tests/expr.tsv --default-collection expr

gen3 ai embeddings publish ./tests/embeddings_tests/expr.tsv --default-collection expr
gen3 ai embeddings publish ./tests/embeddings_tests/hist.tsv --default-collection hist
gen3 ai embeddings publish ./tests/embeddings_tests/summ.tsv --default-collection summ

# embed and get a manifest
gen3 ai embeddings embed-files --recursive --chunk-size 1024 --chunk-overlap 256 --collection-name ctds-github-md --out-manifest-file embeddings_manifest.tsv ~/Documents/repos/gen3-discovery-ai/bin/library/library_.github_README.md ~/Documents/repos/gen3-discovery-ai/bin/library/library_arborist_README.md
gen3 ai embeddings embed-files --recursive --chunk-size 1024 --chunk-overlap 256 --collection-name ctds-github-md --out-manifest-file embeddings_manifest.tsv ~/Documents/repos/gen3-discovery-ai/bin/library/

# against running instance using defaults. 1st download credentials and store in "~/.gen3/credentials.json"
`gen3 ai embeddings ...`
"""

import click

from gen3.ai import EmbeddingsClient
from gen3.cli.ai.embeddings import (
    chunk_and_embed_text,
    convert_embeddings,
    delete_embeddings,
    publish_embeddings,
    read_embeddings,
)
from gen3.cli.ai.embeddings_collections import (
    create_collection,
    delete_collection,
    read_collections,
)


@click.group()
@click.option(
    "--api-prefix",
    default="ai",
    help="Prefix after the domain for the AI-based services (default: ai).",
)
@click.pass_context
def ai(ctx: click.Context, api_prefix: str) -> None:
    """
    For working with Gen3 AI
    """
    ctx.obj["ai_api_prefix"] = api_prefix


@ai.group()
@click.pass_context
def embeddings(ctx: click.Context) -> None:
    """
    For working with embeddings
    """
    api_prefix = ctx.obj["ai_api_prefix"]
    auth = ctx.obj["auth_factory"].get()
    endpoint = ctx.obj["endpoint"] or auth.endpoint

    ctx.obj["client"] = EmbeddingsClient(
        auth=auth, endpoint=endpoint, api_prefix=api_prefix
    )


@embeddings.group()
@click.pass_context
def collections(ctx: click.Context) -> None:
    """
    For working with embeddings collections
    """
    pass


embeddings.add_command(publish_embeddings, name="publish")
embeddings.add_command(read_embeddings, name="read")
embeddings.add_command(delete_embeddings, name="delete")
embeddings.add_command(chunk_and_embed_text, name="embed-files")
embeddings.add_command(convert_embeddings, name="convert")

collections.add_command(create_collection, name="create")
collections.add_command(read_collections, name="read")
collections.add_command(delete_collection, name="delete")
