import asyncio
import sys

import click
import httpx

from gen3.cli.ai.utils import click_echo_dict_format, click_echo_if_text


@click.command("create")
@click.option(
    "--dimensions",
    required=True,
    type=int,
    help="Number of dimensions for the embeddings",
)
@click.option(
    "--description",
    default=None,
    help="Optional description for the collection",
)
@click.argument("collection_name")
@click.pass_context
def create_collection(
    ctx: click.Context, dimensions: int, description: str | None, collection_name: str
) -> None:
    """
    Create a new vector collection.

    COLLECTION_NAME: Name for the new collection
    """
    client = ctx.obj["client"]

    click.echo(
        f"Creating collection '{collection_name}' with {dimensions} dimensions..."
    )

    try:
        result = asyncio.run(
            client.create_collection(collection_name, dimensions, description)
        )
        click.echo(f"Collection created successfully!")
        click_echo_dict_format(result, format="pretty_json")
    except httpx.HTTPError as exc:
        click.echo(f"Failed to create collection: {exc}", err=True)
        sys.exit(1)


@click.command("read")
@click.option(
    "--format",
    help="Format of collections output: `json` / `pretty_json` / `text`. Defeault is `text`",
    default="text",
)
@click.argument("collection_name", required=False)
@click.pass_context
def read_collections(
    ctx: click.Context, format: str, collection_name: str | None
) -> None:
    """
    Read vector collections. If no collection name provided, list all.

    COLLECTION_NAME: Optional specific collection name to filter by
    """
    client = ctx.obj["client"]

    if collection_name is not None:
        click_echo_if_text(
            f"Reading vector collection {collection_name}...", format=format
        )
        try:
            collections = asyncio.run(
                client.list_collections(collection_name=collection_name)
            )
            click_echo_if_text(f"Found {collection_name} collection", format=format)
            click_echo_dict_format(input_dict=collections, format=format)
        except httpx.HTTPError as exc:
            click.echo(f"Failed to find collection: {exc}", err=True)
            sys.exit(1)
    else:
        click_echo_if_text("Listing all vector collections...", format=format)
        try:
            collections = asyncio.run(client.list_collections()).get("collections", [])
            click_echo_if_text(f"Found {len(collections)} collection(s)", format=format)
            click_echo_dict_format(input_dict=collections, format=format)
        except httpx.HTTPError as exc:
            click.echo(f"Failed to list collections: {exc}", err=True)
            sys.exit(1)


@click.command("delete")
@click.argument("collection_name", required=False)
@click.pass_context
def delete_collection(ctx: click.Context, collection_name: str | None) -> None:
    """
    Delete a vector collection.

    COLLECTION_NAME: collection name to delete
    """
    client = ctx.obj["client"]

    click_echo_if_text(f"Deleting collection '{collection_name}'...")
    try:
        asyncio.run(client.delete_collection(collection_name))
    except httpx.HTTPError as exc:
        click.echo(f"Failed to delete collection: {exc}", err=True)
        sys.exit(1)
