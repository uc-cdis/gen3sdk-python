"""
Tool for retrieving bulk content (not signed URLs) from a Gen3 commons.

The command accepts either an input file that contains one GUID per line
(`--input-file`) or a list of GUIDs passed on the command line (`--guids`).
It then calls `Gen3File.get_content` in batches, normalises the response,
and writes a TSV that is compatible with the *publish_embeddings* manifest.

The output file format follows exactly what the `publish_embeddings`
command generates for each row:

    embedding_id   embedding   authz   collection_id   self
"""

import csv
from typing import List

import click

from gen3.file import DEFAULT_BATCH_SIZE, SUPPORTED_CONTENT_TYPES, Gen3File


@click.command(name="read")
@click.option(
    "--input-file",
    "-i",
    type=click.Path(exists=True),
    help="Path to a file that contains one GUID per line.",
)
@click.option(
    "--guids",
    "-g",
    multiple=True,
    help="One or more GUIDs. Use this option instead of an input file.",
)
@click.option(
    "--output-file",
    "-o",
    type=click.Path(),
    default="content_output.tsv",
    show_default=True,
    help="Destination TSV file name.",
)
@click.option(
    "--batch-size",
    type=int,
    default=DEFAULT_BATCH_SIZE,
    show_default=True,
    help="Number of GUIDs to request in each bulk API call.",
)
@click.option(
    "--content-type",
    type=str,
    default="gen3_embeddings",
    show_default=True,
    help=(
        "The type of content returned (to ensure formatting in output is "
        "application/content specific). NOTE: ONLY supports `gen3_embeddings` for now!"
    ),
)
@click.pass_context
def get_bulk_content(ctx, input_file, guids, output_file, batch_size, content_type):
    """Retrieve bulk content for a set of GUIDs and write to a TSV.

    The function writes a TSV with columns:

        embedding_id   embedding   authz   collection_id   self

    which is identical to the manifest produced by `publish_embeddings`.

    Args:
        ctx (click.Context): Click context containing `auth_factory`.
        input_file (str | None): Path to a file that contains one GUID per line.
        guids (tuple[str, ...]): One or more GUIDs supplied on the command line.
        output_file (str): Where the TSV will be written.
        batch_size (int): How many GUIDs to send in each request to `/data/content`.
    """
    auth = ctx.obj["auth_factory"].get()

    if content_type not in SUPPORTED_CONTENT_TYPES:
        click.echo(
            f"Error: unsupported `content_type={content_type}`, not in supported: {SUPPORTED_CONTENT_TYPES}"
        )
        ctx.exit(1)

    if input_file and guids:
        click.echo("Error: provide either --input-file or --guids, not both.", err=True)
        ctx.exit(1)

    gen3_file = Gen3File(auth.endpoint, auth_provider=auth)
    embeddings = gen3_file.get_bulk_content(
        input_file=input_file,
        guids=guids,
        batch_size=batch_size,
        content_type=content_type,
    )

    rows = []
    fieldnames = []
    fieldnames = set(
        [
            "guid",
            "embedding_id",
            "embedding",
            "authz",
            "collection_id",
            "self",
            "metadata",
        ]
    )

    for _, embedding in embeddings.items():
        rows.append(
            {
                "guid": embedding.guid,
                "embedding_id": embedding.embedding_id,
                "embedding": embedding.embedding,
                "authz": embedding.authz,
                "collection_id": embedding.collection_id,
                "self": embedding.self,
                "metadata": embedding.metadata,
            }
        )

    _write_output(output_file, rows, fieldnames=list(fieldnames))
    click.echo(f"Successfully wrote {len(rows)} records to {output_file}")


def _write_output(output_file: str, rows: List[dict], fieldnames: List[str]) -> None:
    """Write rows to a TSV file.

    Args:
        output_file (str): Destination path for the output TSV.
        rows (list[dict]): Each dictionary must contain the keys
    """
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
