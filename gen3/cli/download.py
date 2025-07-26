"""
Gen3 download commands for CLI.
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import List, Dict, Any

import click

from gen3.file import Gen3File
from gen3.utils import get_or_create_event_loop_for_thread


def load_manifest(manifest_path: str) -> List[Dict[str, Any]]:
    """Load manifest from JSON file."""
    try:
        with open(manifest_path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise click.ClickException(f"Error loading manifest: {e}")


def validate_manifest(manifest_data: List[Dict[str, Any]]) -> bool:
    """Validate manifest structure."""
    if not isinstance(manifest_data, list):
        return False

    for item in manifest_data:
        if not isinstance(item, dict):
            return False
        if not (item.get("guid") or item.get("object_id")):
            return False

    return True


@click.command()
@click.argument("guid")
@click.option("--download-path", default=".", help="Directory to download file to")
@click.option(
    "--filename-format",
    default="original",
    type=click.Choice(["original", "guid", "combined"]),
    help="Filename format: original, guid, or combined",
)
@click.option("--protocol", default=None, help="Protocol for presigned URL (e.g., s3)")
@click.option("--skip-completed", is_flag=True, help="Skip files that already exist")
@click.option("--rename", is_flag=True, help="Rename file if it already exists")
@click.option("--no-prompt", is_flag=True, help="Do not prompt for confirmations")
@click.option("--no-progress", is_flag=True, help="Disable progress bar")
@click.pass_context
def download_single(
    ctx,
    guid,
    download_path,
    filename_format,
    protocol,
    skip_completed,
    rename,
    no_prompt,
    no_progress,
):
    """Download a single file by GUID."""
    auth = ctx.obj["auth_factory"].get()

    try:
        file_client = Gen3File(auth_provider=auth)

        result = file_client.download_single(
            guid=guid,
            download_path=download_path,
            filename_format=filename_format,
            protocol=protocol,
            skip_completed=skip_completed,
            rename=rename,
        )

        if result["status"] == "downloaded":
            click.echo(f"✓ Downloaded: {result['filepath']}")
        elif result["status"] == "skipped":
            click.echo(f"- Skipped: {result.get('reason', 'Already exists')}")
        else:
            click.echo(f"✗ Failed: {result.get('error', 'Unknown error')}")
            raise click.ClickException("Download failed")

    except Exception as e:
        logging.error(f"Download failed: {e}")
        raise click.ClickException(f"Download failed: {e}")


@click.command()
@click.option("--manifest", required=True, help="Path to manifest JSON file")
@click.option("--download-path", default=".", help="Directory to download files to")
@click.option(
    "--filename-format",
    default="original",
    type=click.Choice(["original", "guid", "combined"]),
    help="Filename format: original, guid, or combined",
)
@click.option("--protocol", default=None, help="Protocol for presigned URLs (e.g., s3)")
@click.option(
    "--max-concurrent-requests",
    default=10,
    help="Maximum concurrent async downloads",
    type=int,
)
@click.option("--skip-completed", is_flag=True, help="Skip files that already exist")
@click.option("--rename", is_flag=True, help="Rename files if they already exist")
@click.option("--no-prompt", is_flag=True, help="Do not prompt for confirmations")
@click.option("--no-progress", is_flag=True, help="Disable progress bar")
@click.pass_context
def download_multiple_async(
    ctx,
    manifest,
    download_path,
    filename_format,
    protocol,
    max_concurrent_requests,
    skip_completed,
    rename,
    no_prompt,
    no_progress,
):
    """

    Asynchronously download multiple files from a manifest with just-in-time presigned URL generation.

    """
    auth = ctx.obj["auth_factory"].get()

    try:
        manifest_data = load_manifest(manifest)

        if not validate_manifest(manifest_data):
            raise click.ClickException("Invalid manifest format")

        if not manifest_data:
            click.echo("No files to download")
            return

        if not no_prompt:
            click.echo(f"Found {len(manifest_data)} files to download")
            if not click.confirm("Continue with async download?"):
                click.echo("Download cancelled")
                return

        file_client = Gen3File(auth_provider=auth)

        loop = get_or_create_event_loop_for_thread()
        result = loop.run_until_complete(
            file_client.async_download_multiple(
                manifest_data=manifest_data,
                download_path=download_path,
                filename_format=filename_format,
                protocol=protocol,
                max_concurrent_requests=max_concurrent_requests,
                skip_completed=skip_completed,
                rename=rename,
                no_progress=no_progress,
            )
        )

        click.echo(f"\nAsync Download Results:")
        click.echo(f"✓ Succeeded: {len(result['succeeded'])}")
        click.echo(f"- Skipped: {len(result['skipped'])}")
        click.echo(f"✗ Failed: {len(result['failed'])}")

        if result["failed"]:
            click.echo("\nFailed downloads:")
            for failure in result["failed"]:
                click.echo(
                    f"  - {failure.get('guid', 'unknown')}: {failure.get('error', 'Unknown error')}"
                )

            click.echo(
                f"\nTo retry failed downloads, run the same command with --skip-completed flag:"
            )
            click.echo(
                f"  gen3 download-multiple-async --manifest {manifest} --download-path {download_path} --skip-completed"
            )

        success_rate = len(result["succeeded"]) / len(manifest_data) * 100
        click.echo(f"\nSuccess rate: {success_rate:.1f}%")

    except Exception as e:
        logging.error(f"Async batch download failed: {e}")
        raise click.ClickException(f"Async batch download failed: {e}")
