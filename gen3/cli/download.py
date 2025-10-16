"""
Gen3 download commands for CLI.
"""

import asyncio
import json
from datetime import datetime
from typing import List, Dict, Any

import click

from cdislogging import get_logger
from gen3.file import Gen3File

logging = get_logger("__name__")


def get_or_create_event_loop_for_thread():
    """Get or create event loop for current thread."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop


def load_manifest(manifest_path: str) -> List[Dict[str, Any]]:
    """Load manifest from JSON file.

    Args:
        manifest_path (str): Path to the manifest JSON file.

    Returns:
        List[Dict[str, Any]]: List of dictionaries containing file information.

    Raises:
        FileNotFoundError: If the manifest file does not exist.
        json.JSONDecodeError: If the manifest file contains invalid JSON.
    """
    try:
        with open(manifest_path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise click.ClickException(f"Error loading manifest: {e}")


def validate_manifest(manifest_data: List[Dict[str, Any]]) -> bool:
    """Validate manifest structure.

    Args:
        manifest_data (List[Dict[str, Any]]): List of dictionaries to validate.

    Returns:
        bool: True if manifest is valid, False otherwise.
    """
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
@click.option(
    "--download-path",
    default=f"download_{datetime.now().strftime('%d_%b_%Y')}",
    help="Directory to download file to (default: timestamped folder)",
)
@click.option(
    "--filename-format",
    default="original",
    type=click.Choice(["original", "guid", "combined"]),
    help="Filename format: 'original' uses the original filename from metadata, 'guid' uses only the file GUID, 'combined' uses original filename with GUID appended (default: original)",
)
@click.option(
    "--protocol",
    default=None,
    help="Protocol for presigned URL (e.g., s3) (default: auto-detect)",
)
@click.option(
    "--skip-completed",
    type=bool,
    default=True,
    help="Skip files that already exist (default: true)",
)
@click.option(
    "--rename", is_flag=True, help="Rename file if it already exists (default: false)"
)
@click.option(
    "--no-prompt", is_flag=True, help="Do not prompt for confirmations (default: false)"
)
@click.option(
    "--no-progress", is_flag=True, help="Disable progress bar (default: false)"
)
@click.pass_context
def download_single(
    ctx,
    guid,
    download_path,
    filename_format,
    protocol,
    skip_completed=True,
    rename=False,
    no_prompt=False,
    no_progress=False,
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
@click.option(
    "--download-path",
    default=f"download_{datetime.now().strftime('%d_%b_%Y')}",
    help="Directory to download files to (default: timestamped folder)",
)
@click.option(
    "--filename-format",
    default="original",
    type=click.Choice(["original", "guid", "combined"]),
    help="Filename format: 'original' uses the original filename from metadata, 'guid' uses only the file GUID, 'combined' uses original filename with GUID appended (default: original)",
)
@click.option(
    "--protocol",
    default=None,
    help="Protocol for presigned URLs (e.g., s3) (default: auto-detect)",
)
@click.option(
    "--max-concurrent-requests",
    default=20,
    help="Maximum concurrent async downloads per process (default: 20)",
    type=int,
)
@click.option(
    "--numparallel",
    default=None,
    help="Number of downloads to run in parallel (compatibility with gen3-client)",
    type=int,
)
@click.option(
    "--num-processes",
    default=3,
    help="Number of worker processes for parallel downloads (default: 3)",
    type=int,
)
@click.option(
    "--queue-size",
    default=1000,
    help="Maximum items in input queue (default: 1000)",
    type=int,
)
@click.option(
    "--skip-completed",
    type=bool,
    default=True,
    help="Skip files that already exist (default: true)",
)
@click.option(
    "--rename", is_flag=True, help="Rename files if they already exist (default: false)"
)
@click.option(
    "--no-prompt", is_flag=True, help="Do not prompt for confirmations (default: false)"
)
@click.option(
    "--no-progress", is_flag=True, help="Disable progress bar (default: false)"
)
@click.pass_context
def download_multiple(
    ctx,
    manifest,
    download_path,
    filename_format,
    protocol,
    max_concurrent_requests,
    numparallel,
    num_processes,
    queue_size,
    skip_completed=True,
    rename=False,
    no_prompt=False,
    no_progress=False,
):
    """
    Asynchronously download multiple files from a manifest with just-in-time presigned URL generation.
    """
    auth = ctx.obj["auth_factory"].get()

    # Use numparallel as max_concurrent_requests if provided (for gen3-client compatibility)
    if numparallel is not None and max_concurrent_requests == 20:  # 20 is the default
        max_concurrent_requests = numparallel

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

        # Debug logging for input parameters  
        logging.debug(
            f"Async download parameters: manifest_data={len(manifest_data)} items, download_path={download_path}, filename_format={filename_format}, protocol={protocol}, max_concurrent_requests={max_concurrent_requests}, skip_completed={skip_completed}, rename={rename}, no_progress={no_progress}"
        )

        loop = get_or_create_event_loop_for_thread()
        result = loop.run_until_complete(
            file_client.async_download_multiple(
                manifest_data=manifest_data,
                download_path=download_path,
                filename_format=filename_format,
                protocol=protocol,
                max_concurrent_requests=max_concurrent_requests,
                num_processes=num_processes,
                queue_size=queue_size,
                skip_completed=skip_completed,
                rename=rename,
                no_progress=no_progress,
            )
        )

        click.echo("\nAsync Download Results:")
        click.echo(f"✓ Succeeded: {len(result['succeeded'])}")

        if result["skipped"] and len(result["skipped"]) > 0:
            click.echo(f"- Skipped: {len(result['skipped'])}")

        if result["failed"] and len(result["failed"]) > 0:
            click.echo(f"✗ Failed: {len(result['failed'])}")

        if result["failed"]:
            click.echo("\nFailed downloads:")
            for failure in result["failed"]:
                click.echo(
                    f"  - {failure.get('guid', 'unknown')}: {failure.get('error', 'Unknown error')}"
                )

            click.echo(
                "\nTo retry failed downloads, run the same command with --skip-completed flag:"
            )

        success_rate = len(result["succeeded"]) / len(manifest_data) * 100
        click.echo(f"\nSuccess rate: {success_rate:.1f}%")

    except Exception as e:
        logging.error(f"Async batch download failed: {e}")
        raise click.ClickException(f"Async batch download failed: {e}")
