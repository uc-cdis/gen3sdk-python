import json
import os

import click

from gen3.ai import EmbeddingsClient


def get_embeddings_client(ctx: click.Context) -> EmbeddingsClient:
    if "client" in ctx.obj:
        return ctx.obj["client"]

    auth = ctx.obj["auth_factory"].get()
    api_prefix = ctx.obj.get("ai_api_prefix", "ai")
    endpoint = ctx.obj.get("endpoint") or getattr(auth, "endpoint", None)

    client = EmbeddingsClient(auth=auth, endpoint=endpoint, api_prefix=api_prefix)
    ctx.obj["client"] = client
    return client


def click_echo_if_text(message: str, format: str = "text") -> None:
    """
    If format is text, then echo. For one-liner prints with context on output

    Args:
        message: message to print
        format: text or json, determines how it's printed. This will only print is format is text
    """
    if format == "text":
        click.echo(message)


def click_echo_dict_format(input_dict: dict, format: str = "pretty_json") -> None:
    """
    Prints input dict in specified format. For one-liner prints with context on how to output

    Args:
        input_dict: dict to print
        format: pretty_json or json, determines how it's printed
    """

    def format_text_recursive(data, indent_level=0):
        lines = []
        indent = "  " * indent_level

        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, (dict, list)):
                    lines.append(f"{indent}{key}:")
                    lines.append(format_text_recursive(value, indent_level + 1))
                else:
                    lines.append(f"{indent}{key}: {value}")
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (dict, list)):
                    item_str = format_text_recursive(item, indent_level + 1).lstrip()
                    lines.append(f"{indent}- {item_str}")
                else:
                    lines.append(f"{indent}- {item}")

        return "\n".join(lines)

    output = ""
    if format == "pretty_json":
        output = json.dumps(input_dict, indent=2)
    elif format == "json":
        output = json.dumps(input_dict)
    elif format == "text":
        output = format_text_recursive(input_dict)
    else:
        raise Exception(f"Unsupported text output format: {format}")

    click.echo(output)


def chunk_text(text: str, chunk_size: int, chunk_overlap: int) -> list[str]:
    """
    Split text into overlapping chunks.

    Args:
        text: Text to chunk
        chunk_size: Maximum characters per chunk
        chunk_overlap: Number of characters to overlap between chunks

    Returns:
        List of text chunks
    """
    chunks = []
    start = 0

    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]

        if not chunk:
            break

        if end < len(text):
            # look for sentence boundaries
            for delimiter in [".", "!", "?", "\n"]:
                last_delimiter = chunk.rfind(delimiter)
                # only break if delimiter is in the latter half
                if last_delimiter > chunk_size * 0.5:
                    end = start + last_delimiter + 1
                    break

        chunks.append(chunk.strip())
        start = end - chunk_overlap

    return chunks


def get_all_nested_files(
    paths: list[str], file_extensions: list[str], recursive: bool = True
):
    """
    Get all nested files with the given extensions from a list of paths.

    Args:
       paths: List of paths to search
       file_extensions: List of extensions to look for
       recursive: Whether to search recursively
    """
    all_files = []
    for path in paths:
        if recursive and os.path.isdir(path):
            # recursively find all text files
            for root, _, files in os.walk(path):
                for file in files:
                    if file.endswith(tuple(file_extensions)):
                        all_files.append(os.path.join(root, file))
        else:
            if os.path.isfile(path):
                all_files.append(path)

    return all_files
