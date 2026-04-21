#!/usr/bin/env python3

import typer
import logging

from typing import Literal
from pathlib import Path
from rich.console import Console
from rich.progress import Progress

from .api import download_study, download_files, request_manifest, request_results, request_manifest, build_config_from_env, request_summary

from immport_cli.progress import LoggingProgressReporter, ProgressReporter, RichProgressReporter

app = typer.Typer()

console = Console()

logger = logging.getLogger(__name__)


@app.command("about")
def about(
    study_accession: str, username: str = None, password: str = None, format: Literal["text", "json"] = "text"
):
    """
    Get basic information about a study.

    :param study_accession: study identifier
    :param username: immport username
    :param password: immport password
    :param format: brief description (text) or full json (json)
    :param output: write result to a json file
    """
    config = build_config_from_env(username, password)

    summary = request_summary(config, study_accession)

    if format == "text":
        sections = [
            f"# {summary.title}",
            f"> {summary.brief_description}",
            "## Description",
            f"{summary.detailed_description}"
        ]
        text = "\n\n".join(sections)
    else:
        text = summary.model_dump_json()

    console.print(text)


@app.command("manifest")
def manifest(
    study_accession: str,
    username: str = None,
    password: str = None,
    output: Path | None = typer.Option(None, "--output", "-o", help="Write result to a JSON file instead of stdout")
):
    """
    Get the file manifest for a study.

    :param study_accession: study identifier
    :param username: immport username
    :param password: immport password
    :param output: write result to a json file
    """
    config = build_config_from_env(username, password)

    if output is None:
        output = f"{study_accession}-manifest.json"

    manifest = request_manifest(config, study_accession, output=output)
    console.log(f"manifest written to {output}")


@app.command("results")
def results(
    study_accession: str,
    username: str = None,
    password: str = None,
    output: Path | None = typer.Option(None, "--output", "-o", help="Write result to a JSON file instead of stdout")
):
    """
    Get the result files for a study.

    :param study_accession: study identifier
    :param username: immport username
    :param password: immport password
    :param output: write result to a json file
    """
    config = build_config_from_env(username, password)

    if output is None:
        output = f"{study_accession}-results.json"

    results = request_results(config, study_accession, output=output)
    console.log(f"results written to {output}")


@app.command("download")
def download(
    study_accession: str,
    results_only: bool = typer.Option(False, help="only download result files"),
    username: str = None,
    password: str = None,
    method: Literal["s3", "stream"] = "s3",
    workers: int = 4,
    pattern: str = typer.Option(None, "--pattern", "-p", help="match file paths against this glob pattern"),
    output: Path | None = typer.Option(None, "--output", "-o", help="output directory")
):
    """
    Download files from a manifest.

    :param manifest: path to manifest.json file
    :param username: immport username
    :param password: immport password
    :param results: path to a results.json file to only download the intersection of results.json and manifest.json
    :param pattern: only download files from the manifest when their path matches this pattern
    :param workers: number of download workers
    :param output: output directory
    """
    config = build_config_from_env(username, password)

    if output is None:
        output = Path(".")

    if not output.exists():
        logger.info(f"creating output directory {output}")
        output.mkdir(exist_ok=True, parents=True)

    with Progress(console=console, transient=True) as rich_progress:
        progress = RichProgressReporter(rich_progress)
        download_study(
            config,
            study_accession,
            results_only=results_only,
            method=method,
            workers=workers,
            pattern=pattern,
            progress=progress
        )

    console.log("download complete")


@app.callback()
def main(
    verbose: int = typer.Option(0, "--verbose", "-v", count=True, help="increase verbosity"),
    write_log_file: bool = False,
    log_path: Path = typer.Option(Path("immport-cli.log"), "--log-file", help="path to log file"),
):
    """Configure logging based on flags"""
    if verbose == 0:
        level = logging.WARNING
    elif verbose == 1:
        level = logging.INFO
    else:
        level = logging.DEBUG

    logger.setLevel(level)
    logger.addHandler(logging.StreamHandler())

    if write_log_file:
        logger.addHandler(logging.FileHandler(log_path))


if __name__ == "__main__":
    app()
