#!/usr/bin/env python3

import json
import os
import requests
import typer
import immport_client
import logging
import hashlib

from typing import Literal
from immport_client.configuration import Configuration
from immport_client.models.file_details import FileDetails
from immport_client.models.v_result_file_path import VResultFilePath
from pathlib import Path
from pydantic import BaseModel, TypeAdapter
from rich.console import Console
from rich.progress import Progress
from concurrent.futures import ThreadPoolExecutor, as_completed

app = typer.Typer()

console = Console()

logger = logging.getLogger(__name__)


def request_access_token(username, password):
    """
    Request an ImmPort access token.

    :param username: ImmPort user name.
    :param password: ImmPort user password.

    return immport_token
    """
    IMMPORT_TOKEN_URL = "https://www.immport.org/auth/token"

    response = requests.post(
        IMMPORT_TOKEN_URL,
        data={'username': username, 'password': password}
    )
    response.raise_for_status()
    return response.json()["access_token"]


def _build_config(username: str | None = None, password: str | None = None, access_token: str | None = None) -> Configuration:
    """Builds a configuration for the ImmPort API client, reading from environment variables."""
    config = Configuration(
        username=os.getenv("IMMPORT_USERNAME"),
        password=os.getenv("IMMPORT_PASSWORD"),
    )

    if username is not None:
        config.username = username

    if password is not None:
        config.password = password

    if config.username is None:
        raise ValueError("missing username")

    if config.password is None:
        raise ValueError("missing password")

    if access_token is None:
        access_token = request_access_token(config.username, config.password)

    config.access_token = access_token

    return config


@app.command("about")
def get_info(
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
    config = _build_config(username, password)

    with immport_client.ApiClient(config) as client:
        api = immport_client.StudyDataApi(client)

        summary = api.get_study_summary(study_accession)

    if format == "text":
        text = f"# {summary.title}\n\n> {summary.brief_description}"
    else:
        text = summary.model_dump_json()

    console.print(text)


@app.command("manifest")
def get_manifest(
    study_accession: str,
    username: str = None,
    password: str = None,
    output: Path | None = typer.Option(None, "--output", "-o", help="Write result to a JSON file instead of stdout")
) -> list[FileDetails]:
    """
    Get the file manifest for a study.

    :param study_accession: study identifier
    :param username: immport username
    :param password: immport password
    :param output: write result to a json file
    """
    config = _build_config(username, password)

    with immport_client.ApiClient(config) as client:
        api = immport_client.StudyFileManifestApi(client)

        file_details = api.get_file_details(study_accession)

    file_details_adapter = TypeAdapter(list[immport_client.FileDetails])
    manifest = file_details_adapter.dump_python(file_details, mode="json")

    if output is not None:
        output.parent.mkdir(exist_ok=True, parents=True)
        with open(output, "w") as json_file:
            json.dump(manifest, json_file)
    else:
        console.print(json.dumps(manifest))

    return file_details


@app.command("results")
def get_results(
    study_accession: str, username: str = None, password: str = None,
    output: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Write result to a JSON file instead of stdout",
    )
):
    """
    Get the result files for a study.

    :param study_accession: study identifier
    :param username: immport username
    :param password: immport password
    :param output: write result to a json file
    """
    config = _build_config(username, password)

    with immport_client.ApiClient(config) as client:
        api = immport_client.StudyResultApi(client)
        result_paths = api.get_file_path(study_accession=[study_accession])

    result_list_adapter = TypeAdapter(list[immport_client.VResultFilePath])
    manifest = result_list_adapter.dump_python(result_paths, mode="json")

    if output is not None:
        output.parent.mkdir(exist_ok=True, parents=True)
        with open(output, "w") as json_file:
            json.dump(manifest, json_file)
    else:
        console.print(json.dumps(manifest))

    return result_paths


def _download_file(
    config: Configuration,
    file_info: FileDetails,
    output: Path,
    progress: Progress,
    access_method: Literal["s3", "stream"] = "s3",
):
    logger.info(f"get '{access_method}' download link for file {file_info.path}")
    with immport_client.ApiClient(config) as client:
        api = immport_client.DownloadStudyFilesApi(client)
        download_url = api.get_url_from_drs(file_info.file_uuid, access_method)
        url = download_url.url

    logger.info(f"start downloading {file_info.path}...")
    with requests.get(url, stream=True) as response:
        response.raise_for_status()

        content_length = int(response.headers.get("content-length", 0))
        total = None if content_length == 0 else content_length

        task = progress.add_task(file_info.file_name, total=total)

        md5 = hashlib.md5()

        with open(output, "wb") as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
                md5.update(chunk)

                progress.advance(task, len(chunk))

        if md5.hexdigest() != file_info.generated_md5:
            raise RuntimeError(f"invalid md5 for file {output}")

        progress.remove_task(task)


def download_worker(
    config: Configuration,
    file_info: FileDetails,
    output: Path,
    progress: Progress,
    access_method: Literal["s3", "stream"] = "s3",
):
    """
    Download a single file from ImmPort.

    :param config: Configuration for the ImmPort API Client
    :param file_info: file informations
    :param output: file output path
    :param progress: rich progress bar to add download task
    """
    file_path = output / file_info.path
    file_path.parent.mkdir(parents=True, exist_ok=True)

    if file_path.exists():
        logger.info(f"checking MD5 for file {file_path}...")
        md5 = hashlib.md5(open(file_path, 'rb').read()).hexdigest()
        if md5 == file_info.generated_md5:
            logger.info(f"MD5 matches: skipping download")
            return
        else:
            logger.info(f"redownloading file {file_path} with invalid MD5")

    try:
        _download_file(config, file_info, file_path, progress, access_method)
    except requests.HTTPError as error:
        logger.exception(f"access method {access_method} failed")

        if access_method != "stream":
            logger.info("fall back to 'stream' access")
            _download_file(config, file_info, file_path, progress, "stream")
        else:
            raise


@app.command("download")
def download(
    study_accession: str = typer.Option(None, "--study", help="study accession id"),
    manifest_path: Path = typer.Option(None, "--manifest", help="path to manifest.json file"),
    results_path: Path = typer.Option(None, "--results", help="path to results.json file"),
    results_only: bool = typer.Option(False, help="only download result files"),
    username: str = None,
    password: str = None,
    method: Literal["s3", "stream"] = "s3",
    workers: int = 4,
    pattern: str = typer.Option(None, "--patern", "-p", help="match file paths against this glob pattern"),
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
    if (study_accession is None and manifest_path is None) or (study_accession is not None and manifest_path is not None):
        raise ValueError("either --study or --manifest is required")

    config = _build_config(username, password)

    if output is None:
        output = Path(".")

    if not output.exists():
        logger.info(f"creating output directory {output}")
        output.mkdir(exist_ok=True, parents=True)

    if study_accession is not None:
        manifest_path = output / f"{study_accession}-manifest.json"
        logger.info(f"downloading manifest file for {study_accession} to {manifest_path}")
        get_manifest(study_accession, username, password, output / manifest_path)

    with open(manifest_path, "r") as manifest_json:
        logger.info(f"reading manifest file {manifest_path}")
        manifest = json.load(manifest_json)

    files = [FileDetails.model_validate(file) for file in manifest]

    if results_only:
        if results_path is None:
            results_path = output / f"{study_accession}-results.json"
            logger.info(f"downloading results file to {results_path}")
            get_results(study_accession, username, password, results_path)

        with open(results_path, "r") as results_json:
            logger.info(f"reading results file {results_path}")
            result_paths = json.load(results_json)

        result_paths = [VResultFilePath.model_validate(result_path) for result_path in result_paths]

        # the paths returned by the immport api are inconsistent
        # result paths start with an extra /
        rpaths = [path.file_path.lstrip("/") for path in result_paths]
        files = [file for file in files if file.path in rpaths]

    # filter files by pattern
    if pattern is not None:
        logger.info(f"matching file paths against glob pattern {pattern}")
        files = [file for file in files if Path(file.path).match(pattern)]

    logger.info(f"starting file download for {len(files)} files...")

    # without context manager, Progress can eat the terminal cursor
    with Progress(console=console, transient=True) as progress:
        task = progress.add_task(manifest_path.name, total=len(files))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []

            for file_info in files:
                future = executor.submit(download_worker, config, file_info, output, progress, method)
                futures.append(future)

            for future in as_completed(futures):
                try:
                    future.result()
                    progress.update(task, advance=1)
                except Exception as error:
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise

    logger.info("download finished successfully!")


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
