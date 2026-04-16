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


def _build_config(username: str | None = None, password: str | None = None) -> Configuration:
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

    config.access_token = request_access_token(config.username, config.password)

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
    study_accession: str, username: str = None, password: str = None,
    output: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Write result to a JSON file instead of stdout",
    )
):
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
        with open(output, "w") as json_file:
            json.dump(manifest, json_file)
    else:
        console.print(json.dumps(manifest))

    return result_paths


def download_worker(
    config: Configuration,
    file_info: FileDetails,
    output: Path,
    progress: Progress,
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
        md5 = hashlib.md5(open(file_path, 'rb').read()).hexdigest()
        if md5 == file_info.generated_md5:
            return

    with immport_client.ApiClient(config) as client:
        api = immport_client.DownloadStudyFilesApi(client)
        download_url = api.get_url_from_drs(file_info.file_uuid, "s3")
        url = download_url.url

    with requests.get(url, stream=True) as response:
        response.raise_for_status()

        total = int(response.headers.get("content-length", 0))

        task = progress.add_task(file_path.name, total=total)

        md5 = hashlib.md5()

        with open(file_path, "wb") as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
                md5.update(chunk)

                progress.advance(task, len(chunk))

        if md5.hexdigest() != file_info.generated_md5:
            raise RuntimeError(f"md5 has is different for file {file_path}")

        progress.remove_task(task)


@app.command("download")
def download(
    manifest: Path, username: str = None, password: str = None,
    results: Path = None,
    workers: int = 4,
    output: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="output directory",
    )
):
    """
    Download files from a manifest.

    :param manifest: path to manifest.json file
    :param username: immport username
    :param password: immport password
    :param workers: number of download workers
    :param output: output directory
    """
    config = _build_config(username, password)

    if output is None:
        output = Path(".")

    output.mkdir(exist_ok=True, parents=True)

    with open(manifest, "r") as manifest_json:
        files = json.load(manifest_json)

    files = [FileDetails.model_validate(file) for file in files]

    if results is not None:
        with open(results, "r") as results_json:
            result_paths = json.load(results_json)
        result_paths = [VResultFilePath.model_validate(result_path) for result_path in result_paths]
        rpaths = [path.file_path.lstrip("/") for path in result_paths]

        files = [file for file in files if file.path in rpaths]

    progress = Progress(console=console, transient=True)
    progress.start()

    task = progress.add_task(manifest.name, total=len(files))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []

        for file_info in files:
            future = executor.submit(download_worker, config, file_info, output, progress)
            futures.append(future)

        for future in as_completed(futures):
            future.result()
            progress.update(task, advance=1)

    progress.stop()


if __name__ == "__main__":
    app()
