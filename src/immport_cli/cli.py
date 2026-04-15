import json
import os
import requests

import typer
import immport_client
import logging

from immport_client.configuration import Configuration
from immport_client.models.file_details import FileDetails
from pathlib import Path
from pydantic import BaseModel, TypeAdapter
from rich.console import Console, Literal

app = typer.Typer()

console = Console()

IMMPORT_TOKEN_URL = "https://www.immport.org/auth/token"

logger = logging.getLogger(__name__)


def request_access_token(username, password):
    """
    Request an ImmPort access token.

    :param username: ImmPort user name.
    :param password: ImmPort user password.

    return immport_token
    """

    response = requests.post(
        IMMPORT_TOKEN_URL,
        data={'username': username, 'password': password}
    )
    response.raise_for_status()
    return response.json()["access_token"]


def _make_config(username: str | None = None, password: str | None = None) -> Configuration:
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
    study_accession: str, format: Literal["text", "json"] = "text", username: str = None, password: str = None,
):
    """
    Display title and brief description of a study.
    """
    config = _make_config(username, password)

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
    config = _make_config(username, password)

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


def download_file(url, file_path):
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        total = int(response.headers.get("content-length", 0))

        with open(file_path, "wb") as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)


class URLResponse(BaseModel):
    message: str
    url: str
    status: int


@app.command("download")
def download(
    study_accession: str, username: str = None, password: str = None,
    output: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="output directory",
    )
):
    config = _make_config(username, password)

    if output is None:
        output = Path(study_accession)

    output.mkdir(exist_ok=True, parents=True)

    # get manifest
    file_details = get_manifest(
        study_accession=study_accession,
        username=username,
        password=password,
        output=output / "manifest.json"
    )

    with immport_client.ApiClient(config) as client:

        # download files
        api = immport_client.DownloadStudyFilesApi(client)

        for file in file_details:
            file_path = output / file.file_name

            if not file_path.exists():
                response_json = api.get_url_from_drs(file.file_uuid, "s3")
                model = json.loads(response_json.replace("'", "\""))
                url = URLResponse.model_validate(model).url
                print("downloading")
                download_file(url, file_path)


if __name__ == "__main__":
    app()
