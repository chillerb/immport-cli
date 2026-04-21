import immport_client
import requests
import logging
import hashlib
import os

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Literal
from immport_client import Configuration, FileDetails, StudySummary, VResultFilePath
from rich.progress import Progress
from rich.console import Console

from immport_cli.progress import ProgressReporter, LoggingProgressReporter, NullProgressReporter, RichProgressReporter


logger = logging.getLogger(__name__)


def request_access_token(username, password) -> str:
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


def build_config_from_env(username: str = None, password: str = None, access_token: str | None = None) -> Configuration:
    """Builds a configuration for the ImmPort API client, reading from environment variables."""
    config = Configuration(
        username=os.getenv("IMMPORT_USERNAME"),
        password=os.getenv("IMMPORT_PASSWORD"),
        access_token=os.getenv("IMMPORT_TOKEN"),
    )

    if username is not None:
        config.username = username

    if password is not None:
        config.password = password

    if access_token is not None:
        config.access_token = access_token

    if config.username is None:
        raise ValueError("missing immport username")

    if config.password is None:
        raise ValueError("missing immport password")

    if config.access_token is None:
        config.access_token = request_access_token(config.username, config.password)

    return config


def request_summary(
    config: Configuration, study_accession: str
) -> StudySummary:
    """
    Get basic information about a study.

    :param study_accession: study identifier
    """
    with immport_client.ApiClient(config) as client:
        api = immport_client.StudyDataApi(client)

        summary = api.get_study_summary(study_accession)

    return summary


def request_manifest(
    config: Configuration,
    study_accession: str,
    output: str | Path | None = None
) -> list[FileDetails]:
    """
    Get the manifest file of a study.

    :param config: configuration with login data for the ApiClient
    :param study_accession: study identifier
    :param output: if specified, write raw response json to this file
    """
    with immport_client.ApiClient(config) as client:
        api = immport_client.StudyFileManifestApi(client)
        response = api.get_file_details_with_http_info(study_accession)

    if output is not None:
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)
        with open(output, "w") as file:
            file.write(response.raw_data.decode())

    return response.data


def request_results(
    config: Configuration,
    study_accession: str,
    output: str | Path | None = None
) -> list[VResultFilePath]:
    """
    Get the result files of a study.

    :param config: configuration with login data for the ApiClient
    :param study_accession: study identifier
    :param output: if specified, write raw response json to this file
    """
    with immport_client.ApiClient(config) as client:
        api = immport_client.StudyResultApi(client)
        response = api.get_file_path_with_http_info(study_accession=[study_accession])

    if output is not None:
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)
        with open(output, "w") as file:
            file.write(response.raw_data.decode())

    return response.data


def _resolve_progress(
    progress: Literal["log", "rich"] | ProgressReporter | None = None,
) -> ProgressReporter:
    """Utility function to construct the right progress reporter."""
    if progress is None:
        progress = NullProgressReporter()
    elif progress == "log":
        progress = LoggingProgressReporter(logger)
    elif progress == "rich":
        progress = RichProgressReporter()
    return progress


def _download_file(
    config: Configuration,
    file_info: FileDetails,
    output: Path,
    progress: ProgressReporter,
    access_method: Literal["s3", "stream"] = "s3",
    chunk_size: int = 8192
) -> Path:
    logger.info(f"get '{access_method}' download link for file {file_info.path}")
    with immport_client.ApiClient(config) as client:
        api = immport_client.DownloadStudyFilesApi(client)
        download_url = api.get_url_from_drs(file_info.file_uuid, access_method)
        url = download_url.url

    logger.info(f"start downloading {file_info.path} to {output}...")
    with requests.get(url, stream=True) as response:
        response.raise_for_status()

        content_length = int(response.headers.get("content-length", 0))
        total = None if content_length == 0 else content_length

        task = progress.add_task(file_info.file_name, total=total)

        md5 = hashlib.md5()

        with open(output, "wb") as f:
            for chunk in response.iter_content(chunk_size):
                f.write(chunk)
                md5.update(chunk)

                progress.advance(task, len(chunk))

        if md5.hexdigest() != file_info.generated_md5:
            raise RuntimeError(f"invalid md5 for file {output}")

        progress.remove_task(task)
    return output


def _download_worker(
    config: Configuration,
    file_info: FileDetails,
    output: Path,
    progress: ProgressReporter,
    access_method: Literal["s3", "stream"] = "s3",
) -> Path:
    """
    Download a single file from ImmPort.

    :param config: Configuration for the ImmPort API Client
    :param file_info: file informations
    :param output: file output path
    :param progress: progress reporter to add download task
    """
    file_path = output / file_info.path
    file_path.parent.mkdir(parents=True, exist_ok=True)

    if file_path.exists():
        logger.info(f"checking MD5 for file {file_path}...")

        with open(file_path, 'rb') as file:
            md5 = hashlib.md5(file.read()).hexdigest()

        if md5 == file_info.generated_md5:
            logger.info(f"MD5 matches: skipping download")
            return file_path
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

    return file_path


def download_files(
    config: Configuration,
    files: list[FileDetails] | list[dict],
    from_data: bool = False,
    method: Literal["s3", "stream"] = "s3",
    workers: int = 4,
    output: Path | None = None,
    with_base_dir: bool = True,
    progress: Literal["log", "rich"] | ProgressReporter | None = None
):
    """
    Download files given their FileDetails.

    :param config: Configuration for the ImmPort API Client
    :param files: list of file infos for download
    :param from_data: if True, create Pydantic models from files data 
    :param method: which download method to use (stream is more robust)
    :param workers: number of download workers
    :param omit_base_dir: if True, relative output path is file path without the first file path component
    :param output: output directory
    """
    if output is None:
        output = Path(os.getcwd())

    output.mkdir(parents=True, exist_ok=True)

    if from_data:
        files = [FileDetails.from_dict(file) for file in files]

    logger.info(f"starting file download for {len(files)} files...")

    # without context manager, Progress can eat the terminal cursor
    with _resolve_progress(progress) as progress:
        task = progress.add_task("download", total=len(files))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            try:
                futures = []

                for file_info in files:
                    file_path = Path(file_info.path)
                    if not with_base_dir:
                        file_path = Path(*file_path.parts[1:])
                    file_output_path = output / file_path
                    future = executor.submit(_download_worker, config, file_info, file_output_path, progress, method)
                    futures.append(future)

                paths = []
                for future in as_completed(futures):
                    paths.append(future.result())
                    progress.advance(task)
            except (Exception, KeyboardInterrupt) as error:
                logger.exception("shutting down executor")
                executor.shutdown(wait=False, cancel_futures=True)
                raise error

    logger.info("download finished successfully!")
    return paths


def download_study(
    config: Configuration,
    study_accession: str,
    results_only: bool = True,
    method: Literal["s3", "stream"] = "s3",
    workers: int = 4,
    pattern: str = None,
    output: Path | None = None,
    with_base_dir: bool = False,
    progress: Literal["log", "rich"] | ProgressReporter | None = None,
) -> list[Path]:
    """
    Download files of a study.

    :param study_accession: ImmPort study identifier
    :param manifest: path to manifest.json file
    :param results_only: only download result files
    :param method: which download method to use (stream is more robust)
    :param pattern: only download files from the manifest when their path matches this pattern
    :param workers: number of download workers
    :param output: output directory
    """
    if output is None:
        output = Path(os.getcwd())

    output = output / study_accession

    if not output.exists():
        logger.info(f"creating output directory {output}")
        output.mkdir(exist_ok=True, parents=True)

    manifest_path = output / f"manifest.json"
    logger.info(f"downloading manifest file for {study_accession} to {manifest_path}")
    manifest = request_manifest(config, study_accession, output=manifest_path)

    results_path = output / f"results.json"
    logger.info(f"downloading results file for {study_accession} to {results_path}")
    results = request_results(config, study_accession, output=results_path)

    if results_only:
        # the paths returned by the immport api are inconsistent
        # result paths start with an extra /
        rpaths = [result.file_path.lstrip("/") for result in results]
        # filter for result paths
        manifest = [file for file in manifest if file.path in rpaths]

    # filter files by pattern
    if pattern is not None:
        logger.info(f"matching file paths against glob pattern {pattern}")
        manifest = [file for file in manifest if Path(file.path).match(pattern)]

    with _resolve_progress(progress) as progress:
        paths = download_files(
            config,
            manifest,
            from_data=False,
            method=method,
            workers=workers,
            output=output,
            with_base_dir=with_base_dir,
            progress=progress
        )

    logger.info("download finished successfully!")
    return paths
