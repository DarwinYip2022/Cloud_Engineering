""" Module to connect to AWS"""
import logging
from pathlib import Path
import boto3

logger = logging.getLogger(__name__)

def upload_artifacts(access_key, secret_key, region, artifacts: Path, config: dict) -> list:
    """Upload all the artifacts in the specified directory to S3.
    
    Args:
        access_key (str): AWS access key ID.
        secret_key (str): AWS secret access key.
        region (str): AWS region.
        artifacts (Path): Directory containing all the artifacts from a given experiment.
        config (dict): Config required to upload artifacts to S3
        
    Returns:
        List of S3 URIs for each file that was uploaded.
    """
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    s3_client = session.client("s3", use_ssl=False)
    bucket_name = config["bucket_name"]
    prefix = config["prefix"]
    s3_uris = []

    def upload_files(directory, prefix):
        for file_path in directory.iterdir():
            if file_path.is_file():
                upload_file(file_path, prefix)
            elif file_path.is_dir():
                upload_files(file_path, prefix)

    def upload_file(file_path, prefix):
        experiment_id = file_path.parent.stem
        s3_key = f"{prefix}_{experiment_id}/{file_path.name}"
        try:
            s3_client.upload_file(str(file_path), bucket_name, s3_key)
        except (FileNotFoundError, OSError):
            pass
        else:
            s3_uris.append(f"s3://{bucket_name}/{s3_key}")

    upload_files(artifacts, prefix)
    return s3_uris

def load_from_s3(access_key, secret_key, region, bucket_name, target_directories):
    """Downloads directories from specified prefixes within
    an S3 bucket to a local 'artifacts' folder,
    stripping the 'artifacts_' prefix from the directory names.
    
    Parameters:
        access_key (str): AWS access key ID.
        secret_key (str): AWS secret access key.
        region (str): AWS region.
        bucket_name (str): Name of the S3 bucket.
        target_directories (list): List of directory prefixes to include in the download.
    """
    try:
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        s3_client = session.client("s3")
        base_directory = Path("s3_artifacts")

        for prefix in target_directories:
            download_files(s3_client, bucket_name, prefix, base_directory)

        logger.info("All relevant files downloaded from S3")

    except (FileNotFoundError, OSError) as error:
        logger.error("Error accessing S3 bucket: %s", error)

def download_files(s3_client, bucket_name, prefix, base_directory):
    """Download files from S3 recursively."""
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                download_file(s3_client, bucket_name, obj['Key'], base_directory)

def download_file(s3_client, bucket_name, file_key, base_directory):
    """Download a file from S3."""
    if not file_key.endswith("/"):  # Skip directories
        local_file_path = base_directory / (file_key[10:] if
                                             file_key.startswith("artifacts_")
                                             else file_key)
        local_file_path.parent.mkdir(parents=True, exist_ok=True)
        s3_client.download_file(bucket_name, file_key, str(local_file_path))
        logger.info("Downloaded and saved %s to %s", file_key, local_file_path)
