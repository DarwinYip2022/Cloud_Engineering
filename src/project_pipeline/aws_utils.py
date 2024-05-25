import os
import logging
import boto3
from pathlib import Path
logger = logging.getLogger(__name__)


def upload_artifacts(access_key, secret_key, region, artifacts: Path, config: dict) -> list:
    """Upload all the artifacts in the specified directory to S3
    Args:
        artifacts: Directory containing all the artifacts from a given experiment
        config: Config required to upload artifacts to S3; see example config file for structure
    Returns:
        List of S3 URIs for each file that was uploaded
    """

    # Create a Boto3 session using AWS credentials from the default profile
    session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name= region
    )

    # Explicitly specify the use of standard AWS credentials by setting use_ssl=False
    s3_client = session.client("s3", use_ssl=False)
    bucket_name = config["bucket_name"]
    prefix = config["prefix"]
    print(bucket_name)
    s3_uris = []

    # Function to upload files recursively
    def upload_files(directory, prefix):
        for file_path in directory.iterdir():
            if file_path.is_file():
                # Extract the directory name (experiment ID) from the path
                experiment_id = directory.stem
                s3_key = f"{prefix}_{experiment_id}/{file_path.name}"
                print(s3_key)
                try:
                    s3_client.upload_file(str(file_path), bucket_name, s3_key)
                except FileNotFoundError:
                    pass
                except OSError:
                    pass
                else:
                    s3_uri = f"s3://{bucket_name}/{s3_key}"
                    s3_uris.append(s3_uri)
            elif file_path.is_dir():
                upload_files(file_path, prefix)

    upload_files(artifacts, prefix)
    
    return s3_uris

def load_from_s3(access_key, secret_key, region, bucket_name, target_directories):
    """
    Downloads directories from specified prefixes within an S3 bucket to a local 'artifacts' folder,
    stripping the 'artifacts_' prefix from the directory names.

    Parameters:
        access_key (str): AWS access key ID.
        secret_key (str): AWS secret access key.
        region (str): AWS region.
        bucket_name (str): Name of the S3 bucket.
        target_directories (list): List of directory prefixes to include in the download.
    """
    try:
        # Set up the logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger()

        # Create a session with AWS
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        s3_client = session.client("s3")

        # Base local directory
        base_directory = Path("s3_artifacts")

        # Download each specified directory and its contents
        for prefix in target_directories:
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        file_key = obj['Key']
                        if not file_key.endswith("/"):  # Skip directories
                            # Remove the 'artifacts_' prefix from the S3 key
                            local_key = file_key[10:] if file_key.startswith("artifacts_") else file_key
                            local_file_path = base_directory / local_key
                            local_file_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure directory exists

                            # Download the file from S3
                            s3_client.download_file(bucket_name, file_key, str(local_file_path))
                            logger.info("Downloaded and saved %s to %s", file_key, local_file_path)

        logger.info("All relevant files downloaded from S3")

    except Exception as e:
        logger.error("Error accessing S3 bucket: %s", e)


