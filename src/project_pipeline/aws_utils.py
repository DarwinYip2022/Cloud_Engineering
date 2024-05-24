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


def load_from_s3(access_key, secret_key, region, bucket_name):
    """
    Lists the contents of an S3 bucket and downloads files based on provided credentials.
    """
    try:
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        s3_client = session.client("s3")
        
        # List objects in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                file_key = obj['Key']
                logger.info("Attempting to download file from S3 key: %s", file_key)
                
                # Determine the local directory based on file extension
                if file_key.endswith("/"):  # Skip directories
                    continue
                elif file_key.endswith(".pkl"):
                    local_directory = "models/"
                elif file_key.endswith(".csv"):
                    local_directory = "data/"
                else:
                    local_directory = "./"  # Default to current directory if the file type is not recognized
                
                # Ensure the local directory exists
                os.makedirs(local_directory, exist_ok=True)
                
                try:
                    # Determine the local file path based on the file type
                    file_name = file_key.split("/")[-1]
                    local_file_name = os.path.join(local_directory, file_name)
                    
                    # Download the file from S3
                    s3_client.download_file(bucket_name, file_key, local_file_name)
                    
                    logger.info("Downloaded and saved %s to %s", file_key, local_file_name)
                except Exception as e:
                    logger.error("Error downloading file %s from S3: %s", file_key, e)
        
        logger.info("All files downloaded from S3")
        
    except Exception as e:
        logger.error("Error accessing S3 bucket: %s", e)