import os
import logging
import boto3
logger = logging.getLogger(__name__)

"""
session = boto3.session.Session(profile_name = 'zzp8676')

s3 = session.client("s3")
print(s3.list_buckets().get("Buckets", []))


def upload_files_to_s3(bucket_name: str, prefix: str, directory: Path) -> bool:
    # Check for AWS credentials
    try:
        session = boto3.Session()
        s3 = session.client("s3")
    except Exception as e:
        logger.error("Failed to create boto3 session: %s", e)
        return False

    # Check if bucket exists
    try:
        s3.head_bucket(Bucket=bucket_name)
    except Exception as e:
        logger.error(
            "The bucket '%s' does not exist or you do not have permission to access it",
            bucket_name,
        )
        logger.error(e)
        return False

    # Iterate through files in directory and upload to S3
    for file_path in directory.glob("*"):
        if file_path.is_file():
            try:
                key = str(
                    Path(prefix) / Path(file_path.name)
                )  # Use prefix instead of file parent
                with file_path.open("rb") as data:
                    s3.upload_fileobj(data, bucket_name, key)
                logger.debug(
                    "File '%s' uploaded to S3 bucket '%s'", file_path.name, bucket_name
                )
            except Exception as e:
                logger.error("Failed to upload file '%s': %s", file_path.name, e)
                return False

    return True
"""
def upload_directory_to_s3(bucket_name, local_directory, root_directory):
    """
    Uploads a directory to an S3 bucket, preserving the folder structure.

    :param bucket_name: str. Name of the S3 bucket.
    :param local_directory: str. The local directory to upload.
    :param root_directory: str. Root directory prefix to be added in S3.
    """
    s3_client = boto3.client('s3')
    # Walk through the local directory
    for root, _, files in os.walk(local_directory):
        for filename in files:
            # construct the full local path
            local_path = os.path.join(root, filename)
            # construct the full S3 path
            relative_path = os.path.relpath(local_path, local_directory)
            s3_path = os.path.join(root_directory, relative_path)
            logger.info("Uploading %s to %s/%s", local_path, bucket_name, s3_path)
            # Perform the upload
            s3_client.upload_file(local_path, bucket_name, s3_path)
    return True