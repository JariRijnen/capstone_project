from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class LocalToS3:
    """Small helper function."""
    def _local_to_s3(filename, key, bucket_name):
        s3 = S3Hook()
        s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)
