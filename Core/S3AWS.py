import io
import boto3
from botocore.exceptions import ClientError
from Core.BaseAlgObject import BaseAlgObject
from Core.S3Base import S3Base, S3Exception


class S3AWS(S3Base):
    s3_client: boto3.client = None
    s3_resource = None

    @classmethod
    def connect(cls) -> None:
        try:
            cls.s3_client: boto3.client = boto3.client('s3')
            cls.s3_resource = boto3.resource('s3')
        except Exception as ex:
            line = f'connect exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)

    @classmethod
    def disconnect(cls) -> None:
        import warnings
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        cls.s3_resource = None
        cls.s3_client = None

    @classmethod
    def upload_file(cls, bucket: str, local_file_name: str, key: str) -> None:
        try:
            response = cls.s3_client.upload_file(local_file_name, bucket, key)
        except Exception as ex:
            line = f'upload_file {bucket} {key} exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)

    @classmethod
    def upload_bytes(cls, file_like: io.BytesIO, bucket: str, key: str) -> None:
        try:
            cls.s3_client.put_object(Body=file_like, Bucket=bucket, Key=key)
        except ClientError as ex:
            line = f'upload_bytes {bucket} {key} ClientError exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)
        except IOError as ex:
            line = f'upload_bytes {bucket} {key} IOError exception: {ex}'
            BaseAlgObject.logger.error(line)
            raise S3Exception(line)

    @classmethod
    def download_file(cls, bucket: str, key: str, local_file_name: str) -> None:
        try:
            cls.s3_client.download_file(bucket, key, local_file_name)
        except ClientError as ex:
            line = f'download_file {bucket} {key} exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)

    @classmethod
    def download_bytes(cls, file_like: io.BytesIO, bucket: str, key: str) -> None:
        try:
            cls.s3_client.download_fileobj(Bucket=bucket, Key=key, Fileobj=file_like)
            file_like.seek(0)
        except Exception as ex:
            line = f'download_bytes {bucket} {key} exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)

    @classmethod
    def get_file(cls, bucket: str, key: str) -> io.BytesIO:
        try:
            response_object = cls.s3_client.get_object(Bucket=bucket, Key=key)
            response = response_object['Body'].read()
        except ClientError as ex:
            line = f'get_file {bucket} {key} ClientError exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)
        except Exception as ex:
            line = f'get_file {bucket} {key} exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)
        return response

    @classmethod
    def delete_file(cls, bucket: str, key: str) -> None:
        try:
            cls.s3_resource.Object(bucket, key).delete()
        except ClientError as ex:
            line = f'delete_file {bucket} {key} exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)

    @classmethod
    def list_objects(cls, bucket: str, key_filter: str) -> list:
        s3_objects = []
        try:
            paginator = cls.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=key_filter)
            for page in pages:
                objects_list = page['Contents'] if 'Contents' in page else None
                if objects_list is not None:
                    s3_objects_page = [i['Key'] for i in objects_list]
                    s3_objects += s3_objects_page
        except ClientError as ex:
            line = f'list_objects {bucket} {key_filter} exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)
        return s3_objects

    @classmethod
    def is_exist(cls, bucket: str, key: str) -> bool:
        is_exist = True
        try:
            cls.s3_resource.Object(bucket, key).load()
        except ClientError as ex:
            is_exist = False
            if ex.response['Error']['Code'] != "404":
                line = f'is_exist {bucket} {key} exception: {ex}'
                cls.logger.error(line)
                raise S3Exception(line)
        return is_exist
