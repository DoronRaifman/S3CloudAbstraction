import io
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from Core.BaseAlgObject import BaseAlgObject
from Core.S3Base import S3Base, S3Exception


class S3Azure(S3Base):
    connect_str = BaseAlgObject.config['Azure']['connect_str']
    blob_service_client: BlobServiceClient = None

    @classmethod
    def connect(cls) -> None:
        try:
            cls.blob_service_client = BlobServiceClient.from_connection_string(cls.connect_str)
        except Exception as ex:
            line = f'connect exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)

    @classmethod
    def disconnect(cls) -> None:
        try:
            cls.blob_service_client.close()
            cls.blob_service_client = None
        except Exception as ex:
            line = f'disconnect exception: {ex}'
            cls.logger.error(line)
            cls.blob_service_client = None
            raise S3Exception(line)

    @classmethod
    def upload_file(cls, bucket: str, local_file_name: str, key: str) -> None:
        try:
            if cls.is_exist(bucket, key):
                cls.delete_file(bucket, key)
            blob_client = cls.blob_service_client.get_blob_client(container=bucket, blob=key)
            with open(local_file_name, "rb") as data:
                blob_client.upload_blob(data)
        except Exception as ex:
            line = f'upload_file {bucket} {key} exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)

    @classmethod
    def upload_bytes(cls, file_like: io.BytesIO, bucket: str, key: str) -> None:
        try:
            if cls.is_exist(bucket, key):
                cls.delete_file(bucket, key)
            blob_client = cls.blob_service_client.get_blob_client(container=bucket, blob=key)
            blob_client.upload_blob(file_like)
            file_like.close()
        except Exception as ex:
            file_like.close()
            line = f'upload_bytes {bucket} {key} exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(f"@@@ Exception upload_bytes {key}. Ex: {str(ex)}")

    @classmethod
    def download_file(cls, bucket: str, key: str, local_file_name: str) -> None:
        try:
            blob_client = cls.blob_service_client.get_blob_client(container=bucket, blob=key)
            with open(local_file_name, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
        except Exception as ex:
            line = f'download_file {bucket} {key} exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)

    @classmethod
    def download_bytes(cls, file_like: io.BytesIO, bucket: str, key: str) -> None:
        try:
            blob_client = cls.blob_service_client.get_blob_client(container=bucket, blob=key)
            file_like.write(blob_client.download_blob().readall())
            file_like.seek(0)
        except Exception as ex:
            line = f'download_bytes {bucket} {key} exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)

    @classmethod
    def get_file(cls, bucket: str, key: str) -> io.BytesIO:
        try:
            memory_blob = io.BytesIO()
            blob_client = cls.blob_service_client.get_blob_client(container=bucket, blob=key)
            memory_blob.write(blob_client.download_blob().readall())
            memory_blob.seek(0)
        except Exception as ex:
            line = f'get_file {bucket} {key} exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)
        return memory_blob

    @classmethod
    def delete_file(cls, bucket: str, key: str) -> None:
        try:
            container_client = cls.blob_service_client.get_container_client(bucket)
            container_client.delete_blobs(key)
        except Exception as ex:
            line = f'delete_file {bucket} {key} exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)

    @classmethod
    def list_objects(cls, bucket: str, key_filter: str) -> list:
        try:
            blob_client = cls.blob_service_client.get_container_client(container=bucket)
            blob_list = blob_client.list_blobs(name_starts_with=key_filter)
            result = [blob for blob in blob_list]
        except Exception as ex:
            line = f'list_objects {bucket} {key_filter} exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)
        return result

    @classmethod
    def is_exist(cls, bucket: str, key: str) -> bool:
        try:
            blob_client = cls.blob_service_client.get_blob_client(container=bucket, blob=key)
            is_exist = blob_client.exists()
        except Exception as ex:
            line = f'is_exist {bucket} {key} exception: {ex}'
            cls.logger.error(line)
            raise S3Exception(line)
        return is_exist


if __name__ == '__main__':
    cloud = BaseAlgObject.config['General']['cloud']
    print(f'Using cloud {cloud}')

    S3Azure.connect()

    bucket = 'aqs-dev'
    print(f'uploading files to bucket {bucket}')
    S3Azure.upload_file(bucket, 'S3Azure.py', 'src_files/S3Azure.py')
    S3Azure.upload_file(bucket, 'S3Azure.py', 'src_files/S3Azure2.py')
    S3Azure.upload_file(bucket, 'S3Azure.py', 'src_files/S3Azure3.py')
    S3Azure.upload_file(bucket, 'S3Azure.py', 'src_files/test/S3Azure.py')
    S3Azure.upload_file(bucket, 'S3Azure.py', 'src_files/test/S3Azure2.py')

    prefix = 'src_files/'
    result = S3Azure.list_objects(bucket, prefix)
    print(f'list prefix {prefix}')
    for res in result:
        print(f"\t{res['name']}")

    prefix = 'src_files/test/'
    result = S3Azure.list_objects(bucket, prefix)
    print(f'list prefix {prefix}')
    for res in result:
        print(f"\t{res['name']}")

    S3Azure.disconnect()

