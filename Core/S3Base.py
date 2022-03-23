import io
from typing import Union, Type
from Core.BaseAlgObject import BaseAlgObject


class S3Exception(Exception):
    pass


class S3Base:
    logger = BaseAlgObject.logger

    @classmethod
    def connect(cls) -> None:
        raise NotImplementedError

    @classmethod
    def disconnect(cls) -> None:
        raise NotImplementedError

    @classmethod
    def upload_file(cls, bucket: str, local_file_name: str, key: str) -> None:
        raise NotImplementedError

    @classmethod
    def upload_bytes(cls, file_like, bucket: str, key: str) -> None:
        raise NotImplementedError

    @classmethod
    def download_file(cls, bucket: str, key: str, local_file_name: str) -> None:
        raise NotImplementedError

    @classmethod
    def download_bytes(cls, file_like, bucket: str, key: str) -> None:
        raise NotImplementedError

    @classmethod
    def get_file(cls, bucket: str, key: str) -> io.BytesIO:
        raise NotImplementedError

    @classmethod
    def delete_file(cls, bucket: str, key: str) -> None:
        raise NotImplementedError

    @classmethod
    def list_objects(cls, bucket: str, key_filter: str) -> list:
        raise NotImplementedError

    @classmethod
    def is_exist(cls, bucket: str, key: str) -> bool:
        raise NotImplementedError

    # ===================
    # base class implementation
    # ===================

    @classmethod
    def move_delete_file(cls, bucket_source: str, key_source: str,
                         bucket_destination: str, key_destination: str) -> None:
        blob = cls.get_file(bucket_source, key_source)
        cls.upload_bytes(blob, bucket_destination, key_destination)
        cls.delete_file(bucket_source, key_source)
        blob.close()

    @classmethod
    def copy_file(cls, bucket_source: str, key_source: str,
                  bucket_destination: str, key_destination: str) -> None:
        blob = cls.get_file(bucket_source, key_source)
        cls.upload_bytes(blob, bucket_destination, key_destination)
        blob.close()

    @staticmethod
    def get_instance():
        from Core.S3AWS import S3AWS
        from Core.S3Azure import S3Azure

        cloud = BaseAlgObject.config['General']['cloud']
        s3_instance: Type[Union[S3AWS, S3Azure]] = S3AWS if cloud == 'AWS' else S3Azure
        return s3_instance
