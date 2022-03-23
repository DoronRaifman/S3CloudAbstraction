from unittest import TestCase
from Core.BaseAlgObject import BaseAlgObject
from Core.S3Base import S3Base


class TestS3(TestCase):
    def test1(self):
        cloud = BaseAlgObject.config['General']['cloud']
        print(f'Using cloud {cloud}')

        s3 = S3Base.get_instance()
        s3.connect()

        bucket = 'aqs-prod4-test'
        print(f'uploading files to bucket {bucket}')
        s3.upload_file(bucket, 'S3Azure.py', 'src_files/S3Azure.py')
        s3.upload_file(bucket, 'S3Azure.py', 'src_files/S3Azure2.py')
        s3.upload_file(bucket, 'S3Azure.py', 'src_files/S3Azure3.py')
        s3.upload_file(bucket, 'S3Azure.py', 'src_files/test/S3Azure.py')
        s3.upload_file(bucket, 'S3Azure.py', 'src_files/test/S3Azure2.py')

        prefix = 'src_files/'
        result = s3.list_objects(bucket, prefix)
        print(f'list prefix {prefix}')
        for res in result:
            print(f"\t{res}")

        prefix = 'src_files/test/'
        result = s3.list_objects(bucket, prefix)
        print(f'list prefix {prefix}')
        for res in result:
            print(f"\t{res}")

        s3.disconnect()

