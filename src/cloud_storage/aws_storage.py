import boto3
from src.configuration.aws_connection import S3Client
from io import StringIO
from typing import Union, List
import os, sys
from src.logger import logging
from mypy_boto3_s3.service_resource import Bucket
from src.exception import MyException
from botocore.exceptions import ClientError
from pandas import DataFrame, read_csv
import pickle


class SimpleStorageService:
    """
    A class for interacting with AWS S3 storage, providing methods for file management,
    data uploads, and data retrieval in S3 buckets.
    """

    def __init__(self):
        s3_client = S3Client()
        self.s3_resource = s3_client.s3_resource
        self.s3_client = s3_client.s3_client

    def s3_key_path_available(self, bucket_name, s3_key) -> bool:
        try:
            bucket = self.get_bucket(bucket_name)
            file_objects = [obj for obj in bucket.objects.filter(Prefix=s3_key)]
            return len(file_objects) > 0
        except Exception as e:
            raise MyException(e, sys)

    @staticmethod
    def read_object(object_name, decode: bool = True, make_readable: bool = False) -> Union[StringIO, str]:
        """
        Read an S3 object.
        """
        try:
            func = (
                lambda: object_name.get()["Body"].read().decode()
                if decode else object_name.get()["Body"].read()
            )
            conv_func = lambda: StringIO(func()) if make_readable else func()
            return conv_func()
        except Exception as e:
            raise MyException(e, sys)

    def get_bucket(self, bucket_name: str) -> Bucket:
        try:
            return self.s3_resource.Bucket(bucket_name)
        except Exception as e:
            raise MyException(e, sys)

    def get_file_object(self, filename: str, bucket_name: str):
        """
        FIXED: Always return exactly ONE S3 object.
        """
        logging.info("Entered get_file_object method")
        try:
            bucket = self.get_bucket(bucket_name)
            files = [obj for obj in bucket.objects.filter(Prefix=filename)]

            if len(files) == 0:
                raise MyException(f"No file found in bucket '{bucket_name}' with prefix '{filename}'", sys)

            # Always return the first match
            file_obj = files[0]

            logging.info(f"Found S3 object: {file_obj.key}")
            return file_obj

        except Exception as e:
            raise MyException(e, sys)

    def load_model(self, model_name: str, bucket_name: str, model_dir: str = None) -> object:
        try:
            model_key = f"{model_dir}/{model_name}" if model_dir else model_name
            file_object = self.get_file_object(model_key, bucket_name)

            model_bytes = self.read_object(file_object, decode=False)
            model = pickle.loads(model_bytes)

            logging.info("Production model successfully loaded from S3.")
            return model

        except Exception as e:
            raise MyException(e, sys)

    def create_folder(self, folder_name: str, bucket_name: str) -> None:
        try:
            self.s3_resource.Object(bucket_name, folder_name).load()
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self.s3_client.put_object(Bucket=bucket_name, Key=folder_name + "/")

    def upload_file(self, from_filename: str, to_filename: str, bucket_name: str, remove: bool = True):
        try:
            logging.info(f"Uploading {from_filename} → s3://{bucket_name}/{to_filename}")
            self.s3_resource.meta.client.upload_file(from_filename, bucket_name, to_filename)

            if remove:
                os.remove(from_filename)

        except Exception as e:
            raise MyException(e, sys)

    def upload_df_as_csv(self, data_frame: DataFrame, local_filename: str, bucket_filename: str, bucket_name: str):
        try:
            data_frame.to_csv(local_filename, index=False)
            self.upload_file(local_filename, bucket_filename, bucket_name)
        except Exception as e:
            raise MyException(e, sys)

    def get_df_from_object(self, object_: object) -> DataFrame:
        try:
            content = self.read_object(object_, make_readable=True)
            return read_csv(content, na_values="na")
        except Exception as e:
            raise MyException(e, sys)

    def read_csv(self, filename: str, bucket_name: str) -> DataFrame:
        try:
            csv_obj = self.get_file_object(filename, bucket_name)
            return self.get_df_from_object(csv_obj)
        except Exception as e:
            raise MyException(e, sys)
