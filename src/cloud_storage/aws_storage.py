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
        """
        Initializes the SimpleStorageService instance with S3 resource and client.
        """
        s3_client = S3Client()
        self.s3_resource = s3_client.s3_resource
        self.s3_client = s3_client.s3_client

    def s3_key_path_available(self, bucket_name, s3_key) -> bool:
        """
        Checks if a specified S3 key path (file path) is available.
        """
        try:
            bucket = self.get_bucket(bucket_name)
            file_objects = [obj for obj in bucket.objects.filter(Prefix=s3_key)]
            return len(file_objects) > 0
        except Exception as e:
            raise MyException(e, sys)

    @staticmethod
    def read_object(object_name, decode: bool = True, make_readable: bool = False) -> Union[StringIO, str]:
        """
        Reads the specified S3 object content. Expects an object that supports .get()
        (i.e. boto3 s3.Object).
        """
        try:
            # If user accidentally passed an ObjectSummary or list, normalize before use is recommended
            func = (
                lambda: object_name.get()["Body"].read().decode()
                if decode else object_name.get()["Body"].read()
            )
            conv_func = lambda: StringIO(func()) if make_readable else func()
            return conv_func()
        except Exception as e:
            raise MyException(e, sys)

    def get_bucket(self, bucket_name: str) -> Bucket:
        """
        Retrieves the S3 bucket object.
        """
        logging.info("Entered get_bucket")
        try:
            bucket = self.s3_resource.Bucket(bucket_name)
            logging.info("Exited get_bucket")
            return bucket
        except Exception as e:
            raise MyException(e, sys)

    def get_file_object(self, filename: str, bucket_name: str):
        """
        Retrieve a single boto3 s3.Object corresponding to the first match for the given prefix.
        Always returns an s3.Object (has .get()), never a list or ObjectSummary.
        """
        logging.info("Entered get_file_object")

        try:
            bucket = self.get_bucket(bucket_name)
            summaries = [obj for obj in bucket.objects.filter(Prefix=filename)]

            if not summaries:
                raise MyException(f"File '{filename}' not found in bucket '{bucket_name}'", sys)

            # Use the first summary's key to create a full s3.Object (this object supports .get())
            key = summaries[0].key
            s3_object = self.s3_resource.Object(bucket_name, key)

            logging.info(f"Found S3 object: s3://{bucket_name}/{key}")
            logging.info("Exited get_file_object")
            return s3_object

        except Exception as e:
            raise MyException(e, sys)

    def load_model(self, model_name: str, bucket_name: str, model_dir: str = None):
        """
        Loads a serialized model stored in S3 using pickle.
        """
        try:
            model_file = model_dir + "/" + model_name if model_dir else model_name
            # get_file_object now returns an s3.Object (has .get())
            file_object = self.get_file_object(model_file, bucket_name)
            model_binary = self.read_object(file_object, decode=False)
            model = pickle.loads(model_binary)
            logging.info("Model loaded from S3.")
            return model
        except Exception as e:
            raise MyException(e, sys)

    def create_folder(self, folder_name: str, bucket_name: str):
        """
        Creates a folder in an S3 bucket.
        """
        logging.info("Entered create_folder")

        try:
            self.s3_resource.Object(bucket_name, folder_name).load()
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self.s3_client.put_object(Bucket=bucket_name, Key=f"{folder_name}/")

        logging.info("Exited create_folder")

    def upload_file(self, from_filename: str, to_filename: str, bucket_name: str, remove: bool = True):
        """
        Uploads a file to S3.
        """
        logging.info("Entered upload_file")

        try:
            self.s3_resource.meta.client.upload_file(from_filename, bucket_name, to_filename)

            if remove:
                os.remove(from_filename)

            logging.info("Exited upload_file")
        except Exception as e:
            raise MyException(e, sys)

    def upload_df_as_csv(self, data_frame: DataFrame, local_filename: str, bucket_filename: str, bucket_name: str):
        """
        Saves DataFrame as CSV and uploads to S3.
        """
        logging.info("Entered upload_df_as_csv")

        try:
            data_frame.to_csv(local_filename, index=None, header=True)
            self.upload_file(local_filename, bucket_filename, bucket_name)
            logging.info("Exited upload_df_as_csv")
        except Exception as e:
            raise MyException(e, sys)

    def get_df_from_object(self, object_) -> DataFrame:
        """
        Converts S3 object CSV → DataFrame.
        """
        logging.info("Entered get_df_from_object")

        try:
            content = self.read_object(object_, make_readable=True)
            df = read_csv(content, na_values="na")
            logging.info("Exited get_df_from_object")
            return df
        except Exception as e:
            raise MyException(e, sys)

    def read_csv(self, filename: str, bucket_name: str) -> DataFrame:
        """
        Reads a CSV from S3.
        """
        logging.info("Entered read_csv")

        try:
            csv_obj = self.get_file_object(filename, bucket_name)
            df = self.get_df_from_object(csv_obj)
            logging.info("Exited read_csv")
            return df
        except Exception as e:
            raise MyException(e, sys)
