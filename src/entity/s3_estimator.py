import sys
from src.cloud_storage.aws_storage import SimpleStorageService
from src.entity.estimator import MyModel
from src.exception import MyException
from src.logger import logging
from pandas import DataFrame


class Proj1Estimator:
    """
    Handles loading a model from S3 and performing predictions.
    """

    def __init__(self, bucket_name: str, model_path: str):
        """
        :param bucket_name: S3 bucket name
        :param model_path: File path of model inside S3 bucket
        """
        try:
            self.bucket_name = bucket_name
            self.model_path = model_path
            self.s3 = SimpleStorageService()
            self.loaded_model: MyModel = None
        except Exception as e:
            raise MyException(e, sys)

    def is_model_present(self, model_path: str) -> bool:
        """
        Check if model exists in S3.
        """
        try:
            exists = self.s3.s3_key_path_available(
                bucket_name=self.bucket_name,
                s3_key=model_path
            )

            if not exists:
                logging.error(f"Model not found in bucket '{self.bucket_name}' at '{model_path}'")

            return exists

        except Exception as e:
            raise MyException(e, sys)

    def load_model(self) -> MyModel:
        """
        Load the model from S3.
        """
        try:
            logging.info(f"Loading model from s3://{self.bucket_name}/{self.model_path}")

            model = self.s3.load_model(
                model_name=self.model_path,
                bucket_name=self.bucket_name
            )

            logging.info("Model successfully loaded.")
            return model

        except Exception as e:
            raise MyException(e, sys)

    def save_model(self, from_file: str, remove: bool = False) -> None:
        """
        Upload local model to S3.
        """
        try:
            logging.info(f"Uploading model to s3://{self.bucket_name}/{self.model_path}")

            self.s3.upload_file(
                from_filename=from_file,
                to_filename=self.model_path,
                bucket_name=self.bucket_name,
                remove=remove
            )

            logging.info("Model successfully uploaded.")

        except Exception as e:
            raise MyException(e, sys)

    def predict(self, dataframe: DataFrame):
        """
        Perform prediction using the loaded model.
        """
        try:
            logging.info("Entered predict method of Proj1Estimator")

            # Ensure model is loaded
            if self.loaded_model is None:
                logging.info("Model not loaded yet → loading from S3...")
                self.loaded_model = self.load_model()

            # RUN PREDICTION
            result = self.loaded_model.predict(dataframe=dataframe)

            logging.info("Prediction completed successfully.")
            return result

        except Exception as e:
            raise MyException(e, sys)
