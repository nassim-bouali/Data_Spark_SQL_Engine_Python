import logging
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from typing import Optional, Dict, List
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AccessToken
from azure.storage.blob import generate_container_sas, ContainerSasPermissions
from datetime import datetime, timedelta

from models.models import CsvStorage, ParquetStorage, JdbcStorage


class InputReader(ABC):
    @abstractmethod
    def read_input(self, input):
        pass

    @abstractmethod
    def is_compatible(self, input):
        pass


class CsvReader(InputReader):
    def __init__(self, spark_session):
        self.spark_session = spark_session

    def read_input(self, input):
        if not isinstance(input, CsvStorage):
            raise ValueError("Input must be an instance of CsvStorage")
        csv_input = input
        self.spark_session.read.options(**csv_input.options).csv(csv_input.get_absolute_path()).createOrReplaceTempView(
            csv_input.id)

    def is_compatible(self, input):
        return isinstance(input, CsvStorage)


class ParquetReader(InputReader):
    def __init__(self, spark_session):
        self.spark_session = spark_session

    def read_input(self, input):
        if not isinstance(input, ParquetStorage):
            raise ValueError("Input must be an instance of ParquetStorage")
        parquet_input = input
        self.spark_session.read.options(**parquet_input.options).parquet(
            parquet_input.get_absolute_path()).createOrReplaceTempView(parquet_input.id)

    def is_compatible(self, input):
        return isinstance(input, ParquetStorage)


class JdbcReader(InputReader):
    def __init__(self, spark_session):
        self.spark_session = spark_session

    def read_input(self, input):
        if not isinstance(input, JdbcStorage):
            raise ValueError("Input must be an instance of JdbcStorage")
        jdbc_input = input
        (self.spark_session.read
         .options(**jdbc_input.options)
         .jdbc(jdbc_input.table
               , jdbc_input.uri
               , get_properties(jdbc_input.options))
         .createOrReplaceTempView(jdbc_input.id))

    def is_compatible(self, input):
        return isinstance(input, JdbcStorage)


def get_properties(options: Optional[Dict[str, str]]) -> Dict[str, str]:
    connection_props = {"loginTimeout": "300"}
    if options is None or "password" not in options:
        connection_props.update({
            "user": "",
            "password": "",
            "accessToken": get_jdbc_access_token()
        })
    else:
        connection_props.update(options)
    return connection_props


def get_jdbc_access_token() -> str:
    credential = DefaultAzureCredential()

    # Specify the token request context
    request_context = ["https://database.azure.net/.default"]

    # Obtain the access token asynchronously
    token = credential.get_token(*request_context)

    access_token = token.token
    print("Access Token:", access_token)

    return access_token


class Transformer(ABC):
    @abstractmethod
    def transform(self, transformation):
        pass


class SqlTransformer(Transformer):
    def __init__(self, spark_session):
        self.spark_session = spark_session

    def transform(self, transformation):
        logging.debug(f"Applying transformation: {transformation}")
        self.spark_session.sql(transformation.sql).createOrReplaceTempView(transformation.id)
        logging.debug("Transformation applied")


class OutputWriter(ABC):
    @abstractmethod
    def write_output(self, target):
        pass

    @abstractmethod
    def is_compatible(self, target):
        pass


class CsvWriter(OutputWriter):
    def __init__(self, spark_session):
        self.spark_session = spark_session

    def write_output(self, target):
        if not isinstance(target.output, CsvStorage):
            raise ValueError("Output must be an instance of CsvStorage")

        self.spark_session.sql(target.sql()).write.options(**target.output.options).mode("append").csv(target.output.get_absolute_path())

    def is_compatible(self, target):
        return isinstance(target.output, CsvStorage)


class ParquetWriter(OutputWriter):
    def __init__(self, spark_session):
        self.spark_session = spark_session

    def write_output(self, target):
        if not isinstance(target.output, ParquetStorage):
            raise ValueError("Output must be an instance of ParquetStorage")

        self.spark_session.sql(target.sql()).write.parquet(target.output.get_absolute_path, **target.output.options,
                                                           mode="append")

    def is_compatible(self, target):
        return isinstance(target.output, ParquetStorage)


class JdbcWriter(OutputWriter):
    def __init__(self, spark_session):
        self.spark_session = spark_session

    def write_output(self, target):
        if not isinstance(target.output, JdbcStorage):
            raise ValueError("Output must be an instance of JdbcStorage")

        self.spark_session.sql(target.sql()).write.jdbc(url=target.output.uri, table=target.output.table,
                                                        properties=get_properties(target.output.options),
                                                        mode="overwrite")

    def is_compatible(self, target):
        return isinstance(target.output, JdbcStorage)


class InputProcessor:
    input_readers: List['InputReader']

    def __init__(self, input_readers):
        self.input_readers = input_readers

    def process_input(self, input):
        logging.debug(f"Processing input: {input}")
        input_reader = next((reader for reader in self.input_readers if reader.is_compatible(input)), None)
        if input_reader:
            logging.debug("Reading input...")
            input_reader.read_input(input)
            logging.debug("Input read successfully.")
        else:
            raise ValueError("Input type still not implemented yet " + input)


class OutputProcessor:
    output_writers: List['OutputWriter']

    def __init__(self, output_writers):
        self.output_writers = output_writers

    def process_output(self, target):
        logging.debug(f"Processing output: {target}")
        output_writer = next((writer for writer in self.output_writers if writer.is_compatible(target)), None)
        if output_writer:
            logging.debug("Writing output...")
            output_writer.write_output(target)
            logging.debug("Output written successfully.")
        else:
            raise ValueError("Input type still not implemented yet " + target)


class DataProcessor(ABC):
    @abstractmethod
    def process(self, batch_plan):
        pass


class BatchDataProcessor(DataProcessor):
    def __init__(self, input_processor, output_processor, transformer):
        self.input_processor = input_processor
        self.output_processor = output_processor
        self.transformer = transformer

    def process(self, batch_plan):
        logging.debug(f"Started processing inputs list: {batch_plan.inputs}")
        for input in batch_plan.inputs:
            self.input_processor.process_input(input)
            logging.debug("Finished processing inputs list")

        logging.debug(f"Started applying transformations: {batch_plan.transformations}")
        for transformation in batch_plan.transformations:
            self.transformer.transform(transformation)
            logging.debug("Ended applying transformations")

        logging.debug(f"Started processing targets list: {batch_plan.targets}")
        for target in batch_plan.targets:
            self.output_processor.process_output(target)
        logging.debug("Finished processing outputs list")


def update_hadoop_configuration(batch_plan, configuration):
    for file_storage in filter(
            lambda storage: hasattr(storage, "storage_account") and storage.storage_account is not None,
            batch_plan.inputs):
        configuration.set(f"fs.azure.sas.{file_storage.container}.{file_storage.storage_account}.blob.core.windows.net", get_sas_token(file_storage))

    for file_storage in filter(lambda storage: hasattr(storage, "storage_account") and storage.storage_account is not None, map(lambda target: target.output, batch_plan.targets)):
        configuration.set(f"fs.azure.sas.{file_storage.container}.{file_storage.storage_account}.blob.core.windows.net", get_sas_token(file_storage))


def get_sas_token(file_storage):
    return file_storage.sas_token if file_storage.sas_token is not None else generate_sas_token(file_storage)


def generate_sas_token(file_storage):
    return generate_container_sas(
        account_name=file_storage.storage_account,
        container_name=file_storage.container,
        account_key=None,
        permission=ContainerSasPermissions(read=True, write=True, delete=True),
        expiry=datetime.utcnow() + timedelta(hours=1)
    )
