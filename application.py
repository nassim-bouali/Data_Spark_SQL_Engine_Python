#!/usr/bin/env python
import sys
print(sys.executable)

import argparse
import logging

from pyspark.sql import SparkSession
import json
import base64

from models.models import BatchPlan
from services.services import update_hadoop_configuration, InputProcessor, CsvReader, ParquetReader, JdbcReader, \
    OutputProcessor, CsvWriter, ParquetWriter, JdbcWriter, SqlTransformer, BatchDataProcessor
from config import configure_logging

# Set Py4J logger to ERROR level
logging.getLogger("py4j").setLevel(logging.ERROR)


def main():
    configure_logging()

    parser = argparse.ArgumentParser(description="My Script Description")

    parser.add_argument("--app-name", dest="appName", default="Spark Batch Processing Engine",
                        help="Application description")
    parser.add_argument("--is-local", dest="isLocal", action="store_false", help="Determine if application is run "
                                                                                 "locally")
    parser.add_argument("--inline", dest="inline", action="store_false",
                        help="Determine if application ingestion plan is set inline")
    parser.add_argument("--plan", dest="plan", help="Application plan")

    args = parser.parse_args()

    print(f"App Name: {args.appName}")
    print(f"Islocal: {args.isLocal}")
    print(f"Inline: {args.inline}")
    print(f"Plan: {args.plan}")

    if args.inline:
        batch_plan = BatchPlan.from_dict(json.loads(base64.b64decode(args.plan).decode("utf-8")))
    else:
        with open(args.plan, 'r') as file:
            batch_plan_dict = json.load(file)
            print(batch_plan_dict)
            inputs = batch_plan_dict.get('inputs', [])
            print(inputs)
            batch_plan = BatchPlan.from_dict(batch_plan_dict)

    logging.debug(f"Application batch plan: {batch_plan}")

    jdbc_driver_path = "resources/lib/postgresql-42.7.1.jar"

    spark = SparkSession.builder.appName(args.appName)
    if args.isLocal:
        spark = spark.master("local[*]")
    spark_session = spark.config("spark.jars", jdbc_driver_path).getOrCreate()

    configuration = spark_session.conf
    update_hadoop_configuration(batch_plan, configuration)

    input_processor = InputProcessor([CsvReader(spark_session), ParquetReader(spark_session), JdbcReader(spark_session)])
    output_processor = OutputProcessor([CsvWriter(spark_session), ParquetWriter(spark_session), JdbcWriter(spark_session)])
    transformer = SqlTransformer(spark_session)

    batch_processor = BatchDataProcessor(input_processor, output_processor, transformer)
    batch_processor.process(batch_plan)


if __name__ == "__main__":
    main()
