"""
upload.py
upload data to InfluxDB
"""

from csv import DictReader
import reactivex as rx
from reactivex import operators as ops

import logging
import pandas as pd
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write.retry import WritesRetry
from influxdb_client.client.write_api import SYNCHRONOUS

from database_manage.utils import local2utc


def _dataframe_to_generator(dataframe: pd.DataFrame, measurement, tag, height_or_length):
    """
    Parse DataFrame into generator
    """
    dataframe["start_time"] = local2utc(dataframe["start_time"])
    dataframe["end_time"] = local2utc(dataframe["end_time"])
    for idx, row in dataframe.iterrows():
        dict_structure = {
            "measurement": measurement,
            "tags": {tag: height_or_length},
            "fields": row.iloc[2:].to_dict(),
            "time": row["start_time"],
        }
        pt = Point.from_dict(dict_structure)
        yield pt


retries = WritesRetry(total=3, retry_interval=1, exponential_base=2)


def write_file(url, token, org, bucket,
               dataframe, measurement, tag, height_or_length, logger: logging.Logger,
               retries=retries, batch_size=500):
    with InfluxDBClient(url=url, token=token, org=org, retries=retries) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        batches = (
            rx.from_iterable(_dataframe_to_generator(
                dataframe=dataframe,
                measurement=measurement,
                tag=tag,
                height_or_length=height_or_length,
            ))
            .pipe(ops.buffer_with_count(batch_size))
        )

        batches.subscribe(
            on_next=lambda batch: write_api.write(bucket=bucket, record=batch),
            on_error=lambda ex: logger.error(f"Unexpected error: {ex}"),
            on_completed=lambda: logger.info("Import finished!"),
        )
