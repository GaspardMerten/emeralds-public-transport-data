import enum
import os
import tempfile
from datetime import datetime, timedelta
from typing import List

import minio
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import Table
from pytz import timezone


class FeedType(enum.Enum):
    VEHICLE_POSITION = "VehiclePosition"
    TRIP_UPDATE = "TripUpdate"
    ALERT = "Alert"


def get_available_dates(
        folder: str,
        access_key=os.environ.get("MINIO_ACCESS_KEY"),
        secret_key=os.environ.get("MINIO_SECRET_KEY"),
) -> List[datetime.date]:
    client = minio.Minio(
        "minio-api.apps.emeralds.ari-aidata.eu",
        access_key=access_key,
        secret_key=secret_key,
        secure=True,
    )

    bucket = "public"
    print(f"Fetching available dates from {folder} in bucket {bucket}")
    days_in_cloud = list(client.list_objects(bucket, folder))
    print(f"Found {len(days_in_cloud)} days in cloud")
    days_in_cloud_names = [day.object_name.split("/")[-2] for day in days_in_cloud]

    # Parse to dates
    days_in_cloud_dates = [
        datetime.strptime(day, "%Y-%m-%d").date() for day in days_in_cloud_names if day not in ["individual", "latest"]
    ]

    return days_in_cloud_dates


def default_parse_date(date, file_name):
    start_time = file_name.split("/")[-1].split("_")[0]
    end_time = (
        file_name.split("/")[-1]
        .split("_")[2]
        .replace(".parquet", "")
    )

    date_str = date.strftime("%Y-%m-%d")
    file_start_date = datetime.strptime(
        date_str + "_" + start_time, "%Y-%m-%d_%H-%M-%S"
    )
    file_end_date = datetime.strptime(
        date_str + "_" + end_time, "%Y-%m-%d_%H-%M-%S"
    )

    if file_start_date == file_end_date:
        file_end_date = file_start_date + timedelta(days=1)

    return file_start_date, file_end_date


def fetch_data(
        start_date,
        end_date,
        feed_path: str,
        parse_date=None,
        access_key=os.environ.get("MINIO_ACCESS_KEY"),
        secret_key=os.environ.get("MINIO_SECRET_KEY"),
        timezone_str="Europe/Brussels",
        limit: int=None,
) -> pa.Table:
    parse_date = parse_date or default_parse_date

    client = minio.Minio(
        "minio-api.apps.emeralds.ari-aidata.eu",
        access_key=access_key,
        secret_key=secret_key,
        secure=True,
    )

    time_zone = timezone(timezone_str)

    start_date = time_zone.localize(
        start_date,
    )
    end_date = time_zone.localize(
        end_date,
    )
    bucket = "public"
    days_of_request = []
    current_date = start_date
    while current_date < end_date:
        days_of_request.append(current_date.strftime("%Y-%m-%d"))
        current_date = current_date + timedelta(days=1)
    service_path = feed_path
    t = datetime.now()

    days_in_cloud = list(client.list_objects(bucket, service_path))
    days_in_cloud_names = [day.object_name.split("/")[-2] for day in days_in_cloud]

    with tempfile.TemporaryDirectory() as tmpdir:
        for day in days_of_request:
            if day in days_in_cloud_names:
                day_path = service_path + day + "/"
                for file in client.list_objects(bucket, day_path):
                    if file.object_name.endswith("/"):
                        continue
                    current_date = datetime.strptime(day, "%Y-%m-%d")
                    file_start_date, file_end_date = parse_date(current_date, file.object_name)
                    file_start_date = time_zone.localize(file_start_date, is_dst=None)
                    file_end_date = time_zone.localize(file_end_date, is_dst=None)

                    if end_date <= file_start_date or start_date >= file_end_date:
                        continue
                    print("Fetching file:", file.object_name)
                    client.fget_object(
                        bucket,
                        file.object_name,
                        tmpdir
                        + "/"
                        + f"{file_start_date.strftime('%Y-%m-%d_%H-%M-%S')}_{file_end_date.strftime('%Y-%m-%d_%H-%M-%S')}.parquet",
                    )

        table = None

        for file in os.listdir(tmpdir):
            print(file)

            for batch in pq.ParquetFile(tmpdir + "/" + file).iter_batches(batch_size=limit or 65536):
                print(batch)
                batch = Table.from_batches(batches=[batch])

                if table is None:
                    table = batch
                else:
                    table = pa.concat_tables([table, batch])

                if table is not None and limit is not None and len(table) >= limit:
                    break

    if limit is not None and table is not None:
        table = table[:limit]

    return table

riga_code = """
from datetime import datetime, timedelta
import os
import tempfile
import minio
import pyarrow as pa
import pyarrow.parquet as pq
from pytz import timezone
import pandas as pd

def fetch_data(
        start_date={start_date},
        end_date={end_date},
        feed_path: str = {feed_path},
        access_key=os.environ.get("MINIO_ACCESS_KEY"),
        secret_key=os.environ.get("MINIO_SECRET_KEY"),
        timezone_str="Europe/Brussels",
) -> pd.DataFrame:
    client = minio.Minio(
        "minio-api.apps.emeralds.ari-aidata.eu",
        access_key=access_key,
        secret_key=secret_key,
        secure=True,
    )

    time_zone = timezone(timezone_str)

    start_date = time_zone.localize(
        start_date,
    )
    end_date = time_zone.localize(
        end_date,
    )
    bucket = "public"
    days_of_request = []
    current_date = start_date
    while current_date < end_date:
        days_of_request.append(current_date.strftime("%Y-%m-%d"))
        current_date = current_date + timedelta(days=1)
    service_path = feed_path

    days_in_cloud = list(client.list_objects(bucket, service_path))
    days_in_cloud_names = [day.object_name.split("/")[-2] for day in days_in_cloud]

    def parse_date(date, file_name):
        hour = int(file_name.split("/")[-1].split(".")[0])
        return (
            datetime(date.year, date.month, date.day, hour, 0, 0),
            datetime(date.year, date.month, date.day, hour, 59, 59)
        )

    with tempfile.TemporaryDirectory() as tmpdir:
        for day in days_of_request:
            if day in days_in_cloud_names:
                day_path = service_path + day + "/"
                for file in client.list_objects(bucket, day_path):
                    if file.object_name.endswith("/"):
                        continue
                    current_date = datetime.strptime(day, "%Y-%m-%d")
                    file_start_date, file_end_date = parse_date(current_date, file.object_name)
                    file_start_date = time_zone.localize(file_start_date, is_dst=None)
                    file_end_date = time_zone.localize(file_end_date, is_dst=None)

                    if end_date <= file_start_date or start_date >= file_end_date:
                        continue

                    client.fget_object(
                        bucket,
                        file.object_name,
                        tmpdir
                        + "/"
                        + f"{file_start_date.strftime('%Y-%m-%d_%H-%M-%S')}_{file_end_date.strftime('%Y-%m-%d_%H-%M-%S')}.parquet",
                    )

        table = None

        for file in os.listdir(tmpdir):
            current_table = pq.read_table(
                tmpdir + "/" + file,
            )

            if table is None:
                table = current_table
            else:
                table = pa.concat_tables([table, current_table])

    return table.to_pandas()  # Convert to pandas DataFrame for easier handling

"""


all_code ="""
from datetime import datetime, timedelta
import os
import tempfile
import minio
import pyarrow as pa
import pyarrow.parquet as pq
from pytz import timezone
import pandas as pd

def fetch_data(
        start_date={start_date},
        end_date={end_date},
        feed_path: str = {feed_path},
        access_key=os.environ.get("MINIO_ACCESS_KEY"),
        secret_key=os.environ.get("MINIO_SECRET_KEY"),
        timezone_str="Europe/Brussels",
) -> pd.DataFrame:
    client = minio.Minio(
        "minio-api.apps.emeralds.ari-aidata.eu",
        access_key=access_key,
        secret_key=secret_key,
        secure=True,
    )

    time_zone = timezone(timezone_str)

    start_date = time_zone.localize(
        start_date,
    )
    end_date = time_zone.localize(
        end_date,
    )
    bucket = "public"
    days_of_request = []
    current_date = start_date
    while current_date < end_date:
        days_of_request.append(current_date.strftime("%Y-%m-%d"))
        current_date = current_date + timedelta(days=1)
    service_path = feed_path

    days_in_cloud = list(client.list_objects(bucket, service_path))
    days_in_cloud_names = [day.object_name.split("/")[-2] for day in days_in_cloud]

    def parse_date(date, file_name):
        start_time = file_name.split("/")[-1].split("_")[0]
        end_time = (
            file_name.split("/")[-1]
            .split("_")[2]
            .replace(".parquet", "")
        )

        date_str = date.strftime("%Y-%m-%d")
        file_start_date = datetime.strptime(
            date_str + "_" + start_time, "%Y-%m-%d_%H-%M-%S"
        )
        file_end_date = datetime.strptime(
            date_str + "_" + end_time, "%Y-%m-%d_%H-%M-%S"
        )
        
        if file_start_date == file_end_date:
            file_end_date = file_start_date + timedelta(days=1)

        return file_start_date, file_end_date

    with tempfile.TemporaryDirectory() as tmpdir:
        for day in days_of_request:
            if day in days_in_cloud_names:
                day_path = service_path + day + "/"
                for file in client.list_objects(bucket, day_path):
                    current_date = datetime.strptime(day, "%Y-%m-%d")
                    file_start_date, file_end_date = parse_date(current_date, file.object_name)
                    file_start_date = time_zone.localize(file_start_date, is_dst=None)
                    file_end_date = time_zone.localize(file_end_date, is_dst=None)

                    if end_date <= file_start_date or start_date >= file_end_date:
                        continue

                    client.fget_object(
                        bucket,
                        file.object_name,
                        tmpdir
                        + "/"
                        + f"{file_start_date.strftime('%Y-%m-%d_%H-%M-%S')}_{file_end_date.strftime('%Y-%m-%d_%H-%M-%S')}.parquet",
                    )

        table = None

        for file in os.listdir(tmpdir):
            current_table = pq.read_table(
                tmpdir + "/" + file,
            )

            if table is None:
                table = current_table
            else:
                table = pa.concat_tables([table, current_table])

    return table.to_pandas()  # Convert to pandas DataFrame for easier handling
"""