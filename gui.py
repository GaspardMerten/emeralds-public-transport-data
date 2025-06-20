import json
from datetime import timedelta, datetime
from io import BytesIO

import streamlit as st
from pytz import timezone
from streamlit_calendar_input import calendar_input

from fetch import FeedType, get_available_dates, fetch_data, riga_code, all_code
import pyarrow.parquet as pq
from pyarrow import csv

def parse_date_riga(date, file_name):
    hour = int(file_name.split("/")[-1].split(".")[0])
    return (
        datetime(date.year, date.month, date.day, hour, 0, 0),
        datetime(date.year, date.month, date.day, hour, 59, 59)
    )


providers = {
    "riga": {
        "name": "Riga Public Transport",
        "feeds": {
            FeedType.VEHICLE_POSITION: "data/riga/flattened_position/",
            FeedType.TRIP_UPDATE: "data/riga/flattened_trip_update/",
        },
        "file_to_period": parse_date_riga,
        "fetch_time_column": "timestamp",
        "timezone": "Europe/Riga",
        "code": riga_code,
    },
    "ovapi": {
        "name": "OVAPI",
        "path": "data/ovapi",
        "feeds": {
            FeedType.VEHICLE_POSITION: "data/ovapi/VehiclePosition/",
            FeedType.ALERT: "data/ovapi/Alert/",
            FeedType.TRIP_UPDATE: "data/ovapi/TripUpdate/",
        },
        "fetch_time_column": "fetchTime",
        "timezone": "Europe/Brussels",
        "code": all_code,
    },
    "ovapi-train": {
        "name": "OVAPI train",
        "path": "data/ovapi-train",
        "feeds": {
            FeedType.VEHICLE_POSITION: "data/ovapi-train/VehiclePosition/",
        },
        "fetch_time_column": "fetchTime",
        "timezone": "Europe/Brussels",
        "code": all_code,
    },
    "york": {
        "name": "YORK",
        "path": "data/york",
        "feeds": {
            FeedType.VEHICLE_POSITION: "data/ovapi/VehiclePosition/",
        },
        "fetch_time_column": "fetchTime",
        "timezone": "Europe/London",
        "code": all_code,
    }
}

st.title("Emeralds - GTFS RT Data Viewer")
feed = st.selectbox("Select a GTFS RT feed", list(providers.keys()), key="feed_selector", placeholder="Select a feed",
                    index=None)

if feed:
    provider = providers[feed]
    st.subheader(f"Selected Provider: {provider['name']}")
    st.write("Available Feeds:")
    for feed_type in provider['feeds']:
        st.write(f"- {feed_type.value}")

    # Choose feed type
    feed_type = st.selectbox("Select a feed type", [ft.value for ft in provider['feeds']], key="feed_type_selector",
                             placeholder="Select a feed type", index=None)

    if feed_type:
        feed_type_enum = FeedType(feed_type)
        feed_path = provider['feeds'][feed_type_enum]

        available_dates = get_available_dates(feed_path)

        st.write("Available Dates:")
        if not available_dates:
            st.write("No available dates found for this feed type.")

        if available_dates:
            selected_date = calendar_input(available_dates)

            st.text(f"Selected date: {selected_date}")

            if not selected_date:
                st.info("Please select a date from the calendar.")

            if selected_date:
                hour = st.selectbox("Select hour", list(range(24)), key="hour_selector", index=None)

                if hour is not None:
                    start_date = selected_date.replace(hour=hour, minute=0, second=0, microsecond=0)
                    end_date = start_date + timedelta(hours=1)


                    tz = timezone(
                        provider.get('timezone', 'Europe/Brussels')
                    )
                    with st.spinner("Fetching data..."):
                        data = fetch_data(start_date, end_date, feed_path=feed_path,
                                      parse_date=provider.get('file_to_period', None),
                                      timezone_str=provider.get('timezone', 'UTC'),
                                      )

                    if data:
                        class ProxyParquetBytesIO(BytesIO):
                            def __init__(self, table):
                                super().__init__()
                                self.table = table

                            def getvalue(self, *args, **kwargs):
                                bytes_io = BytesIO()
                                pq.write_table(self.table, bytes_io)
                                bytes_io.seek(0)
                                print("Data written to bytes_io")
                                return bytes_io.getvalue()


                        class ProxyCSVBytesIO(BytesIO):
                            def __init__(self, table):
                                super().__init__()
                                self.table = table

                            def getvalue(self, *args, **kwargs):
                                bytes_io = BytesIO()
                                csv.write_csv(self.table, bytes_io)
                                bytes_io.seek(0)
                                print("Data written to bytes_io")
                                return bytes_io.getvalue()


                        class ProxyJSONBytesIO(BytesIO):
                            def __init__(self, table):
                                super().__init__()
                                import pyarrow as pa
                                self.table:pa.Table = table

                            def getvalue(self, *args, **kwargs):
                                bytes_io = BytesIO()
                                bytes_io.seek(0)
                                data = self.table.to_pylist()
                                bytes_io.write(json.dumps(data).encode('utf-8'))

                                print("Data written to bytes_io")
                                return bytes_io.getvalue()




                        if st.button("Prepare Data for Download"):
                            col1, col2, col3 = st.columns(3)

                            with col1:
                                st.download_button(
                                    label="Download as Parquet",
                                    data=ProxyParquetBytesIO(data),
                                    file_name=f"{feed_type}_{start_date.strftime('%Y-%m-%d_%H-%M')}.parquet",
                                    mime="application/octet-stream"
                                )

                            with col3:
                                    st.download_button(
                                    label="Download as JSON",
                                    data=ProxyJSONBytesIO(data),
                                    file_name=f"{feed_type}_{start_date.strftime('%Y-%m-%d_%H-%M')}.json",
                                    mime="application/json"
                                )

                    st.subheader("Get the code")

                    code = provider['code']
                    code = code.replace('{start_date}', f'datetime({start_date.year}, {start_date.month}, {start_date.day}, {start_date.hour}, {start_date.minute}, 0, 0)')
                    code = code.replace('{end_date}', f'datetime({end_date.year}, {end_date.month}, {end_date.day}, {end_date.hour}, {end_date.minute}, 0, 0)')
                    code = code.replace('{feed_path}', f'"{feed_path}"')
                    file_name = f"{provider['name'].replace(' ', '_').lower()}_fetch_data.py"
                    st.download_button(
                        label="Download Code",
                        data=code,
                        file_name=file_name,
                        mime="text/plain"
                    )

                    st.text("Once, you have downloaded the code, you can run it in your local environment to fetch the data, but first you need to install the required packages:")
                    st.code("pip install pytz pyarrow minio pandas")

                    st.code(f"""from {file_name.split('.')[0]} import fetch_data
from datetime import datetime

df = fetch_data(
    start_date=datetime({start_date.year}, {start_date.month}, {start_date.day}, {start_date.hour}, {start_date.minute}),
    end_date=datetime({end_date.year}, {end_date.month}, {end_date.day}, {end_date.hour}, {end_date.minute}),
    access_key="YOUR_ACCESS_KEY",
    secret_key="YOUR_SECRET_KEY",
)
""")
