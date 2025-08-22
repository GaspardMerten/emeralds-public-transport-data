import gzip
import json
import time
import uuid
from datetime import timedelta, datetime
from io import BytesIO

import plotly.express
import pyarrow.parquet as pq
import pydeck
import streamlit as st
from pyarrow import csv
from pytz import timezone
from streamlit_calendar_input import calendar_input
from streamlit_downloader import downloader

from fetch import FeedType, get_available_dates, fetch_data, riga_code, all_code, fetch_data_per_days

st.set_page_config(
    page_title="Emeralds - GTFS RT Data Viewer",
    page_icon="favicon.ico",
)

if 'download' not in st.session_state:
    st.session_state['download'] = False
    st.session_state["current_fetch_day"] = None

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
        "columns": {
            "latitude": "vehicle_position_latitude",
            "longitude": "vehicle_position_longitude",
            "id": "id"
        }
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
            FeedType.TRIP_UPDATE: "data/ovapi-train/TripUpdate/",
        },
        "fetch_time_column": "fetchTime",
        "timezone": "Europe/Brussels",
        "code": all_code,
    },
    "york": {
        "name": "YORK",
        "path": "data/york",
        "feeds": {
            FeedType.VEHICLE_POSITION: "data/york/VehiclePosition/",
        },
        "fetch_time_column": "fetchTime",
        "timezone": "Europe/London",
        "code": all_code,
        "columns": {
            "id": "trip_tripId"
        }
    }
}

st.logo("logo.png", )
st.title("Emeralds - GTFS RT Data Viewer")
st.text(
    "This application allows you to view and download GTFS RT data from various providers, by selecting a feed and a date.")
feed = st.selectbox("Select a GTFS RT feed", list(providers.keys()), key="feed_selector", placeholder="Select a feed",
                    index=None)


def bulk_dl(start_date=None, end_date=None):
    if st.session_state.current_fetch_day is None:
        st.session_state.current_fetch_day = start_date


    day = st.session_state.current_fetch_day
    st.text(f"Downloading {day.isoformat()[:10]}...")

    table = fetch_data(
            start_date=day,
            end_date=day + timedelta(days=1),
            feed_path=feed_path,
            parse_date=provider.get('file_to_period', None),
            timezone_str=provider.get('timezone', 'UTC'),

    )
    st.session_state.current_fetch_day += timedelta(days=1)

    if st.session_state.current_fetch_day > end_date:
        st.session_state.current_fetch_day = None
        st.session_state.download = False

    if table:
        import pyarrow as pa
        download_id = str(uuid.uuid4())
        file_path = f"{download_id}.csv.gz"

        print(table.schema)
        columns_todrop = ["multiCarriageDetails", "trip_modifiedTrip"]
        try:
            table = table.drop(columns_todrop)
        except Exception:
            pass
        with pa.CompressedOutputStream(file_path, "gzip") as out:
            csv.write_csv(table, out)
        del table
        downloader(
            open(file_path, "rb").read(),
            day.isoformat()[:10] + '.csv',
            "application/gzip",
        )
    time.sleep(1)
    st.rerun()


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


        tab1, tab2= st.tabs(["General", "Bulk download"])
        with tab2:
            start_date = st.date_input(key="start_date", label="Start Date", value=datetime.now() - timedelta(days=7))
            end_date = st.date_input(key="end_date", label="End Date", value=datetime.now())
            start_date = datetime(start_date.year, start_date.month, start_date.day)
            end_date = datetime(end_date.year, end_date.month, end_date.day)
            if feed_type_enum != FeedType.VEHICLE_POSITION:
                st.warning("Bulk download on web is only supported for vehicle position feeds. Please use the code below")
            elif provider['name'] == 'OVAPI':
                st.warning("Bulk download on web is not supported for OVAPI, Streamlit cloud does not offer enough RAM")
            else:
                st.write("Specify a start and end date. Then click on download button. CSV files for each day will be downloaded. For large provider such as OVAPI you can expect to wait close to a minute per file.")


                download = st.button("Download")
                if download:
                    st.session_state["download"] = True

                if st.session_state.download:
                    if st.button("Stop download"):
                        st.session_state.download = False
                        st.session_state.current_fetch_day = None

                if st.session_state.download:
                    bulk_dl(
                        start_date=datetime(start_date.year, start_date.month, start_date.day),
                        end_date=datetime(end_date.year, end_date.month, end_date.day),
                    )

            st.subheader("Get the code")

            code = provider['code']
            code = code.replace('{start_date}',
                                f'datetime({start_date.year}, {start_date.month}, {start_date.day}, {start_date.hour}, {start_date.minute}, 0, 0)')
            code = code.replace('{end_date}',
                                f'datetime({end_date.year}, {end_date.month}, {end_date.day}, {end_date.hour}, {end_date.minute}, 0, 0)')
            code = code.replace('{feed_path}', f'"{feed_path}"')
            file_name = f"{provider['name'].replace(' ', '_').lower()}_fetch_data.py"
            st.download_button(
                label="Download Code",
                data=code,
                file_name=file_name,
                mime="text/plain"
            )

            st.text(
                "Once, you have downloaded the code, you can run it in your local environment to fetch the data, but first you need to install the required packages:")
            st.code("pip install pytz pyarrow minio pandas")
            st.code(f"""from {file_name.split('.')[0]} import fetch_data
from datetime import datetime

df = fetch_data_per_days(
start_date=datetime({start_date.year}, {start_date.month}, {start_date.day}, {start_date.hour}, {start_date.minute}),
end_date=datetime({end_date.year}, {end_date.month}, {end_date.day}, {end_date.hour}, {end_date.minute}),
access_key="YOUR_ACCESS_KEY",
secret_key="YOUR_SECRET_KEY",
)
""")
        with tab1:
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
                                              limit=100 if feed_type_enum == FeedType.TRIP_UPDATE else None
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
                                    return bytes_io.getvalue()


                            class ProxyCSVBytesIO(BytesIO):
                                def __init__(self, table):
                                    super().__init__()
                                    self.table = table

                                def getvalue(self, *args, **kwargs):
                                    bytes_io = BytesIO()
                                    csv.write_csv(self.table, bytes_io)
                                    bytes_io.seek(0)
                                    return bytes_io.getvalue()


                            class ProxyJSONBytesIO(BytesIO):
                                def __init__(self, table):
                                    super().__init__()
                                    import pyarrow as pa
                                    self.table: pa.Table = table

                                def getvalue(self, *args, **kwargs):
                                    bytes_io = BytesIO()
                                    bytes_io.seek(0)
                                    data = self.table.to_pylist()
                                    bytes_io.write(json.dumps(data).encode('utf-8'))

                                    return bytes_io.getvalue()


                            if st.button("Prepare Data for Download"):
                                if feed == "ovapi" and feed_type_enum == FeedType.TRIP_UPDATE:
                                    st.warning(
                                        "The OVAPI Trip Update feed is too large to download. Please use the code provided below to fetch the data in your local environment.")
                                    st.stop()
                                else:
                                    data = fetch_data(
                                        start_date,
                                        end_date,
                                        feed_path=feed_path,
                                        parse_date=provider.get('file_to_period', None),
                                        timezone_str=provider.get('timezone', 'UTC'),
                                        limit=None
                                    )
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
                        code = code.replace('{start_date}',
                                            f'datetime({start_date.year}, {start_date.month}, {start_date.day}, {start_date.hour}, {start_date.minute}, 0, 0)')
                        code = code.replace('{end_date}',
                                            f'datetime({end_date.year}, {end_date.month}, {end_date.day}, {end_date.hour}, {end_date.minute}, 0, 0)')
                        code = code.replace('{feed_path}', f'"{feed_path}"')
                        file_name = f"{provider['name'].replace(' ', '_').lower()}_fetch_data.py"
                        st.download_button(
                            label="Download Code",
                            data=code,
                            file_name=file_name,
                            mime="text/plain"
                        )

                        st.text(
                            "Once, you have downloaded the code, you can run it in your local environment to fetch the data, but first you need to install the required packages:")
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

                        if not data:
                            st.warning("No data found for the selected date and hour.")
                            st.stop()

                        st.subheader("Data Preview")
                        st.write("First 100 rows of the data:")
                        st.write(data[:100].to_pandas())

                        with st.expander("Show data schema"):
                            schema_str = data.schema.to_string()
                            st.code(schema_str, language="yaml")

                        if feed_type == FeedType.VEHICLE_POSITION.value and data:
                            st.subheader("Data Visualization")
                            columns = provider.get('columns', {})
                            fetch_time_column = provider.get('fetch_time_column', 'fetchTime')
                            latitude_column = columns.get('latitude', 'position_latitude')
                            longitude_column = columns.get('longitude', 'position_longitude')

                            figure = plotly.express.scatter_mapbox(
                                data_frame=data.to_pandas().head(10000),
                                lat=latitude_column,
                                lon=longitude_column,
                                zoom=10,
                                mapbox_style="carto-positron",
                                title=f"Static plot, vehicle positions on {start_date.strftime('%Y-%m-%d %H:%M')}",
                            )
                            st.plotly_chart(figure, height=800)

                            vehicle_id = columns.get('id', 'trip_tripId')
                            df = data.to_pandas()[
                                [
                                    vehicle_id,
                                    fetch_time_column,
                                    latitude_column,
                                    longitude_column,
                                ]
                            ]
                            # order by id
                            df = df.sort_values(by=[vehicle_id, fetch_time_column])

                            groups = []

                            start_timestamp = df[fetch_time_column].min()
                            end_timestamp = df[fetch_time_column].max() - start_timestamp

                            for _, group in df.groupby(vehicle_id):
                                timestamps = group[fetch_time_column].tolist()
                                path = group[[longitude_column, latitude_column]].values.tolist()
                                groups.append({
                                    "timestamps": list(map(int, [timestamp - start_timestamp for timestamp in timestamps])),
                                    "path": [
                                        [float(coord) for coord in point] for point in path
                                    ],
                                    "color": [255, 0, 0]  # Red color for the path
                                })

                            st.markdown(
                                f"**Replaying data from {start_date.strftime('%Y-%m-%d %H:%M')} to {end_date.strftime('%Y-%m-%d %H:%M')}**")

                            replay_speed = st.slider(
                                "Replay speed (how many seconds to show per 1 second), 1 means same speed",
                                min_value=1,
                                max_value=60,
                                value=1,
                                step=1,
                            )

                            number_of_trips_at_the_same_time = st.number_input(
                                "Number of trips at the same time",
                                min_value=1,
                                max_value=1000,
                                value=10,
                            )

                            groups = groups[:number_of_trips_at_the_same_time]

                            trip_layer = pydeck.Layer(
                                "TripsLayer",
                                id="trips-layer",
                                data=groups,
                                get_timestamps="timestamps",
                                get_path="path",
                                current_time=0,
                                trail_length=100,
                                width_min_pixels=8,
                                get_color="color",
                            )
                            mean_lat = df[latitude_column].mean()
                            mean_lon = df[longitude_column].mean()
                            deck = pydeck.Deck(
                                initial_view_state=pydeck.ViewState(
                                    latitude=float(mean_lat),
                                    longitude=float(mean_lon),
                                    zoom=9,
                                    pitch=50,
                                ),
                                layers=[trip_layer],
                            )

                            placeholder = st.empty()

                            for i in range(start_timestamp // replay_speed):
                                with placeholder.container():
                                    st.progress(round(i / (start_timestamp // replay_speed) * 100))
                                    st.pydeck_chart(deck)
                                    trip_layer.current_time += replay_speed
                                    time.sleep(1)
