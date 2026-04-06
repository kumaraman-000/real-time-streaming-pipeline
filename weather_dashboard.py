"""
Streamlit weather dashboard — redesigned for clarity.

Layout
------
Sidebar         : database, Kafka, and producer configuration (collapsed by default)
Tab "Overview"  : KPI cards, per-city condition cards, temperature + humidity + wind trends
Tab "Pipeline"  : start / stop producer & consumer, status, live logs
Tab "Data"      : hourly aggregation table, raw records table, CSV download
"""

import atexit
import os
import pathlib
import re
import subprocess
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_DIR          = os.path.dirname(os.path.abspath(__file__))
PROCESS_LOG_DIR   = os.path.join(BASE_DIR, "runtime_logs")
LOCAL_HADOOP_HOME = os.path.join(BASE_DIR, "tools", "hadoop")

load_dotenv()


def _secret_or_env(key: str, default: str = "") -> str:
    try:
        value = st.secrets.get(key, None)
        if value not in (None, ""):
            return str(value)
    except Exception:
        pass
    return os.getenv(key, default)

DEFAULT_DB: Dict[str, str] = {
    "host":      _secret_or_env("POSTGRES_HOST",      "localhost"),
    "port":      _secret_or_env("POSTGRES_PORT",      "5432"),
    "dbname":    _secret_or_env("POSTGRES_DB",        "postgres"),
    "user":      _secret_or_env("POSTGRES_USER",      "postgres"),
    "password":  _secret_or_env("POSTGRES_PASSWORD",  ""),
    "raw_table": _secret_or_env("POSTGRES_RAW_TABLE", "raw_weather"),
    "agg_table": _secret_or_env("POSTGRES_AGG_TABLE", "weather_hourly_agg"),
}
DEFAULT_KAFKA: Dict[str, str] = {
    "bootstrap_servers": _secret_or_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "topic":             _secret_or_env("KAFKA_TOPIC",             "weather_events"),
}
DEFAULT_API: Dict[str, str] = {
    "fetch_interval_seconds": _secret_or_env("FETCH_INTERVAL_SECONDS", "10"),
}

# WMO Weather Interpretation Codes (https://open-meteo.com/en/docs)
WMO_CODES: Dict[int, str] = {
    0:  "Clear sky",
    1:  "Mainly clear",        2:  "Partly cloudy",          3:  "Overcast",
    45: "Fog",                 48: "Icy fog",
    51: "Light drizzle",       53: "Moderate drizzle",        55: "Dense drizzle",
    61: "Slight rain",         63: "Moderate rain",           65: "Heavy rain",
    71: "Slight snow",         73: "Moderate snow",           75: "Heavy snow",
    77: "Snow grains",
    80: "Slight showers",      81: "Moderate showers",        82: "Violent showers",
    85: "Light snow showers",  86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm + slight hail",  99: "Thunderstorm + heavy hail",
}

_VALID_TABLE_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------

def weather_label(code: int) -> str:
    return WMO_CODES.get(int(code), f"Code {code}")


def _validate_table(name: str) -> str:
    if not _VALID_TABLE_RE.match(name):
        raise ValueError(f"Invalid table name: {name!r}")
    return name


def _fmt_temp(val: Any) -> str:
    return f"{float(val):.1f} C" if val is not None else "-"


# ---------------------------------------------------------------------------
# Process management
# ---------------------------------------------------------------------------

def init_session() -> None:
    st.session_state.setdefault("producer_process", None)
    st.session_state.setdefault("consumer_process", None)


def is_running(proc: Optional[subprocess.Popen]) -> bool:
    return proc is not None and proc.poll() is None


def _cleanup_finished() -> None:
    for key in ("producer_process", "consumer_process"):
        proc = st.session_state.get(key)
        if proc is not None and proc.poll() is not None:
            st.session_state[key] = None


def _terminate(key: str) -> None:
    proc = st.session_state.get(key)
    if proc is None:
        return
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    st.session_state[key] = None


def cleanup_all() -> None:
    for key in ("producer_process", "consumer_process"):
        proc = st.session_state.get(key)
        if proc is not None and proc.poll() is None:
            try:
                proc.terminate()
            except Exception:
                pass


def start_process(
    script: str,
    key: str,
    db: Dict[str, str],
    kafka: Dict[str, str],
    api: Dict[str, str],
) -> str:
    if is_running(st.session_state.get(key)):
        return f"{script} is already running."

    os.makedirs(PROCESS_LOG_DIR, exist_ok=True)
    log_path = os.path.join(PROCESS_LOG_DIR, f"{script.replace('.py', '')}.log")
    log_fh   = open(log_path, "a", encoding="utf-8")

    env = os.environ.copy()
    env.update({
        "POSTGRES_HOST":           db["host"],
        "POSTGRES_PORT":           db["port"],
        "POSTGRES_DB":             db["dbname"],
        "POSTGRES_USER":           db["user"],
        "POSTGRES_PASSWORD":       db["password"],
        "POSTGRES_RAW_TABLE":      db["raw_table"],
        "POSTGRES_AGG_TABLE":      db["agg_table"],
        "KAFKA_BOOTSTRAP_SERVERS": kafka["bootstrap_servers"],
        "KAFKA_TOPIC":             kafka["topic"],
        "FETCH_INTERVAL_SECONDS":  api["fetch_interval_seconds"],
    })
    if os.name == "nt" and os.path.exists(
        os.path.join(LOCAL_HADOOP_HOME, "bin", "winutils.exe")
    ):
        env["HADOOP_HOME"]     = LOCAL_HADOOP_HOME
        env["hadoop.home.dir"] = LOCAL_HADOOP_HOME

    proc = subprocess.Popen(
        [sys.executable, script],
        cwd=BASE_DIR,
        env=env,
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        text=True,
    )
    st.session_state[key] = proc
    return f"Started {script} (logs -> runtime_logs/{script.replace('.py', '')}.log)"


def read_log(script: str, lines: int = 40) -> str:
    path = pathlib.Path(PROCESS_LOG_DIR) / f"{script.replace('.py', '')}.log"
    if not path.exists():
        return "No log file yet. Start the process to begin logging."
    content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(content[-lines:]) if content else "Log file is empty."


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def pg_connect(db: Dict[str, str]) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=db["host"], port=db["port"], dbname=db["dbname"],
        user=db["user"], password=db["password"],
    )


def ensure_tables(db: Dict[str, str]) -> None:
    raw = _validate_table(db["raw_table"])
    agg = _validate_table(db["agg_table"])
    with pg_connect(db) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {raw} (
                    fetched_at       TIMESTAMP        NOT NULL,
                    source           VARCHAR(50)      NOT NULL,
                    city             VARCHAR(100)     NOT NULL,
                    latitude         DOUBLE PRECISION NOT NULL,
                    longitude        DOUBLE PRECISION NOT NULL,
                    temperature_c    DOUBLE PRECISION NOT NULL,
                    wind_speed_kmh   DOUBLE PRECISION NOT NULL,
                    humidity_pct     INTEGER          NOT NULL,
                    precipitation_mm DOUBLE PRECISION NOT NULL,
                    weather_code     INTEGER          NOT NULL
                )
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {agg} (
                    hour_bucket            TIMESTAMP        NOT NULL,
                    city                   VARCHAR(100)     NOT NULL,
                    avg_temperature_c      DOUBLE PRECISION,
                    max_temperature_c      DOUBLE PRECISION,
                    min_temperature_c      DOUBLE PRECISION,
                    avg_wind_speed_kmh     DOUBLE PRECISION,
                    avg_humidity_pct       DOUBLE PRECISION,
                    total_precipitation_mm DOUBLE PRECISION,
                    record_count           INTEGER,
                    PRIMARY KEY (hour_bucket, city)
                )
            """)
        conn.commit()


def load_data(db: Dict[str, str], raw_limit: int) -> Dict[str, Any]:
    """Fetch all dashboard data in a single database connection."""
    raw = _validate_table(db["raw_table"])
    agg = _validate_table(db["agg_table"])

    with pg_connect(db) as conn:
        with conn.cursor() as cur:

            # --- Global KPIs ---
            cur.execute(f"""
                SELECT
                    COUNT(*)                              AS total_records,
                    COUNT(DISTINCT city)                  AS cities_active,
                    ROUND(AVG(temperature_c)::numeric, 1) AS global_avg_temp_c,
                    MAX(fetched_at)                       AS last_updated
                FROM {raw}
            """)
            row  = cur.fetchone()
            cols = [d.name for d in cur.description]
            kpi  = dict(zip(cols, row or (0, 0, None, None)))

            cur.execute(f"""
                SELECT COUNT(*) FROM {raw}
                WHERE fetched_at >= NOW() - INTERVAL '1 hour'
            """)
            kpi["records_last_hour"] = (cur.fetchone() or [0])[0]

            # --- Latest reading per city ---
            cur.execute(f"""
                SELECT DISTINCT ON (city)
                    city, fetched_at,
                    temperature_c, humidity_pct, wind_speed_kmh,
                    precipitation_mm, weather_code
                FROM {raw}
                ORDER BY city, fetched_at DESC
            """)
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
            latest_df = pd.DataFrame(rows, columns=cols)

            # --- Hourly aggregations (last 48 h) ---
            cur.execute(f"""
                SELECT
                    hour_bucket, city,
                    avg_temperature_c, max_temperature_c, min_temperature_c,
                    avg_wind_speed_kmh, avg_humidity_pct,
                    total_precipitation_mm, record_count
                FROM {agg}
                WHERE hour_bucket >= NOW() - INTERVAL '48 hours'
                ORDER BY hour_bucket ASC, city ASC
            """)
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
            agg_df = pd.DataFrame(rows, columns=cols)

            # --- Raw records ---
            cur.execute(f"""
                SELECT fetched_at, city, temperature_c, humidity_pct,
                       wind_speed_kmh, precipitation_mm, weather_code
                FROM {raw}
                ORDER BY fetched_at DESC
                LIMIT %s
            """, (raw_limit,))
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
            raw_df = pd.DataFrame(rows, columns=cols)

    return {
        "kpi":    kpi,
        "latest": latest_df,
        "agg":    agg_df,
        "raw":    raw_df,
    }


def build_demo_data(raw_limit: int) -> Dict[str, Any]:
    cities = ["New York", "London", "Tokyo", "Sydney", "Paris", "Dubai"]
    base = datetime.utcnow().replace(minute=0, second=0, microsecond=0)

    raw_rows = []
    agg_rows = []
    for city_idx, city in enumerate(cities):
        for hour_offset in range(47, -1, -1):
            ts = base - timedelta(hours=hour_offset)
            avg_temp = 12 + city_idx * 3 + ((hour_offset % 6) - 3) * 0.8
            min_temp = avg_temp - 1.5
            max_temp = avg_temp + 1.7
            avg_wind = 8 + city_idx * 1.3 + (hour_offset % 5)
            avg_humidity = 48 + city_idx * 4 + (hour_offset % 9)
            total_precip = 0.0 if city in {"Dubai", "Paris"} else round((hour_offset % 4) * 0.4, 1)
            agg_rows.append({
                "hour_bucket": ts,
                "city": city,
                "avg_temperature_c": round(avg_temp, 1),
                "max_temperature_c": round(max_temp, 1),
                "min_temperature_c": round(min_temp, 1),
                "avg_wind_speed_kmh": round(avg_wind, 1),
                "avg_humidity_pct": round(avg_humidity, 1),
                "total_precipitation_mm": round(total_precip, 1),
                "record_count": 1,
            })

        latest_ts = base - timedelta(minutes=city_idx * 3)
        raw_rows.append({
            "fetched_at": latest_ts,
            "city": city,
            "temperature_c": round(agg_rows[-1]["avg_temperature_c"], 1),
            "humidity_pct": int(55 + city_idx * 5),
            "wind_speed_kmh": round(10 + city_idx * 1.5, 1),
            "precipitation_mm": round(0.2 * (city_idx % 3), 1),
            "weather_code": [1, 3, 2, 61, 45, 0][city_idx],
        })

    raw_df = pd.DataFrame(raw_rows).sort_values("fetched_at", ascending=False).head(raw_limit)
    latest_df = raw_df.sort_values(["city", "fetched_at"], ascending=[True, False]).drop_duplicates("city")
    agg_df = pd.DataFrame(agg_rows)

    kpi = {
        "total_records": len(raw_rows),
        "cities_active": len(cities),
        "global_avg_temp_c": round(float(agg_df["avg_temperature_c"].mean()), 1),
        "last_updated": raw_df["fetched_at"].max(),
        "records_last_hour": sum(raw_df["fetched_at"] >= (datetime.utcnow() - timedelta(hours=1))),
    }

    return {
        "kpi": kpi,
        "latest": latest_df,
        "agg": agg_df,
        "raw": raw_df,
    }


# ---------------------------------------------------------------------------
# Tab 1 — Overview & Trends
# ---------------------------------------------------------------------------

def render_overview(data: Dict[str, Any]) -> None:
    kpi       = data["kpi"]
    latest_df = data["latest"]
    agg_df    = data["agg"]

    # --- KPI cards ---
    st.subheader("Summary")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric(
        "Total Records",
        int(kpi.get("total_records", 0) or 0),
        help="All weather records stored in PostgreSQL",
    )
    c2.metric(
        "Cities Active",
        int(kpi.get("cities_active", 0) or 0),
        help="Number of distinct cities with at least one record",
    )
    c3.metric(
        "Global Avg Temp",
        _fmt_temp(kpi.get("global_avg_temp_c")),
        help="Average temperature across all cities and records",
    )
    c4.metric(
        "Records (last hr)",
        int(kpi.get("records_last_hour", 0) or 0),
        help="Records written to PostgreSQL in the last 60 minutes",
    )
    last = kpi.get("last_updated")
    c5.metric(
        "Last Record At",
        str(last)[:19] if last else "-",
        help="Timestamp of the most recently stored record",
    )

    st.divider()

    # --- Per-city condition cards ---
    st.subheader("Current Conditions per City")
    st.caption("Most recent reading stored for each city.")

    if latest_df.empty:
        st.info("No records yet. Start the producer and consumer, then refresh.")
    else:
        cols = st.columns(len(latest_df))
        for col, (_, row) in zip(cols, latest_df.iterrows()):
            with col:
                with st.container(border=True):
                    st.markdown(f"### {row['city']}")
                    st.metric("Temperature", f"{row['temperature_c']:.1f} C")
                    st.write(f"**Condition:** {weather_label(int(row['weather_code']))}")
                    st.write(f"**Humidity:** {row['humidity_pct']} %")
                    st.write(f"**Wind:** {row['wind_speed_kmh']:.1f} km/h")
                    st.write(f"**Precip:** {row['precipitation_mm']:.1f} mm")
                    st.caption(str(row["fetched_at"])[:19])

    st.divider()

    # --- Trend charts ---
    if agg_df.empty:
        st.info("No hourly aggregations yet. Data appears here once the consumer has run for at least one hour.")
        return

    agg = agg_df.copy()
    agg["hour_bucket"] = pd.to_datetime(agg["hour_bucket"])

    # City filter
    all_cities = sorted(agg["city"].unique().tolist())
    selected   = st.multiselect(
        "Filter cities", all_cities, default=all_cities,
        help="Select which cities to show in the charts below.",
    )
    if selected:
        agg = agg[agg["city"].isin(selected)]

    # Temperature trend
    st.subheader("Average Temperature Over Time (C)")
    st.caption("Hourly average computed from all raw readings for each city.")
    fig = px.line(
        agg,
        x="hour_bucket",
        y="avg_temperature_c",
        color="city",
        markers=True,
        labels={
            "hour_bucket":       "Time (hour)",
            "avg_temperature_c": "Avg Temp (C)",
            "city":              "City",
        },
    )
    fig.update_layout(hovermode="x unified", margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

    # Temperature min/max band
    st.subheader("Temperature Range (Min / Max per Hour)")
    st.caption("Shows how much temperature varied within each hour.")
    fig2 = px.line(
        agg,
        x="hour_bucket",
        y=["min_temperature_c", "avg_temperature_c", "max_temperature_c"],
        color="city",
        facet_col="city",
        facet_col_wrap=3,
        labels={
            "hour_bucket": "Time",
            "value":       "Temp (C)",
            "variable":    "Metric",
        },
    )
    fig2.update_layout(margin=dict(t=30))
    fig2.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    st.plotly_chart(fig2, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Average Humidity Over Time (%)")
        fig3 = px.line(
            agg,
            x="hour_bucket",
            y="avg_humidity_pct",
            color="city",
            markers=True,
            labels={
                "hour_bucket":      "Time (hour)",
                "avg_humidity_pct": "Avg Humidity (%)",
                "city":             "City",
            },
        )
        fig3.update_layout(hovermode="x unified", margin=dict(t=10))
        st.plotly_chart(fig3, use_container_width=True)

    with col2:
        st.subheader("Total Precipitation per Hour (mm)")
        fig4 = px.bar(
            agg,
            x="hour_bucket",
            y="total_precipitation_mm",
            color="city",
            barmode="group",
            labels={
                "hour_bucket":           "Time (hour)",
                "total_precipitation_mm": "Precipitation (mm)",
                "city":                  "City",
            },
        )
        fig4.update_layout(margin=dict(t=10))
        st.plotly_chart(fig4, use_container_width=True)


# ---------------------------------------------------------------------------
# Tab 2 — Pipeline Control
# ---------------------------------------------------------------------------

def render_pipeline(
    db: Dict[str, str],
    kafka: Dict[str, str],
    api: Dict[str, str],
    demo_mode: bool = False,
) -> None:
    _cleanup_finished()

    prod_running = is_running(st.session_state.get("producer_process"))
    cons_running = is_running(st.session_state.get("consumer_process"))

    # --- Status ---
    st.subheader("Pipeline Status")
    st.caption(
        "The **consumer** reads from Kafka and writes to PostgreSQL. "
        "Start it first so no messages are missed."
    )
    if demo_mode:
        st.warning(
            "Demo mode is active because PostgreSQL is not reachable from this deployment. "
            "The charts below are sample data, and the local producer/consumer controls may not work in Streamlit Cloud."
        )

    s1, s2 = st.columns(2)
    with s1:
        with st.container(border=True):
            status = "Running" if prod_running else "Stopped"
            color  = "green"   if prod_running else "red"
            st.markdown(f"**Producer** &nbsp; :{color}[{status}]")
            st.caption(
                "Fetches weather from Open-Meteo every "
                f"{api['fetch_interval_seconds']} s and publishes to Kafka topic "
                f"`{kafka['topic']}`."
            )
    with s2:
        with st.container(border=True):
            status = "Running" if cons_running else "Stopped"
            color  = "green"   if cons_running else "red"
            st.markdown(f"**Consumer** &nbsp; :{color}[{status}]")
            st.caption(
                f"The consumer reads from Kafka topic `{kafka['topic']}`, "
                f"validates records, and writes to `{db['raw_table']}` and `{db['agg_table']}`."
            )

    st.divider()

    # --- Controls ---
    st.subheader("Controls")
    st.caption("Recommended order: Start Consumer first, then Start Producer.")

    bc1, bc2, bc3, bc4 = st.columns(4)
    if bc1.button("Start Consumer", use_container_width=True, type="primary", disabled=demo_mode):
        st.success(start_process("weather_consumer.py", "consumer_process", db, kafka, api))
    if bc2.button("Stop Consumer",  use_container_width=True, disabled=demo_mode):
        _terminate("consumer_process")
        st.info("Consumer stopped.")
    if bc3.button("Start Producer", use_container_width=True, type="primary", disabled=demo_mode):
        st.success(start_process("weather_producer.py", "producer_process", db, kafka, api))
    if bc4.button("Stop Producer",  use_container_width=True, disabled=demo_mode):
        _terminate("producer_process")
        st.info("Producer stopped.")

    st.divider()

    # --- Logs ---
    st.subheader("Live Logs")
    st.caption("Last 40 lines from each process log file. Press R to refresh.")

    log1, log2 = st.columns(2)
    with log1:
        st.markdown("**Consumer log**")
        st.code(read_log("weather_consumer.py"), language="text")
    with log2:
        st.markdown("**Producer log**")
        st.code(read_log("weather_producer.py"), language="text")


# ---------------------------------------------------------------------------
# Tab 3 — Data Tables
# ---------------------------------------------------------------------------

def render_data(data: Dict[str, Any]) -> None:
    agg_df = data["agg"]
    raw_df = data["raw"]

    # --- Hourly aggregations ---
    st.subheader("Hourly Aggregations")
    st.caption(
        "One row per city per hour. Recomputed from raw records every time new data "
        "arrives, so values are always accurate."
    )

    if agg_df.empty:
        st.info("No hourly aggregations yet.")
    else:
        display_agg = agg_df.copy()
        display_agg["hour_bucket"] = pd.to_datetime(display_agg["hour_bucket"])
        display_agg.columns = [
            "Hour", "City",
            "Avg Temp (C)", "Max Temp (C)", "Min Temp (C)",
            "Avg Wind (km/h)", "Avg Humidity (%)",
            "Total Precip (mm)", "Record Count",
        ]
        st.dataframe(display_agg, use_container_width=True, hide_index=True)
        st.download_button(
            "Download Hourly Aggregations as CSV",
            display_agg.to_csv(index=False),
            "weather_hourly_agg.csv",
            "text/csv",
        )

    st.divider()

    # --- Raw records ---
    st.subheader("Raw Records")
    st.caption(
        "Individual readings as written by the consumer after passing "
        "data-quality checks."
    )

    if raw_df.empty:
        st.info("No raw records yet.")
    else:
        display_raw = raw_df.copy()
        display_raw["fetched_at"] = pd.to_datetime(display_raw["fetched_at"])
        display_raw["condition"]  = display_raw["weather_code"].apply(weather_label)
        display_raw = display_raw.drop(columns=["weather_code"])
        display_raw.columns = [
            "Fetched At", "City", "Temp (C)", "Humidity (%)",
            "Wind (km/h)", "Precip (mm)", "Condition",
        ]
        st.dataframe(display_raw, use_container_width=True, hide_index=True)
        st.download_button(
            "Download Raw Records as CSV",
            display_raw.to_csv(index=False),
            "weather_raw_records.csv",
            "text/csv",
        )


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def render_sidebar() -> tuple:
    with st.sidebar:
        st.header("Configuration")

        with st.expander("Database", expanded=True):
            db_host     = st.text_input("Host",           DEFAULT_DB["host"])
            db_port     = st.text_input("Port",           DEFAULT_DB["port"])
            db_name     = st.text_input("Database",       DEFAULT_DB["dbname"])
            db_user     = st.text_input("User",           DEFAULT_DB["user"])
            db_password = st.text_input("Password",       DEFAULT_DB["password"], type="password")
            raw_table   = st.text_input("Staging table",  DEFAULT_DB["raw_table"])
            agg_table   = st.text_input("Analytics table",DEFAULT_DB["agg_table"])

        with st.expander("Kafka", expanded=False):
            kafka_servers = st.text_input("Bootstrap servers", DEFAULT_KAFKA["bootstrap_servers"])
            kafka_topic   = st.text_input("Topic",             DEFAULT_KAFKA["topic"])

        with st.expander("Producer", expanded=False):
            fetch_interval = st.text_input(
                "Fetch interval (seconds)", DEFAULT_API["fetch_interval_seconds"]
            )

        st.divider()
        raw_limit = st.slider(
            "Raw records to display", min_value=10, max_value=500, value=100, step=10,
        )
        st.caption("Press **R** to refresh all data.")

    db = {
        "host": db_host, "port": db_port, "dbname": db_name,
        "user": db_user, "password": db_password,
        "raw_table": raw_table, "agg_table": agg_table,
    }
    kafka = {"bootstrap_servers": kafka_servers, "topic": kafka_topic}
    api   = {"fetch_interval_seconds": fetch_interval}
    return db, kafka, api, raw_limit


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    init_session()
    atexit.register(cleanup_all)

    st.set_page_config(page_title="Weather Pipeline Dashboard", layout="wide")

    st.title("Real-Time Weather Streaming Dashboard")
    st.caption(
        "6 world cities | Open-Meteo API -> Kafka -> Consumer -> PostgreSQL"
    )

    db, kafka, api, raw_limit = render_sidebar()
    demo_mode = False

    # Verify DB connection and bootstrap tables
    try:
        ensure_tables(db)
    except Exception as exc:
        demo_mode = True
        st.warning(f"PostgreSQL connection failed, so the app is using demo data: {exc}")
        st.info(
            "To use live data in Streamlit Cloud, provide a reachable PostgreSQL host in app secrets instead of localhost."
        )

    tab_overview, tab_pipeline, tab_data = st.tabs(
        ["Overview & Trends", "Pipeline Control", "Data"]
    )

    # Load all data once per page render
    if demo_mode:
        data = build_demo_data(raw_limit)
    else:
        try:
            data = load_data(db, raw_limit)
        except Exception as exc:
            demo_mode = True
            st.warning(f"Failed to load data from PostgreSQL, switching to demo mode: {exc}")
            data = build_demo_data(raw_limit)

    with tab_overview:
        render_overview(data)

    with tab_pipeline:
        render_pipeline(db, kafka, api, demo_mode=demo_mode)

    with tab_data:
        render_data(data)


if __name__ == "__main__":
    main()
