"""
Kafka producer for the weather streaming pipeline.

Fetches current weather for six world cities from the Open-Meteo API
(free, no API key required) and publishes one event per city to Kafka
on every cycle.

Event schema
------------
{
  "fetched_at":       "2026-04-06T12:00:00+00:00",
  "source":           "open-meteo",
  "city":             "London",
  "latitude":         51.5074,
  "longitude":        -0.1278,
  "temperature_c":    12.3,
  "wind_speed_kmh":   18.5,
  "humidity_pct":     72,
  "precipitation_mm": 0.0,
  "weather_code":     3
}
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ModuleNotFoundError as exc:
    if "kafka.vendor.six.moves" in str(exc):
        raise RuntimeError(
            "Incompatible Kafka client detected. "
            "Run: pip uninstall -y kafka kafka-python && pip install -r requirements.txt"
        ) from exc
    raise


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
LOGGER = logging.getLogger("weather-producer")

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC",             "weather_events")
FETCH_INTERVAL_SECONDS  = int(os.getenv("FETCH_INTERVAL_SECONDS", "10"))
API_TIMEOUT_SECONDS     = int(os.getenv("API_TIMEOUT_SECONDS",    "10"))

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
CURRENT_FIELDS = (
    "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,weather_code"
)

CITIES: List[Dict[str, Any]] = [
    {"name": "New York", "latitude":  40.7128, "longitude":  -74.0060},
    {"name": "London",   "latitude":  51.5074, "longitude":   -0.1278},
    {"name": "Tokyo",    "latitude":  35.6762, "longitude":  139.6503},
    {"name": "Sydney",   "latitude": -33.8688, "longitude":  151.2093},
    {"name": "Paris",    "latitude":  48.8566, "longitude":    2.3522},
    {"name": "Dubai",    "latitude":  25.2048, "longitude":   55.2708},
]


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        acks="all",
        linger_ms=50,
    )


def fetch_weather(city: Dict[str, Any]) -> Dict[str, Any]:
    """Request current weather for one city and return a normalised event dict."""
    resp = requests.get(
        OPEN_METEO_URL,
        params={
            "latitude":  city["latitude"],
            "longitude": city["longitude"],
            "current":   CURRENT_FIELDS,
            "timezone":  "UTC",
        },
        timeout=API_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    cur = resp.json()["current"]
    return {
        "fetched_at":       datetime.now(timezone.utc).isoformat(),
        "source":           "open-meteo",
        "city":             city["name"],
        "latitude":         float(city["latitude"]),
        "longitude":        float(city["longitude"]),
        "temperature_c":    float(cur["temperature_2m"]),
        "wind_speed_kmh":   float(cur["wind_speed_10m"]),
        "humidity_pct":     int(cur["relative_humidity_2m"]),
        "precipitation_mm": float(cur["precipitation"]),
        "weather_code":     int(cur["weather_code"]),
    }


def shutdown_handler(signum: int, _frame: Any) -> None:
    LOGGER.info("Signal %s received — shutting down.", signum)
    raise KeyboardInterrupt


def main() -> None:
    signal.signal(signal.SIGINT,  shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        producer = create_producer()
        LOGGER.info(
            "Connected | bootstrap=%s topic=%s cities=%d interval=%ds",
            KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, len(CITIES), FETCH_INTERVAL_SECONDS,
        )
    except Exception as exc:
        LOGGER.exception("Failed to create Kafka producer: %s", exc)
        sys.exit(1)

    try:
        while True:
            for city in CITIES:
                try:
                    event = fetch_weather(city)
                    meta  = producer.send(KAFKA_TOPIC, value=event).get(timeout=10)
                    LOGGER.info(
                        "city=%-10s temp=%+6.1f°C hum=%3d%% wind=%5.1f km/h"
                        " | partition=%d offset=%d",
                        event["city"],
                        event["temperature_c"],
                        event["humidity_pct"],
                        event["wind_speed_kmh"],
                        meta.partition,
                        meta.offset,
                    )
                except requests.RequestException as exc:
                    LOGGER.error("API error for %s: %s", city["name"], exc)
                except KafkaError as exc:
                    LOGGER.error("Kafka error for %s: %s", city["name"], exc)
                except Exception as exc:
                    LOGGER.exception("Unexpected error for %s: %s", city["name"], exc)

            time.sleep(FETCH_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        LOGGER.info("Producer stopped.")
    finally:
        try:
            producer.flush(timeout=10)
            producer.close()
        except Exception:
            LOGGER.warning("Cleanup warning on producer close.", exc_info=True)


if __name__ == "__main__":
    main()
