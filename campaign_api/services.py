import json
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from threading import Lock
from typing import Any, Dict, List, Tuple

import requests
from django.conf import settings


class TTLCache:
    def __init__(self):
        self._data: Dict[str, Tuple[float, Any]] = {}
        self._lock = Lock()

    def get(self, key: str):
        now = time.time()
        with self._lock:
            val = self._data.get(key)
            if not val:
                return None
            expires_at, payload = val
            if expires_at <= now:
                self._data.pop(key, None)
                return None
            return payload

    def set(self, key: str, value: Any, ttl_seconds: int):
        with self._lock:
            self._data[key] = (time.time() + ttl_seconds, value)


cache = TTLCache()


def get_cache_key(prefix: str, params: Dict[str, Any] = None) -> str:
    return f"{prefix}:{json.dumps(params or {}, sort_keys=True)}"


def serialize(value: Any):
    if isinstance(value, dict):
        return {k: serialize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [serialize(v) for v in value]
    if isinstance(value, tuple):
        return [serialize(v) for v in value]
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def ongage_headers() -> Dict[str, str]:
    return {
        "x_account_code": settings.ONGAGE_ACCOUNT_CODE,
        "x_username": settings.ONGAGE_USERNAME,
        "x_password": settings.ONGAGE_PASSWORD,
        "Content-Type": "application/json",
    }


def build_date_filter(time_filter: str, start_date: str = None, end_date: str = None) -> List[List[Any]]:
    date_filter: List[List[Any]] = []
    if time_filter == "all_time":
        return date_filter

    if time_filter == "custom" and start_date and end_date:
        now = datetime.now(timezone.utc)
        start_dt = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))

        start_epoch = int(
            datetime(
                start_dt.year,
                start_dt.month,
                start_dt.day,
                0,
                0,
                0,
                tzinfo=timezone.utc,
            ).timestamp()
        )

        is_today = end_dt.date() == now.date()
        if is_today:
            end_epoch = int(now.timestamp())
        else:
            end_epoch = int(
                datetime(
                    end_dt.year,
                    end_dt.month,
                    end_dt.day,
                    23,
                    59,
                    59,
                    tzinfo=timezone.utc,
                ).timestamp()
            )

        return [["stats_date", ">=", start_epoch], ["stats_date", "<=", end_epoch]]

    if time_filter in ("today", "yesterday"):
        now = datetime.now(timezone.utc)
        offset = 1 if time_filter == "yesterday" else 0
        target = datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=timezone.utc) - timedelta(days=offset)
        start_epoch = int(target.timestamp())
        end_epoch = start_epoch + 86399
        return [["stats_date", ">=", start_epoch], ["stats_date", "<=", end_epoch]]

    days_map = {
        "last_7_days": 7,
        "last_10_days": 10,
        "last_20_days": 20,
        "last_30_days": 30,
    }
    last_days = days_map.get(time_filter)
    if not last_days:
        return date_filter

    from_epoch = int(time.time()) - (last_days * 24 * 60 * 60)
    return [["stats_date", ">=", from_epoch]]


def calc_campaign_ttl(campaign_name: str) -> int:
    ttl = 60 * 60 * 24 * 10
    if not campaign_name or len(campaign_name) < 8 or not campaign_name[:8].isdigit():
        return ttl

    y = int(campaign_name[:4])
    m = int(campaign_name[4:6])
    d = int(campaign_name[6:8])
    campaign_day = datetime(y, m, d)
    diff_days = (datetime.now() - campaign_day).total_seconds() / (24 * 60 * 60)

    if diff_days < 1:
        return 60 * 60 * 3
    if diff_days < 5:
        return 60 * 60 * 24
    return ttl


def ongage_post(url: str, payload: Dict[str, Any]):
    response = requests.post(url, headers=ongage_headers(), json=payload, timeout=60)
    response.raise_for_status()
    return response.json()


def ongage_get(url: str):
    response = requests.get(url, headers=ongage_headers(), timeout=60)
    response.raise_for_status()
    return response.json()
