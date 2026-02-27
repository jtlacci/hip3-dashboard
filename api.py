import time
import threading
import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

API_URL = "https://api.hyperliquid.xyz/info"

# Max concurrent candleSnapshot requests
_CANDLE_SEMAPHORE = threading.Semaphore(6)

# Persistent cache for listing dates — these never change once a market trades
_listing_date_cache: dict[str, datetime | None] = {}


def _post(payload: dict, retries: int = 4) -> dict | list:
    delay = 1.0
    for attempt in range(retries):
        resp = requests.post(API_URL, json=payload, timeout=10)
        if resp.status_code == 429:
            time.sleep(delay)
            delay *= 2
            continue
        resp.raise_for_status()
        return resp.json()
    # Final attempt — let it raise
    resp = requests.post(API_URL, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_dexes() -> list[dict]:
    """Return list of HIP-3 DEX dicts (nulls filtered out)."""
    data = _post({"type": "perpDexs"})
    return [d for d in data if d is not None]


def get_dex_markets(dex_name: str) -> list[dict]:
    """Fetch metaAndAssetCtxs for a DEX and zip universe + asset contexts."""
    data = _post({"type": "metaAndAssetCtxs", "dex": dex_name})
    universe = data[0].get("universe", []) if len(data) > 0 and isinstance(data[0], dict) else []
    asset_ctxs = data[1] if len(data) > 1 else []

    markets = []
    for meta, ctx in zip(universe, asset_ctxs):
        if ctx is None:
            ctx = {}
        if meta.get("isDelisted"):
            continue
        markets.append({
            "asset": meta.get("name", ""),
            "max_leverage": meta.get("maxLeverage"),
            "growth_mode": meta.get("growthMode"),
            "last_growth_mode_change_time": meta.get("lastGrowthModeChangeTime"),
            "funding": ctx.get("funding"),
            "open_interest": ctx.get("openInterest"),
            "prev_day_px": ctx.get("prevDayPx"),
            "day_ntl_vlm": ctx.get("dayNtlVlm"),
            "mark_px": ctx.get("markPx"),
            "oracle_px": ctx.get("oraclePx"),
        })
    return markets


def _fetch_listing_date(asset: str, fallback_iso: str | None) -> datetime | None:
    """
    First 1h candle open time = listing date proxy (hour precision).
    Fallback: lastGrowthModeChangeTime for zero-volume markets.
    Throttled by semaphore to avoid 429s.
    """
    with _CANDLE_SEMAPHORE:
        try:
            candles = _post({
                "type": "candleSnapshot",
                "req": {
                    "coin": asset,
                    "interval": "1h",
                    "startTime": 0,
                    "endTime": 9_999_999_999_999,
                },
            })
            if candles and len(candles) > 0:
                return datetime.fromtimestamp(candles[0]["t"] / 1000, tz=timezone.utc)
        except Exception:
            pass

    if fallback_iso:
        try:
            return datetime.fromisoformat(fallback_iso[:26]).replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            pass

    return None


def _get_listing_dates(assets_with_fallbacks: list[tuple[str, str | None]]) -> dict[str, datetime | None]:
    """Fetch listing dates in parallel, skipping already-cached assets."""
    results: dict[str, datetime | None] = {}
    to_fetch = []

    for asset, fallback in assets_with_fallbacks:
        if asset in _listing_date_cache:
            results[asset] = _listing_date_cache[asset]
        else:
            to_fetch.append((asset, fallback))

    if to_fetch:
        with ThreadPoolExecutor(max_workers=12) as executor:
            futures = {
                executor.submit(_fetch_listing_date, asset, fallback): asset
                for asset, fallback in to_fetch
            }
            for future in as_completed(futures):
                asset = futures[future]
                try:
                    dt = future.result()
                except Exception:
                    dt = None
                _listing_date_cache[asset] = dt
                results[asset] = dt

    return results


def get_all_markets() -> pd.DataFrame:
    """Fetch all HIP-3 markets across all DEXes and return a flat DataFrame."""
    dexes = get_dexes()

    oi_cap_lookup: dict[str, float] = {}
    dex_info: dict[str, dict] = {}
    for dex in dexes:
        name = dex.get("name", "")
        dex_info[name] = {
            "full_name": dex.get("fullName", name),
            "deployer": dex.get("deployer", ""),
        }
        for pair in dex.get("assetToStreamingOiCap", []) or []:
            if pair is None or len(pair) < 2:
                continue
            try:
                oi_cap_lookup[pair[0]] = float(pair[1])
            except (ValueError, TypeError):
                pass

    # Fetch all DEX markets in parallel
    raw_rows = []
    with ThreadPoolExecutor(max_workers=len(dexes)) as executor:
        futures = {
            executor.submit(get_dex_markets, dex.get("name", "")): dex
            for dex in dexes
        }
        for future in as_completed(futures):
            dex = futures[future]
            dex_name = dex.get("name", "")
            try:
                markets = future.result()
            except Exception:
                continue

            info = dex_info.get(dex_name, {})
            for m in markets:
                asset = m["asset"]
                ticker = asset.split(":")[-1] if ":" in asset else asset
                oi_cap = oi_cap_lookup.get(asset)

                try:
                    oi_float = float(m["open_interest"] or 0)
                except (ValueError, TypeError):
                    oi_float = 0.0

                try:
                    mark_float = float(m["mark_px"] or 0)
                except (ValueError, TypeError):
                    mark_float = 0.0

                open_interest_usd = oi_float * mark_float

                oi_cap_pct = None
                if oi_cap and oi_cap > 0:
                    oi_cap_pct = open_interest_usd / oi_cap * 100

                raw_rows.append({
                    "dex": dex_name,
                    "dex_full_name": info.get("full_name", dex_name),
                    "deployer": info.get("deployer", ""),
                    "asset": asset,
                    "ticker": ticker,
                    "mark_px": _to_float(m["mark_px"]),
                    "oracle_px": _to_float(m["oracle_px"]),
                    "day_ntl_vlm": _to_float(m["day_ntl_vlm"]),
                    "open_interest": open_interest_usd,
                    "oi_cap": oi_cap,
                    "oi_cap_pct": oi_cap_pct,
                    "funding": _to_float(m["funding"]),
                    "max_leverage": m["max_leverage"],
                    "growth_mode": m["growth_mode"],
                    "_fallback_iso": m.get("last_growth_mode_change_time"),
                })

    if not raw_rows:
        return pd.DataFrame()

    # Fetch listing dates (throttled, cached across refreshes)
    assets_with_fallbacks = [(r["asset"], r["_fallback_iso"]) for r in raw_rows]
    listing_dates = _get_listing_dates(assets_with_fallbacks)

    now = datetime.now(tz=timezone.utc)
    rows = []
    for row in raw_rows:
        listing_dt = listing_dates.get(row["asset"])
        market_age_days = (now - listing_dt).days if listing_dt else None
        rows.append({
            **{k: v for k, v in row.items() if k != "_fallback_iso"},
            "listing_date": listing_dt,
            "market_age_days": market_age_days,
        })

    df = pd.DataFrame(rows)
    df.sort_values(["dex", "day_ntl_vlm"], ascending=[True, False], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _to_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
