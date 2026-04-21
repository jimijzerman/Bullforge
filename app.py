import requests
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from typing import Optional, Dict, List, Tuple
from datetime import datetime
from pathlib import Path

st.set_page_config(page_title="BullForge", layout="wide")

# =========================================================
# Config
# =========================================================
REFRESH_ANALYSIS_SEC = 20
SCANNER_CACHE_SEC = 60

BASE_URL = "https://api.bitvavo.com"
API_PREFIX = "/v2"

COINS = {
    "BTC": {"bitvavo_market": "BTC-EUR"},
    "ETH": {"bitvavo_market": "ETH-EUR"},
    "SOL": {"bitvavo_market": "SOL-EUR"},
    "TAO": {"bitvavo_market": "TAO-EUR"},
    "XRP": {"bitvavo_market": "XRP-EUR"},
}

TIMEFRAMES = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}


# Multi-timeframe hierarchy (stap 1)
# trigger = exacte instap timeframe
# setup   = timeframe voor de trade-opzet
# trend   = timeframe voor de hoofdrichting
TIMEFRAME_HIERARCHY = {
    "1m":  {"trigger": "1m",  "setup": "5m",  "trend": "15m"},
    "5m":  {"trigger": "5m",  "setup": "15m", "trend": "1h"},
    "15m": {"trigger": "15m", "setup": "1h",  "trend": "4h"},
    "30m": {"trigger": "30m", "setup": "1h",  "trend": "4h"},
    "1h":  {"trigger": "1h",  "setup": "4h",  "trend": "1d"},
    "4h":  {"trigger": "4h",  "setup": "1d",  "trend": "1d"},
    "1d":  {"trigger": "1d",  "setup": "1d",  "trend": "1d"},
}

HIGHER_TIMEFRAME_MAP = {
    "1m": "15m",
    "5m": "1h",
    "15m": "4h",
    "30m": "4h",
    "1h": "1d",
    "4h": "1d",
    "1d": "1d",
}

DEFAULT_MAKER_FEE_PCT = 0.09
DEFAULT_TAKER_FEE_PCT = 0.18

DEFAULT_SHORT_LIQUIDATION_FEE_PCT = 2.0

DEFAULT_SHORT_BORROW_HOURLY_PCT = {
    "BTC": 0.01,
    "ETH": 0.01,
    "SOL": 0.012,
    "TAO": 0.02,
    "XRP": 0.012,
}

DEFAULT_EXPECTED_HOLD_HOURS = {
    "1m": 2.0,
    "5m": 4.0,
    "15m": 8.0,
    "30m": 12.0,
    "1h": 24.0,
    "4h": 36.0,
    "1d": 72.0,
}

ENTRY_MODES = {
    "DoopieCash": "doopiecash",
    "Limit": "limit",
    "Gebalanceerd": "balanced",
    "Bevestiging": "confirmation",
}

JOURNAL_FILE = Path("bullforge_trade_journal.csv")
JOURNAL_OUTCOMES = ["OPEN", "TP", "SL", "BE", "MANUAL_EXIT"]


def load_trade_journal() -> pd.DataFrame:
    columns = [
        "journal_id", "logged_at", "coin", "scanner_tf", "trigger_tf", "setup_tf", "trend_tf",
        "context", "trend_label", "side", "plan_type", "entry_variant", "location_quality",
        "entry", "stop", "target", "rr", "net_profit_eur", "conservative_net", "score",
        "current_price", "outcome", "resolved_at", "notes"
    ]
    text_columns = [
        "journal_id", "logged_at", "coin", "scanner_tf", "trigger_tf", "setup_tf", "trend_tf",
        "context", "trend_label", "side", "plan_type", "entry_variant", "location_quality",
        "outcome", "resolved_at", "notes"
    ]

    if JOURNAL_FILE.exists():
        try:
            df = pd.read_csv(JOURNAL_FILE)
            for col in columns:
                if col not in df.columns:
                    df[col] = None
            df = df[columns].copy()
            for col in text_columns:
                df[col] = df[col].astype("object")
            return df
        except Exception:
            return pd.DataFrame(columns=columns)

    df = pd.DataFrame(columns=columns)
    for col in text_columns:
        df[col] = df[col].astype("object")
    return df


def save_trade_journal(df: pd.DataFrame) -> None:
    df_to_save = df.copy()
    text_columns = [
        "journal_id", "logged_at", "coin", "scanner_tf", "trigger_tf", "setup_tf", "trend_tf",
        "context", "trend_label", "side", "plan_type", "entry_variant", "location_quality",
        "outcome", "resolved_at", "notes"
    ]
    for col in text_columns:
        if col in df_to_save.columns:
            df_to_save[col] = df_to_save[col].astype("object")
    df_to_save.to_csv(JOURNAL_FILE, index=False)


def build_journal_entry(
    selected_result: Dict[str, object],
    side: str,
    plan_type: str,
    metrics: Optional[Dict[str, object]],
) -> Dict[str, object]:
    location_info = selected_result.get("long_location", {}) if side == "LONG" else selected_result.get("short_location", {})
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    journal_id = f"{selected_result.get('coin','UNK')}-{side}-{plan_type}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    return {
        "journal_id": journal_id,
        "logged_at": now,
        "coin": selected_result.get("coin"),
        "scanner_tf": selected_result.get("timeframe_label"),
        "trigger_tf": selected_result.get("trigger_timeframe_label"),
        "setup_tf": selected_result.get("setup_timeframe_label"),
        "trend_tf": selected_result.get("trend_timeframe_label"),
        "context": selected_result.get("market_context"),
        "trend_label": selected_result.get("trend_label"),
        "side": side,
        "plan_type": plan_type,
        "entry_variant": selected_result.get("chosen_entry_variant"),
        "location_quality": location_info.get("quality"),
        "entry": float(metrics["entry"]) if metrics else None,
        "stop": float(metrics["stop"]) if metrics else None,
        "target": float(metrics["target"]) if metrics else None,
        "rr": float(metrics["rr"]) if metrics else None,
        "net_profit_eur": float(metrics["net_profit_eur"]) if metrics else None,
        "conservative_net": float(selected_result.get("conservative_best_net") or 0.0) if plan_type == "best" else None,
        "score": float(selected_result.get("score") or 0.0),
        "current_price": float(selected_result.get("current_price") or 0.0) if selected_result.get("current_price") is not None else None,
        "outcome": "OPEN",
        "resolved_at": None,
        "notes": "",
    }



def build_manual_journal_entry(
    coin: str,
    scanner_tf: str,
    trigger_tf: str,
    setup_tf: str,
    trend_tf: str,
    context: str,
    trend_label: str,
    side: str,
    plan_type: str,
    location_quality: str,
    entry: float,
    stop: float,
    target: float,
    notes: str = "",
) -> Dict[str, object]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    journal_id = f"{coin}-{side}-{plan_type}-MAN-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    rr = None
    if side == "LONG" and entry > stop:
        rr = max((target - entry) / (entry - stop), 0.0)
    elif side == "SHORT" and stop > entry:
        rr = max((entry - target) / (stop - entry), 0.0)

    return {
        "journal_id": journal_id,
        "logged_at": now,
        "coin": coin,
        "scanner_tf": scanner_tf,
        "trigger_tf": trigger_tf,
        "setup_tf": setup_tf,
        "trend_tf": trend_tf,
        "context": context,
        "trend_label": trend_label,
        "side": side,
        "plan_type": plan_type,
        "entry_variant": "manual",
        "location_quality": location_quality,
        "entry": float(entry),
        "stop": float(stop),
        "target": float(target),
        "rr": round(float(rr), 2) if rr is not None else None,
        "net_profit_eur": None,
        "conservative_net": None,
        "score": None,
        "current_price": None,
        "outcome": "OPEN",
        "resolved_at": None,
        "notes": notes,
    }

def append_trade_journal(entry: Dict[str, object]) -> None:
    df = load_trade_journal()
    df = pd.concat([df, pd.DataFrame([entry])], ignore_index=True)
    save_trade_journal(df)

# =========================================================
# Header
# =========================================================
st.markdown(
    """
    <div style='text-align:center; padding-top: 8px; padding-bottom: 8px;'>
        <div style='font-size: 52px; font-weight: 800;'>🐂 BullForge</div>
        <div style='font-size: 18px; color: #9CA3AF;'>Smarter Trading • Price Action Driven</div>
    </div>
    """,
    unsafe_allow_html=True
)
st.caption(
    f"📊 Scanner cache {SCANNER_CACHE_SEC} sec • Candle-data cache {REFRESH_ANALYSIS_SEC} sec"
)

# =========================================================
# Helpers
# =========================================================
def _format_number_eu(x: float, decimals: int) -> str:
    return f"{x:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def price_decimals(x: float) -> int:
    abs_x = abs(float(x))
    if abs_x >= 1000:
        return 2
    if abs_x >= 100:
        return 2
    if abs_x >= 1:
        return 4
    if abs_x >= 0.1:
        return 5
    if abs_x >= 0.01:
        return 6
    if abs_x >= 0.001:
        return 7
    return 8


def fmt_eur(x: float) -> str:
    return f"€ {_format_number_eu(float(x), 2)}"


def fmt_price_eur(x: float) -> str:
    decimals = price_decimals(float(x))
    return f"€ {_format_number_eu(float(x), decimals)}"


def pct(x: float) -> str:
    return f"{x:.2f}%"


def safe_pct_distance(value: Optional[float], reference: Optional[float]) -> Optional[float]:
    if value is None or reference is None or reference == 0:
        return None
    return ((float(value) - float(reference)) / float(reference)) * 100


def compute_reclaim_trigger(df: Optional[pd.DataFrame], side: str, lookback: int = 10) -> Optional[float]:
    if df is None or len(df) < 4:
        return None

    recent = df.tail(min(len(df), lookback)).reset_index(drop=True)
    if side == "long":
        pivot_idx = int(recent["low"].idxmin())
        if pivot_idx >= len(recent) - 1:
            return None
        after = recent.iloc[pivot_idx + 1:]
        if after.empty:
            return None
        return float(after["high"].max())

    pivot_idx = int(recent["high"].idxmax())
    if pivot_idx >= len(recent) - 1:
        return None
    after = recent.iloc[pivot_idx + 1:]
    if after.empty:
        return None
    return float(after["low"].min())


def analyze_price_action_confirmation(
    df: Optional[pd.DataFrame],
    side: str,
    zone_level: Optional[float],
    current_price: Optional[float],
    reclaim_trigger: Optional[float],
    vol_profile: Optional[Dict[str, float | str]] = None,
) -> Dict[str, object]:
    result: Dict[str, object] = {
        "confirmed": False,
        "bullish_reject": False,
        "bearish_reject": False,
        "close_back_in_favor": False,
        "reclaim_trigger_hit": False,
        "volume_support": False,
        "touched_zone": False,
        "reason": "Geen prijsactie-bevestiging.",
    }

    if df is None or len(df) < 2 or zone_level is None or current_price is None:
        return result

    zone_level = float(zone_level)
    current_price = float(current_price)
    latest = df.iloc[-1]
    previous = df.iloc[-2]

    avg_range_pct = float((vol_profile or {}).get("avg_range_pct", 1.0))
    zone_width_pct = max(0.20, round(avg_range_pct * 0.35, 2))
    confirm_close_buffer_pct = max(0.02, zone_width_pct * 0.20)

    latest_open = float(latest["open"])
    latest_high = float(latest["high"])
    latest_low = float(latest["low"])
    latest_close = float(latest["close"])
    latest_volume = float(latest["volume"])

    prev_open = float(previous["open"])
    prev_high = float(previous["high"])
    prev_low = float(previous["low"])
    prev_close = float(previous["close"])

    latest_body = abs(latest_close - latest_open)
    prev_body = abs(prev_close - prev_open)
    latest_lower_wick = max(0.0, min(latest_open, latest_close) - latest_low)
    latest_upper_wick = max(0.0, latest_high - max(latest_open, latest_close))
    prev_lower_wick = max(0.0, min(prev_open, prev_close) - prev_low)
    prev_upper_wick = max(0.0, prev_high - max(prev_open, prev_close))

    volume_baseline = float(df["volume"].tail(min(len(df), 20)).mean()) if len(df) > 0 else 0.0
    volume_support = latest_volume > (volume_baseline * 1.05) if volume_baseline > 0 else False
    result["volume_support"] = volume_support

    if side.lower() == "long":
        zone_top = zone_level * (1 + zone_width_pct / 100)
        touched_zone = latest_low <= zone_top or prev_low <= zone_top
        bullish_reject = touched_zone and latest_close > latest_open and latest_lower_wick >= max(latest_body * 1.15, latest_close * 0.001)
        prev_bullish_reject = (prev_low <= zone_top) and (prev_close > prev_open) and (prev_lower_wick >= max(prev_body * 1.15, prev_close * 0.001))
        close_back_in_favor = touched_zone and latest_close >= zone_level * (1 + confirm_close_buffer_pct / 100)
        reclaim_trigger_hit = reclaim_trigger is not None and current_price >= float(reclaim_trigger) * (1 + 0.03 / 100)

        result.update({
            "bullish_reject": bullish_reject or prev_bullish_reject,
            "close_back_in_favor": close_back_in_favor,
            "reclaim_trigger_hit": reclaim_trigger_hit,
            "touched_zone": touched_zone,
        })

        reasons = []
        if bullish_reject or prev_bullish_reject:
            reasons.append("bullish reject candle vanaf support")
        if close_back_in_favor:
            reasons.append("close terug boven supportzone")
        if reclaim_trigger_hit:
            reasons.append("reclaim trigger gebroken")
        if volume_support:
            reasons.append("volume ondersteunt move")

        confirmed = bool((bullish_reject or prev_bullish_reject or close_back_in_favor or reclaim_trigger_hit) and touched_zone)
        if reclaim_trigger_hit:
            confirmed = True
        result["confirmed"] = confirmed
        if reasons:
            result["reason"] = ", ".join(reasons)
    else:
        zone_bottom = zone_level * (1 - zone_width_pct / 100)
        touched_zone = latest_high >= zone_bottom or prev_high >= zone_bottom
        bearish_reject = touched_zone and latest_close < latest_open and latest_upper_wick >= max(latest_body * 1.15, latest_close * 0.001)
        prev_bearish_reject = (prev_high >= zone_bottom) and (prev_close < prev_open) and (prev_upper_wick >= max(prev_body * 1.15, prev_close * 0.001))
        close_back_in_favor = touched_zone and latest_close <= zone_level * (1 - confirm_close_buffer_pct / 100)
        reclaim_trigger_hit = reclaim_trigger is not None and current_price <= float(reclaim_trigger) * (1 - 0.03 / 100)

        result.update({
            "bearish_reject": bearish_reject or prev_bearish_reject,
            "close_back_in_favor": close_back_in_favor,
            "reclaim_trigger_hit": reclaim_trigger_hit,
            "touched_zone": touched_zone,
        })

        reasons = []
        if bearish_reject or prev_bearish_reject:
            reasons.append("bearish reject candle vanaf resistance")
        if close_back_in_favor:
            reasons.append("close terug onder resistancezone")
        if reclaim_trigger_hit:
            reasons.append("reclaim trigger neerwaarts gebroken")
        if volume_support:
            reasons.append("volume ondersteunt move")

        confirmed = bool((bearish_reject or prev_bearish_reject or close_back_in_favor or reclaim_trigger_hit) and touched_zone)
        if reclaim_trigger_hit:
            confirmed = True
        result["confirmed"] = confirmed
        if reasons:
            result["reason"] = ", ".join(reasons)

    return result




def build_limit_plan(
    side: str,
    current_price: Optional[float],
    support_or_resistance: Optional[float],
    hard_level: Optional[float],
    target: Optional[float],
    combined_bias: str,
    market_context: str,
    location_quality: str,
    entry_buffer_pct: float,
    stop_buffer_pct: float,
    account_size: float,
    max_risk_pct: float,
    coin_symbol: str,
    entry_fee_pct: float,
    exit_fee_pct: float,
    min_profit_buffer_eur: float,
    taker_fee_pct: float,
    trigger_vol_profile: Optional[Dict[str, float | str]] = None,
    short_borrow_hourly_pct: float = 0.0,
    expected_hold_hours: float = 0.0,
    short_liquidation_fee_pct: float = DEFAULT_SHORT_LIQUIDATION_FEE_PCT,
) -> Dict[str, object]:
    result: Dict[str, object] = {
        "status": "WAIT",
        "reason": "",
        "entry": None,
        "stop": None,
        "target": None,
        "metrics": None,
        "valid": False,
        "distance_to_entry_pct": None,
    }

    if current_price is None or support_or_resistance is None or target is None:
        result["reason"] = "Onvoldoende data voor limit plan."
        return result

    cp = float(current_price)
    zone = float(support_or_resistance)
    tgt = float(target)
    hard = float(hard_level) if hard_level is not None else zone

    if side == "long":
        trend_note = ""
        if combined_bias == "short" or market_context == "aligned_bearish":
            trend_note = " Trend werkt tegen, dus lagere prioriteit."
        limit_entry = zone * (1 + max(0.02, entry_buffer_pct * 0.20) / 100)
        stop = hard * (1 - stop_buffer_pct / 100)
        distance_to_entry_pct = ((cp - limit_entry) / cp) * 100 if cp else None
        should_plan = cp >= limit_entry and location_quality in {"A_ENTRY", "B_ENTRY", "LATE"}
        if location_quality == "A_ENTRY" and cp <= limit_entry * 1.002:
            result["status"] = "AT_ZONE"
            result["reason"] = "Prijs zit al in of vlak bij de entryzone; confirmed entry is nu logischer." + trend_note
        elif not should_plan:
            result["reason"] = "Prijs zit nog niet duidelijk boven een interessante limit zone." + trend_note
            result["distance_to_entry_pct"] = distance_to_entry_pct
            return result
        metrics = calculate_trade_metrics(
            side="long",
            entry=limit_entry,
            stop=stop,
            target=tgt,
            account_size=account_size * 0.60,
            max_risk_pct=max_risk_pct,
            coin_symbol=coin_symbol,
            entry_fee_pct=entry_fee_pct,
            exit_fee_pct=exit_fee_pct,
            short_borrow_hourly_pct=0.0,
            expected_hold_hours=0.0,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
        )
    else:
        trend_note = ""
        if combined_bias == "long" or market_context == "aligned_bullish":
            trend_note = " Trend werkt tegen, dus lagere prioriteit."
        limit_entry = zone * (1 - max(0.02, entry_buffer_pct * 0.20) / 100)
        stop = hard * (1 + stop_buffer_pct / 100)
        distance_to_entry_pct = ((limit_entry - cp) / cp) * 100 if cp else None
        should_plan = cp <= limit_entry and location_quality in {"A_ENTRY", "B_ENTRY", "LATE"}
        if location_quality == "A_ENTRY" and cp >= limit_entry * 0.998:
            result["status"] = "AT_ZONE"
            result["reason"] = "Prijs zit al in of vlak bij de entryzone; confirmed entry is nu logischer." + trend_note
        elif not should_plan:
            result["reason"] = "Prijs zit nog niet duidelijk onder een interessante limit zone." + trend_note
            result["distance_to_entry_pct"] = distance_to_entry_pct
            return result
        metrics = calculate_trade_metrics(
            side="short",
            entry=limit_entry,
            stop=stop,
            target=tgt,
            account_size=account_size * 0.60,
            max_risk_pct=max_risk_pct,
            coin_symbol=coin_symbol,
            entry_fee_pct=entry_fee_pct,
            exit_fee_pct=exit_fee_pct,
            short_borrow_hourly_pct=short_borrow_hourly_pct,
            expected_hold_hours=expected_hold_hours,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
        )

    result["entry"] = limit_entry
    result["stop"] = stop
    result["target"] = tgt
    result["distance_to_entry_pct"] = distance_to_entry_pct
    result["metrics"] = metrics

    if metrics is None:
        result["reason"] = "Limit plan geeft geen geldige risk/reward."
        return result

    valid = is_setup_valid(metrics, min_profit_buffer_eur, taker_fee_pct)
    result["valid"] = valid

    if valid:
        result["status"] = "LIMIT_READY"
        result["reason"] = "Vooraf ingeplande zone-entry is logisch als prijs terugkomt in de zone." + (trend_note if "trend_note" in locals() else "")
    else:
        result["status"] = "WAIT"
        result["reason"] = "Limit plan is nog niet sterk genoeg na kosten/RR." + (trend_note if "trend_note" in locals() else "")

    return result


def build_doopiecash_plan(
    side: str,
    current_price: Optional[float],
    support_or_resistance: Optional[float],
    hard_level: Optional[float],
    target: Optional[float],
    combined_bias: str,
    market_context: str,
    location_quality: str,
    entry_buffer_pct: float,
    stop_buffer_pct: float,
    account_size: float,
    max_risk_pct: float,
    coin_symbol: str,
    entry_fee_pct: float,
    exit_fee_pct: float,
    min_profit_buffer_eur: float,
    taker_fee_pct: float,
    short_borrow_hourly_pct: float = 0.0,
    expected_hold_hours: float = 0.0,
    short_liquidation_fee_pct: float = DEFAULT_SHORT_LIQUIDATION_FEE_PCT,
) -> Dict[str, object]:
    result: Dict[str, object] = {
        "status": "WAIT",
        "reason": "",
        "entry": None,
        "stop": None,
        "target": None,
        "metrics": None,
        "valid": False,
        "distance_to_entry_pct": None,
    }

    if current_price is None or support_or_resistance is None or target is None:
        result["reason"] = "Onvoldoende data voor DoopieCash plan."
        return result

    cp = float(current_price)
    zone = float(support_or_resistance)
    tgt = float(target)
    hard = float(hard_level) if hard_level is not None else zone

    micro_entry_buffer_pct = max(0.005, min(0.03, entry_buffer_pct * 0.05))
    doopie_stop_buffer_pct = max(0.08, stop_buffer_pct * 0.85)

    if side == "long":
        entry = zone * (1 + micro_entry_buffer_pct / 100)
        stop = hard * (1 - doopie_stop_buffer_pct / 100)
        distance_to_entry_pct = ((cp - entry) / cp) * 100 if cp else None
        metrics = calculate_trade_metrics(
            side="long",
            entry=entry,
            stop=stop,
            target=tgt,
            account_size=account_size,
            max_risk_pct=max_risk_pct,
            coin_symbol=coin_symbol,
            entry_fee_pct=entry_fee_pct,
            exit_fee_pct=exit_fee_pct,
            short_borrow_hourly_pct=0.0,
            expected_hold_hours=0.0,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
        )
    else:
        entry = zone * (1 - micro_entry_buffer_pct / 100)
        stop = hard * (1 + doopie_stop_buffer_pct / 100)
        distance_to_entry_pct = ((entry - cp) / cp) * 100 if cp else None
        metrics = calculate_trade_metrics(
            side="short",
            entry=entry,
            stop=stop,
            target=tgt,
            account_size=account_size,
            max_risk_pct=max_risk_pct,
            coin_symbol=coin_symbol,
            entry_fee_pct=entry_fee_pct,
            exit_fee_pct=exit_fee_pct,
            short_borrow_hourly_pct=short_borrow_hourly_pct,
            expected_hold_hours=expected_hold_hours,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
        )

    result["entry"] = entry
    result["stop"] = stop
    result["target"] = tgt
    result["distance_to_entry_pct"] = distance_to_entry_pct
    result["metrics"] = metrics

    if metrics is None:
        result["reason"] = "DoopieCash plan geeft geen geldige risk/reward."
        return result

    valid = is_setup_valid(metrics, min_profit_buffer_eur, taker_fee_pct)
    if location_quality == "SKIP":
        valid = False
        result["reason"] = "Prijs is al te ver doorgelopen richting target voor een scherpe DoopieCash entry."
    else:
        result["reason"] = "Pure prijsactie-opzet: entry direct op zone uit marktstructuur, zonder bevestiging af te wachten."

    result["valid"] = valid
    result["status"] = "DOOPIECASH_READY" if valid else "WAIT"
    return result

def compute_setup_timing(
    current_price: Optional[float],
    support: Optional[float],
    resistance: Optional[float],
    vol_profile: Dict[str, float | str],
    structure_bias: str = "neutral",
) -> Dict[str, Optional[float] | str]:
    result = {
        "support_zone_top": None,
        "resistance_zone_bottom": None,
        "distance_to_support_pct": None,
        "distance_to_resistance_pct": None,
        "long_timing": "geen data",
        "short_timing": "geen data",
    }

    if current_price is None or support is None or resistance is None or resistance <= support:
        return result

    zone_width_pct = get_zone_width_pct(vol_profile)
    watch_buffer_pct = zone_width_pct + 0.35

    support_zone_top = support * (1 + zone_width_pct / 100)
    resistance_zone_bottom = resistance * (1 - zone_width_pct / 100)

    distance_to_support_pct = safe_pct_distance(current_price, support)
    distance_to_resistance_pct = ((float(resistance) - float(current_price)) / float(resistance)) * 100 if resistance else None

    long_timing = "WATCH"
    if current_price <= support_zone_top:
        long_timing = "READY"
    elif current_price <= support * (1 + watch_buffer_pct / 100):
        long_timing = "NEAR"
    elif current_price >= support + (resistance - support) * 0.45:
        long_timing = "MISSED"

    short_timing = "WATCH"
    if current_price >= resistance_zone_bottom:
        short_timing = "READY"
    elif current_price >= resistance * (1 - watch_buffer_pct / 100):
        short_timing = "NEAR"
    elif current_price <= resistance - (resistance - support) * 0.45:
        short_timing = "MISSED"

    if structure_bias == "long":
        short_timing = "BLOCKED"
    elif structure_bias == "short":
        long_timing = "BLOCKED"
    elif structure_bias == "voorzichtig":
        if long_timing in {"READY", "NEAR"}:
            short_timing = "LOW PRIORITY"
        elif short_timing in {"READY", "NEAR"}:
            long_timing = "LOW PRIORITY"

    result.update({
        "support_zone_top": support_zone_top,
        "resistance_zone_bottom": resistance_zone_bottom,
        "distance_to_support_pct": distance_to_support_pct,
        "distance_to_resistance_pct": distance_to_resistance_pct,
        "long_timing": long_timing,
        "short_timing": short_timing,
    })
    return result




def compute_location_quality(
    current_price: Optional[float],
    support_or_resistance: Optional[float],
    target: Optional[float],
    side: str,
) -> Dict[str, Optional[float] | str]:
    result = {
        "quality": "UNKNOWN",
        "range_progress": None,
        "distance_to_zone_pct": None,
        "distance_to_target_pct": None,
        "reason": "",
    }

    if current_price is None or support_or_resistance is None or target is None:
        return result

    cp = float(current_price)
    zone = float(support_or_resistance)
    tgt = float(target)

    if side.lower() == "long":
        denom = tgt - zone
        if denom <= 0:
            return result
        range_progress = (cp - zone) / denom
        distance_to_zone_pct = abs(cp - zone) / cp * 100 if cp else None
        distance_to_target_pct = max(0.0, (tgt - cp) / cp * 100) if cp else None
    else:
        denom = zone - tgt
        if denom <= 0:
            return result
        range_progress = (zone - cp) / denom
        distance_to_zone_pct = abs(zone - cp) / cp * 100 if cp else None
        distance_to_target_pct = max(0.0, (cp - tgt) / cp * 100) if cp else None

    quality = "UNKNOWN"
    reason = ""
    if range_progress < 0:
        quality = "A_ENTRY"
        reason = "prijs ligt nog gunstig voor de zone"
    elif range_progress <= 0.25:
        quality = "A_ENTRY"
        reason = "dicht op de entry-zone"
    elif range_progress <= 0.45:
        quality = "B_ENTRY"
        reason = "nog prima, maar minder scherp"
    elif range_progress <= 0.65:
        quality = "LATE"
        reason = "technisch geldig, maar al flink onderweg"
    else:
        quality = "SKIP"
        reason = "te ver doorgelopen richting target"

    result.update({
        "quality": quality,
        "range_progress": round(range_progress, 4),
        "distance_to_zone_pct": round(distance_to_zone_pct, 4) if distance_to_zone_pct is not None else None,
        "distance_to_target_pct": round(distance_to_target_pct, 4) if distance_to_target_pct is not None else None,
        "reason": reason,
    })
    return result
def choose_entry_prices(
    current_price: float,
    support: float,
    resistance: float,
    entry_buffer_pct: float,
    entry_mode: str,
    df: Optional[pd.DataFrame],
) -> Tuple[float, float, Optional[float], Optional[float]]:
    limit_long = support * (1 + entry_buffer_pct / 100)
    limit_short = resistance * (1 - entry_buffer_pct / 100)

    balanced_long = support * (1 + (entry_buffer_pct * 2.15) / 100)
    balanced_short = resistance * (1 - (entry_buffer_pct * 2.15) / 100)

    long_trigger = compute_reclaim_trigger(df, "long")
    short_trigger = compute_reclaim_trigger(df, "short")

    confirm_buffer_pct = max(0.03, entry_buffer_pct * 0.35)
    confirmation_long = (long_trigger * (1 + confirm_buffer_pct / 100)) if long_trigger is not None else balanced_long
    confirmation_short = (short_trigger * (1 - confirm_buffer_pct / 100)) if short_trigger is not None else balanced_short

    doopiecash_long = support * (1 + max(0.005, min(0.03, entry_buffer_pct * 0.05)) / 100)
    doopiecash_short = resistance * (1 - max(0.005, min(0.03, entry_buffer_pct * 0.05)) / 100)

    if entry_mode == "doopiecash":
        entry_long = doopiecash_long
        entry_short = doopiecash_short
    elif entry_mode == "balanced":
        entry_long = min(current_price, balanced_long) if current_price <= balanced_long * 1.01 else balanced_long
        entry_short = max(current_price, balanced_short) if current_price >= balanced_short * 0.99 else balanced_short
    elif entry_mode == "confirmation":
        entry_long = max(current_price, confirmation_long)
        entry_short = min(current_price, confirmation_short)
    else:
        entry_long = min(current_price, limit_long) if current_price <= limit_long * 1.01 else limit_long
        entry_short = max(current_price, limit_short) if current_price >= limit_short * 0.99 else limit_short

    return entry_long, entry_short, long_trigger, short_trigger


# =========================================================
# API helpers
# =========================================================
def get_bitvavo_price(market: str) -> Optional[float]:
    url = f"{BASE_URL}{API_PREFIX}/ticker/price"
    try:
        response = requests.get(url, params={"market": market}, timeout=5)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, dict) and "price" in data:
            return float(data["price"])

        if isinstance(data, list) and data and "price" in data[0]:
            return float(data[0]["price"])

        return None
    except requests.RequestException:
        return None
    except (ValueError, TypeError):
        return None


@st.cache_data(ttl=REFRESH_ANALYSIS_SEC, show_spinner=False)
def get_bitvavo_candle_dataframe(
    market: str,
    interval: str = "1h",
    limit: int = 180
) -> Optional[pd.DataFrame]:
    url = f"{BASE_URL}{API_PREFIX}/{market}/candles"
    params = {"interval": interval, "limit": limit}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data or not isinstance(data, list):
            return None

        df = pd.DataFrame(
            data,
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )

        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna().iloc[::-1].reset_index(drop=True)

        if df.empty:
            return None

        df["body_high"] = df[["open", "close"]].max(axis=1)
        df["body_low"] = df[["open", "close"]].min(axis=1)
        return df

    except requests.RequestException:
        return None
    except Exception:
        return None


# =========================================================
# Volatility / auto settings
# =========================================================
def calculate_volatility_profile(df: Optional[pd.DataFrame]) -> Dict[str, float | str]:
    if df is None or len(df) < 30:
        return {"avg_range_pct": 1.0, "vol_label": "onbekend"}

    recent = df.tail(24).copy()
    recent["range_pct"] = ((recent["high"] - recent["low"]) / recent["close"]) * 100
    avg_range_pct = float(recent["range_pct"].mean())

    if avg_range_pct < 1.0:
        vol_label = "laag"
    elif avg_range_pct < 2.0:
        vol_label = "gemiddeld"
    else:
        vol_label = "hoog"

    return {"avg_range_pct": avg_range_pct, "vol_label": vol_label}


def get_auto_trade_settings(coin: str, vol_profile: Dict[str, float | str]) -> Dict[str, float]:
    base = {
        "BTC": {"max_risk_pct": 1.00, "entry_buffer_pct": 0.15, "stop_buffer_pct": 0.30, "rr_target": 2.0},
        "ETH": {"max_risk_pct": 0.90, "entry_buffer_pct": 0.20, "stop_buffer_pct": 0.40, "rr_target": 2.0},
        "SOL": {"max_risk_pct": 0.75, "entry_buffer_pct": 0.30, "stop_buffer_pct": 0.60, "rr_target": 2.2},
    }.get(
        coin,
        {"max_risk_pct": 0.80, "entry_buffer_pct": 0.25, "stop_buffer_pct": 0.50, "rr_target": 2.0}
    ).copy()

    avg_range_pct = float(vol_profile["avg_range_pct"])

    if avg_range_pct < 1.0:
        base["entry_buffer_pct"] *= 0.90
        base["stop_buffer_pct"] *= 0.90
    elif avg_range_pct >= 2.0:
        base["entry_buffer_pct"] *= 1.25
        base["stop_buffer_pct"] *= 1.40
        base["max_risk_pct"] *= 0.80
        base["rr_target"] *= 1.05

    base["max_risk_pct"] = max(0.25, min(5.0, round(base["max_risk_pct"], 2)))
    base["entry_buffer_pct"] = max(0.05, min(1.5, round(base["entry_buffer_pct"], 2)))
    base["stop_buffer_pct"] = max(0.05, min(2.0, round(base["stop_buffer_pct"], 2)))
    base["rr_target"] = max(1.0, min(5.0, round(base["rr_target"], 2)))

    return base


# =========================================================
# Level detection
# =========================================================
def _merge_levels(
    levels: List[float],
    merge_threshold_pct: float = 0.35,
    reverse_sort: bool = False
) -> List[float]:
    if not levels:
        return []

    levels = sorted(levels)
    merged = []
    cluster = [levels[0]]

    for lvl in levels[1:]:
        ref = sum(cluster) / len(cluster)
        pct_diff = abs(lvl - ref) / ref * 100 if ref else 999

        if pct_diff <= merge_threshold_pct:
            cluster.append(lvl)
        else:
            merged.append(sum(cluster) / len(cluster))
            cluster = [lvl]

    merged.append(sum(cluster) / len(cluster))
    return sorted(merged, reverse=reverse_sort)



def detect_swing_levels(
    df: Optional[pd.DataFrame],
    window: int = 3,
    merge_threshold_pct: float = 0.35,
    reference_price: Optional[float] = None,
) -> Dict[str, List[float]]:
    empty_result = {
        "trade_supports": [],
        "trade_resistances": [],
        "hard_supports": [],
        "hard_resistances": [],
    }

    if df is None or len(df) < (window * 2 + 10):
        return empty_result

    highs = df["high"].tolist()
    lows = df["low"].tolist()
    body_highs = df["body_high"].tolist()
    body_lows = df["body_low"].tolist()

    swing_highs = []
    swing_lows = []
    swing_body_highs = []
    swing_body_lows = []

    for i in range(window, len(df) - window):
        if highs[i] == max(highs[i - window:i + window + 1]):
            swing_highs.append(highs[i])

        if lows[i] == min(lows[i - window:i + window + 1]):
            swing_lows.append(lows[i])

        if body_highs[i] == max(body_highs[i - window:i + window + 1]):
            swing_body_highs.append(body_highs[i])

        if body_lows[i] == min(body_lows[i - window:i + window + 1]):
            swing_body_lows.append(body_lows[i])

    merged_hard_supports = _merge_levels(swing_lows, merge_threshold_pct, reverse_sort=True)
    merged_hard_resistances = _merge_levels(swing_highs, merge_threshold_pct, reverse_sort=False)
    merged_trade_supports = _merge_levels(swing_body_lows, merge_threshold_pct * 0.85, reverse_sort=True)
    merged_trade_resistances = _merge_levels(swing_body_highs, merge_threshold_pct * 0.85, reverse_sort=False)

    current_price = float(reference_price) if reference_price is not None else float(df["close"].iloc[-1])

    trade_supports = [lvl for lvl in merged_trade_supports if lvl < current_price]
    trade_resistances = [lvl for lvl in merged_trade_resistances if lvl > current_price]
    hard_supports = [lvl for lvl in merged_hard_supports if lvl < current_price]
    hard_resistances = [lvl for lvl in merged_hard_resistances if lvl > current_price]

    return {
        "trade_supports": trade_supports[:2],
        "trade_resistances": trade_resistances[:2],
        "hard_supports": hard_supports[:2],
        "hard_resistances": hard_resistances[:2],
    }





def analyze_structure_strength(
    df: Optional[pd.DataFrame],
    swings: Dict[str, List[Dict[str, float | int | pd.Timestamp]]],
) -> Dict[str, object]:
    result: Dict[str, object] = {
        "bos_bullish": False,
        "bos_bearish": False,
        "sweep_bullish": False,
        "sweep_bearish": False,
        "failed_breakout_bullish": False,
        "failed_breakout_bearish": False,
        "displacement_bullish": False,
        "displacement_bearish": False,
        "swing_strength": "neutral",
        "impulse_ratio": 1.0,
    }

    if df is None or len(df) < 8:
        return result

    swing_highs = swings.get("swing_highs", [])
    swing_lows = swings.get("swing_lows", [])
    latest = df.iloc[-1]
    recent = df.tail(min(len(df), 20)).copy()

    latest_close = float(latest["close"])
    latest_open = float(latest["open"])
    latest_high = float(latest["high"])
    latest_low = float(latest["low"])

    avg_body = float((recent["close"] - recent["open"]).abs().mean()) if len(recent) > 0 else 0.0
    latest_body = abs(latest_close - latest_open)
    impulse_ratio = (latest_body / avg_body) if avg_body > 0 else 1.0
    result["impulse_ratio"] = round(impulse_ratio, 3)

    if len(swing_highs) >= 2:
        prev_high = float(swing_highs[-2]["price"])
        last_high = float(swing_highs[-1]["price"])
        if latest_close > last_high:
            result["bos_bullish"] = True
        if latest_high > last_high and latest_close < last_high:
            result["sweep_bearish"] = True
            result["failed_breakout_bearish"] = True
        if last_high > prev_high and latest_close < last_high:
            result["failed_breakout_bearish"] = result["failed_breakout_bearish"] or (latest_open > latest_close)

    if len(swing_lows) >= 2:
        prev_low = float(swing_lows[-2]["price"])
        last_low = float(swing_lows[-1]["price"])
        if latest_close < last_low:
            result["bos_bearish"] = True
        if latest_low < last_low and latest_close > last_low:
            result["sweep_bullish"] = True
            result["failed_breakout_bullish"] = True
        if last_low < prev_low and latest_close > last_low:
            result["failed_breakout_bullish"] = result["failed_breakout_bullish"] or (latest_close > latest_open)

    result["displacement_bullish"] = bool(latest_close > latest_open and impulse_ratio >= 1.35)
    result["displacement_bearish"] = bool(latest_close < latest_open and impulse_ratio >= 1.35)

    bull_points = 0
    bear_points = 0
    for key in ["bos_bullish", "sweep_bullish", "failed_breakout_bullish", "displacement_bullish"]:
        bull_points += int(bool(result[key]))
    for key in ["bos_bearish", "sweep_bearish", "failed_breakout_bearish", "displacement_bearish"]:
        bear_points += int(bool(result[key]))

    if bull_points >= bear_points + 1:
        result["swing_strength"] = "bullish"
    elif bear_points >= bull_points + 1:
        result["swing_strength"] = "bearish"
    else:
        result["swing_strength"] = "neutral"

    return result

def _extract_confirmed_swing_points(
    df: Optional[pd.DataFrame],
    window: int = 3,
) -> Dict[str, List[Dict[str, float | int | pd.Timestamp]]]:
    empty_result = {"swing_highs": [], "swing_lows": []}

    if df is None or len(df) < (window * 2 + 5):
        return empty_result

    swing_highs: List[Dict[str, float | int | pd.Timestamp]] = []
    swing_lows: List[Dict[str, float | int | pd.Timestamp]] = []

    highs = df["high"].tolist()
    lows = df["low"].tolist()

    for i in range(window, len(df) - window):
        high_slice = highs[i - window:i + window + 1]
        low_slice = lows[i - window:i + window + 1]

        current_high = float(highs[i])
        current_low = float(lows[i])

        if current_high == max(high_slice):
            swing_highs.append({
                "index": i,
                "price": current_high,
                "timestamp": df.iloc[i]["timestamp"],
            })

        if current_low == min(low_slice):
            swing_lows.append({
                "index": i,
                "price": current_low,
                "timestamp": df.iloc[i]["timestamp"],
            })

    return {
        "swing_highs": swing_highs,
        "swing_lows": swing_lows,
    }



def detect_market_structure(
    df: Optional[pd.DataFrame],
    swing_window: int = 3,
) -> Dict[str, object]:
    empty_result: Dict[str, object] = {
        "swing_highs": [],
        "swing_lows": [],
        "last_high": None,
        "prev_high": None,
        "last_low": None,
        "prev_low": None,
        "high_structure": "unknown",
        "low_structure": "unknown",
        "market_structure": "unknown",
        "bias": "neutral",
        "bos_bullish": False,
        "bos_bearish": False,
        "sweep_bullish": False,
        "sweep_bearish": False,
        "failed_breakout_bullish": False,
        "failed_breakout_bearish": False,
        "displacement_bullish": False,
        "displacement_bearish": False,
        "swing_strength": "neutral",
        "impulse_ratio": 1.0,
    }

    if df is None or len(df) < (swing_window * 2 + 5):
        return empty_result

    swings = _extract_confirmed_swing_points(df, window=swing_window)
    swing_highs = swings["swing_highs"]
    swing_lows = swings["swing_lows"]

    result: Dict[str, object] = {
        **empty_result,
        "swing_highs": swing_highs,
        "swing_lows": swing_lows,
    }

    if len(swing_highs) >= 2:
        prev_high = swing_highs[-2]
        last_high = swing_highs[-1]
        result["prev_high"] = prev_high
        result["last_high"] = last_high

        if float(last_high["price"]) > float(prev_high["price"]):
            result["high_structure"] = "HH"
        elif float(last_high["price"]) < float(prev_high["price"]):
            result["high_structure"] = "LH"
        else:
            result["high_structure"] = "EH"

    if len(swing_lows) >= 2:
        prev_low = swing_lows[-2]
        last_low = swing_lows[-1]
        result["prev_low"] = prev_low
        result["last_low"] = last_low

        if float(last_low["price"]) > float(prev_low["price"]):
            result["low_structure"] = "HL"
        elif float(last_low["price"]) < float(prev_low["price"]):
            result["low_structure"] = "LL"
        else:
            result["low_structure"] = "EL"

    structure_strength = analyze_structure_strength(df, swings)
    result.update(structure_strength)

    high_structure = result["high_structure"]
    low_structure = result["low_structure"]
    swing_strength = str(result.get("swing_strength", "neutral"))

    if high_structure == "HH" and low_structure == "HL":
        result["market_structure"] = "bullish"
        result["bias"] = "long"
    elif high_structure == "LH" and low_structure == "LL":
        result["market_structure"] = "bearish"
        result["bias"] = "short"
    elif high_structure == "unknown" or low_structure == "unknown":
        if swing_strength == "bullish":
            result["market_structure"] = "developing_bullish"
            result["bias"] = "voorzichtig_long"
        elif swing_strength == "bearish":
            result["market_structure"] = "developing_bearish"
            result["bias"] = "voorzichtig_short"
        else:
            result["market_structure"] = "unknown"
            result["bias"] = "neutral"
    else:
        if swing_strength == "bullish":
            result["market_structure"] = "mixed_bullish"
            result["bias"] = "voorzichtig_long"
        elif swing_strength == "bearish":
            result["market_structure"] = "mixed_bearish"
            result["bias"] = "voorzichtig_short"
        else:
            result["market_structure"] = "mixed"
            result["bias"] = "neutral"

    return result


# =========================================================
# Signal stability
# =========================================================
def get_zone_width_pct(vol_profile: Dict[str, float | str]) -> float:
    avg_range_pct = float(vol_profile["avg_range_pct"])
    return max(0.20, round(avg_range_pct * 0.35, 2))


def compute_raw_market_signal(
    price: float,
    support: Optional[float],
    resistance: Optional[float],
    zone_width_pct: float
) -> str:
    if support is None or resistance is None or resistance <= support:
        return "onbekend"

    support_zone_top = support * (1 + zone_width_pct / 100)
    resistance_zone_bottom = resistance * (1 - zone_width_pct / 100)

    if price <= support_zone_top:
        return "laag"
    if price >= resistance_zone_bottom:
        return "hoog"
    return "midden"


def update_stable_signal(signal_key: str, raw_signal: str, confirmations_needed: int = 3) -> str:
    candidate_key = f"{signal_key}_candidate"
    count_key = f"{signal_key}_count"
    stable_key = f"{signal_key}_stable"

    if stable_key not in st.session_state:
        st.session_state[stable_key] = raw_signal
        st.session_state[candidate_key] = raw_signal
        st.session_state[count_key] = 1
        return raw_signal

    stable = st.session_state[stable_key]
    candidate = st.session_state[candidate_key]
    count = st.session_state[count_key]

    if raw_signal == stable:
        st.session_state[candidate_key] = raw_signal
        st.session_state[count_key] = 1
        return stable

    if raw_signal == candidate:
        count += 1
    else:
        candidate = raw_signal
        count = 1

    if count >= confirmations_needed:
        stable = raw_signal
        candidate = raw_signal
        count = 1

    st.session_state[stable_key] = stable
    st.session_state[candidate_key] = candidate
    st.session_state[count_key] = count

    return stable


# =========================================================
# Fee / trade calculation
# =========================================================
def get_fee_pct_from_type(fee_type: str, maker_fee_pct: float, taker_fee_pct: float) -> float:
    return maker_fee_pct if fee_type == "maker" else taker_fee_pct


def calculate_short_borrow_fee_eur(
    entry: float,
    target: float,
    position_size: float,
    borrow_hourly_pct: float,
    expected_hold_hours: float,
) -> float:
    if entry <= 0 or position_size <= 0 or borrow_hourly_pct <= 0 or expected_hold_hours <= 0:
        return 0.0

    estimated_notional = max(float(entry), float(target)) * float(position_size)
    return estimated_notional * (float(borrow_hourly_pct) / 100.0) * float(expected_hold_hours)


def calculate_liquidation_fee_eur(collateral_eur: float, liquidation_fee_pct: float) -> float:
    if collateral_eur <= 0 or liquidation_fee_pct <= 0:
        return 0.0
    return float(collateral_eur) * (float(liquidation_fee_pct) / 100.0)


def calculate_trade_metrics(
    side: str,
    entry: Optional[float],
    stop: Optional[float],
    target: Optional[float],
    account_size: float,
    max_risk_pct: float,
    coin_symbol: str,
    entry_fee_pct: float,
    exit_fee_pct: float,
    short_borrow_hourly_pct: float = 0.0,
    expected_hold_hours: float = 0.0,
    short_liquidation_fee_pct: float = DEFAULT_SHORT_LIQUIDATION_FEE_PCT,
) -> Optional[Dict[str, float | str]]:
    if entry is None or stop is None or target is None:
        return None

    entry = float(entry)
    stop = float(stop)
    target = float(target)

    if entry <= 0 or stop <= 0 or target <= 0 or account_size <= 0:
        return None

    desired_risk_eur = account_size * (max_risk_pct / 100)

    if side == "long":
        risk_per_coin = entry - stop
        reward_per_coin = target - entry
    else:
        risk_per_coin = stop - entry
        reward_per_coin = entry - target

    if risk_per_coin <= 0 or reward_per_coin <= 0:
        return None

    raw_position_size = desired_risk_eur / risk_per_coin
    max_position_size_by_account = account_size / entry
    position_size = min(raw_position_size, max_position_size_by_account)

    if position_size <= 0:
        return None

    entry_notional = entry * position_size
    exit_notional = target * position_size
    gross_profit_eur = reward_per_coin * position_size

    entry_fee_eur = entry_notional * (entry_fee_pct / 100)
    exit_fee_eur = exit_notional * (exit_fee_pct / 100)
    borrow_fee_eur = 0.0
    estimated_liquidation_fee_eur = 0.0

    if side == "short":
        borrow_fee_eur = calculate_short_borrow_fee_eur(
            entry=entry,
            target=target,
            position_size=position_size,
            borrow_hourly_pct=short_borrow_hourly_pct,
            expected_hold_hours=expected_hold_hours,
        )
        estimated_liquidation_fee_eur = calculate_liquidation_fee_eur(account_size, short_liquidation_fee_pct)

    total_fees_eur = entry_fee_eur + exit_fee_eur + borrow_fee_eur

    actual_risk_eur = risk_per_coin * position_size
    stop_notional = stop * position_size
    stop_exit_fee_eur = stop_notional * (exit_fee_pct / 100)
    stop_borrow_fee_eur = borrow_fee_eur if side == "short" else 0.0
    net_loss_if_stopped_eur = actual_risk_eur + entry_fee_eur + stop_exit_fee_eur + stop_borrow_fee_eur

    net_profit_eur = gross_profit_eur - total_fees_eur

    risk_pct_price = (risk_per_coin / entry) * 100
    reward_pct_price = (reward_per_coin / entry) * 100
    rr = reward_pct_price / risk_pct_price if risk_pct_price > 0 else 0

    return {
        "side": side,
        "entry": entry,
        "stop": stop,
        "target": target,
        "risk_eur": actual_risk_eur,
        "desired_risk_eur": desired_risk_eur,
        "position_size": position_size,
        "entry_notional_eur": entry_notional,
        "gross_profit_eur": gross_profit_eur,
        "entry_fee_eur": entry_fee_eur,
        "exit_fee_eur": exit_fee_eur,
        "borrow_fee_eur": borrow_fee_eur,
        "estimated_liquidation_fee_eur": estimated_liquidation_fee_eur,
        "expected_hold_hours": expected_hold_hours,
        "short_borrow_hourly_pct": short_borrow_hourly_pct,
        "total_fees_eur": total_fees_eur,
        "net_profit_eur": net_profit_eur,
        "risk_pct_price": risk_pct_price,
        "reward_pct_price": reward_pct_price,
        "rr": rr,
        "coin_symbol": coin_symbol,
        "net_loss_if_stopped_eur": net_loss_if_stopped_eur,
    }


def calculate_conservative_net_profit(metrics: Optional[Dict[str, float | str]], taker_fee_pct: float) -> Optional[float]:
    if metrics is None:
        return None

    entry = float(metrics["entry"])
    target = float(metrics["target"])
    position_size = float(metrics["position_size"])
    gross_profit_eur = float(metrics["gross_profit_eur"])
    borrow_fee_eur = float(metrics.get("borrow_fee_eur", 0.0))

    conservative_entry_fee_eur = entry * position_size * (taker_fee_pct / 100)
    conservative_exit_fee_eur = target * position_size * (taker_fee_pct / 100)
    conservative_total_fees_eur = conservative_entry_fee_eur + conservative_exit_fee_eur + borrow_fee_eur

    return gross_profit_eur - conservative_total_fees_eur



MIN_RR_HARD_FILTER = 1.00
MIN_DISTANCE_TO_TARGET_PCT = 0.35
RELAXED_PROFIT_BUFFER_FACTOR = 0.25


def passes_hard_filters(
    side: str,
    metrics: Optional[Dict[str, float | str]],
    timing_label: str,
    location_info: Optional[Dict[str, Optional[float] | str]],
    min_profit_buffer_eur: float,
    taker_fee_pct: float,
    min_rr: float = MIN_RR_HARD_FILTER,
    min_distance_to_target_pct: float = MIN_DISTANCE_TO_TARGET_PCT,
) -> Tuple[bool, str]:
    if metrics is None:
        return False, "Geen metrics"

    conservative_net = calculate_conservative_net_profit(metrics, taker_fee_pct)
    if conservative_net is None:
        return False, "Geen conservatief netto"

    rr = float(metrics.get("rr", 0.0))
    effective_profit_buffer = max(0.0, float(min_profit_buffer_eur) * RELAXED_PROFIT_BUFFER_FACTOR)

    if rr < min_rr:
        return False, f"RR te laag ({rr:.2f} < {min_rr:.2f})"

    if conservative_net < effective_profit_buffer:
        return False, "Conservatief netto te laag"

    if str(timing_label) == "BLOCKED":
        return False, "Trigger timing blocked"

    location_quality = str((location_info or {}).get("quality", "UNKNOWN"))
    if location_quality == "SKIP":
        return False, "Location skip"

    distance_to_target_pct = (location_info or {}).get("distance_to_target_pct", None)
    if distance_to_target_pct is not None and float(distance_to_target_pct) < min_distance_to_target_pct:
        return False, "Te dicht bij target"

    return True, "OK"


def is_setup_valid(metrics: Optional[Dict[str, float | str]], min_profit_buffer_eur: float, taker_fee_pct: float) -> bool:
    if metrics is None:
        return False

    conservative_net_profit_eur = calculate_conservative_net_profit(metrics, taker_fee_pct)
    if conservative_net_profit_eur is None:
        return False

    effective_profit_buffer = max(0.0, float(min_profit_buffer_eur) * RELAXED_PROFIT_BUFFER_FACTOR)
    return conservative_net_profit_eur >= effective_profit_buffer and float(metrics["rr"]) > 0.8


# =========================================================
# Multi-timeframe helpers
# =========================================================


def build_confirmed_plan(
    side: str,
    current_price: Optional[float],
    support: Optional[float],
    resistance: Optional[float],
    stop_level: Optional[float],
    target: Optional[float],
    trigger_df: Optional[pd.DataFrame],
    entry_buffer_pct: float,
    account_size: float,
    max_risk_pct: float,
    coin_symbol: str,
    entry_fee_pct: float,
    exit_fee_pct: float,
    min_profit_buffer_eur: float,
    taker_fee_pct: float,
    trigger_vol_profile: Optional[Dict[str, float | str]] = None,
    short_borrow_hourly_pct: float = 0.0,
    expected_hold_hours: float = 0.0,
    short_liquidation_fee_pct: float = DEFAULT_SHORT_LIQUIDATION_FEE_PCT,
) -> Dict[str, object]:
    result: Dict[str, object] = {
        "status": "WAIT",
        "reason": "",
        "entry": None,
        "stop": None,
        "target": None,
        "metrics": None,
        "valid": False,
        "trigger": None,
        "confirmation": {},
    }

    if current_price is None or support is None or resistance is None or stop_level is None or target is None:
        result["reason"] = "Onvoldoende data voor confirmed plan."
        return result

    entry_long, entry_short, long_trigger, short_trigger = choose_entry_prices(
        current_price=float(current_price),
        support=float(support),
        resistance=float(resistance),
        entry_buffer_pct=entry_buffer_pct,
        entry_mode="confirmation",
        df=trigger_df,
    )

    if side.lower() == "long":
        entry = float(entry_long)
        trigger_value = long_trigger
        stop = float(stop_level)
        confirmation_info = analyze_price_action_confirmation(
            df=trigger_df,
            side="long",
            zone_level=support,
            current_price=current_price,
            reclaim_trigger=trigger_value,
            vol_profile=trigger_vol_profile,
        )
        metrics = calculate_trade_metrics(
            side="long",
            entry=entry,
            stop=stop,
            target=float(target),
            account_size=account_size,
            max_risk_pct=max_risk_pct,
            coin_symbol=coin_symbol,
            entry_fee_pct=entry_fee_pct,
            exit_fee_pct=exit_fee_pct,
            short_borrow_hourly_pct=0.0,
            expected_hold_hours=0.0,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
        )
    else:
        entry = float(entry_short)
        trigger_value = short_trigger
        stop = float(stop_level)
        confirmation_info = analyze_price_action_confirmation(
            df=trigger_df,
            side="short",
            zone_level=resistance,
            current_price=current_price,
            reclaim_trigger=trigger_value,
            vol_profile=trigger_vol_profile,
        )
        metrics = calculate_trade_metrics(
            side="short",
            entry=entry,
            stop=stop,
            target=float(target),
            account_size=account_size,
            max_risk_pct=max_risk_pct,
            coin_symbol=coin_symbol,
            entry_fee_pct=entry_fee_pct,
            exit_fee_pct=exit_fee_pct,
            short_borrow_hourly_pct=short_borrow_hourly_pct,
            expected_hold_hours=expected_hold_hours,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
        )

    valid = is_setup_valid(metrics, min_profit_buffer_eur, taker_fee_pct) and bool(confirmation_info.get("confirmed", False))

    ready_reason = str(confirmation_info.get("reason", "Prijsactie bevestigt de move."))
    wait_reason = "Confirmed plan wacht op bullish/bearish reject, close terug in de zone of reclaim."
    if metrics is not None and not bool(confirmation_info.get("confirmed", False)):
        wait_reason = f"Nog geen prijsactie-bevestiging: {ready_reason}"
    elif metrics is None:
        wait_reason = "Confirmed plan haalt netto eisen nog niet."

    result.update({
        "status": "CONFIRMED_READY" if valid else "WAIT",
        "reason": ready_reason if valid else wait_reason,
        "entry": entry,
        "stop": stop,
        "target": float(target),
        "metrics": metrics,
        "valid": valid,
        "trigger": trigger_value,
        "confirmation": confirmation_info,
    })
    return result

def get_timeframe_package(market: str, timeframe_label: str, reference_price: Optional[float] = None):
    df = get_bitvavo_candle_dataframe(
        market,
        interval=TIMEFRAMES[timeframe_label],
        limit=180
    )
    levels = detect_swing_levels(df, reference_price=reference_price)
    vol = calculate_volatility_profile(df)
    return df, levels, vol



def get_hierarchy_packages(
    market: str,
    base_timeframe_label: str,
    reference_price: Optional[float] = None,
) -> Dict[str, Dict[str, object]]:
    """
    Haal trigger/setup/trend packages op op basis van TIMEFRAME_HIERARCHY.
    """
    hierarchy = TIMEFRAME_HIERARCHY.get(base_timeframe_label, {
        "trigger": base_timeframe_label,
        "setup": base_timeframe_label,
        "trend": HIGHER_TIMEFRAME_MAP.get(base_timeframe_label, base_timeframe_label),
    })

    trigger_label = hierarchy["trigger"]
    setup_label = hierarchy["setup"]
    trend_label = hierarchy["trend"]

    trigger_df, trigger_levels, trigger_vol = get_timeframe_package(market, trigger_label, reference_price=reference_price)
    setup_df, setup_levels, setup_vol = get_timeframe_package(market, setup_label, reference_price=reference_price)
    trend_df, trend_levels, trend_vol = get_timeframe_package(market, trend_label, reference_price=reference_price)

    return {
        "trigger": {"label": trigger_label, "df": trigger_df, "levels": trigger_levels, "vol": trigger_vol},
        "setup": {"label": setup_label, "df": setup_df, "levels": setup_levels, "vol": setup_vol},
        "trend": {"label": trend_label, "df": trend_df, "levels": trend_levels, "vol": trend_vol},
    }

def extract_primary_levels(levels: Dict[str, List[float]]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    trade_support = levels["trade_supports"][0] if levels.get("trade_supports") else None
    trade_resistance = levels["trade_resistances"][0] if levels.get("trade_resistances") else None
    hard_support = levels["hard_supports"][0] if levels.get("hard_supports") else trade_support
    hard_resistance = levels["hard_resistances"][0] if levels.get("hard_resistances") else trade_resistance
    return trade_support, trade_resistance, hard_support, hard_resistance


def get_nearest_level_below_price(levels: Optional[List[float]], reference_price: Optional[float]) -> Optional[float]:
    if levels is None or reference_price is None:
        return None
    valid = [float(level) for level in levels if level is not None and float(level) < float(reference_price)]
    if not valid:
        return None
    return max(valid)


def get_nearest_level_above_price(levels: Optional[List[float]], reference_price: Optional[float]) -> Optional[float]:
    if levels is None or reference_price is None:
        return None
    valid = [float(level) for level in levels if level is not None and float(level) > float(reference_price)]
    if not valid:
        return None
    return min(valid)


def select_levels_around_price(levels: Dict[str, List[float]], reference_price: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    trade_support = get_nearest_level_below_price(levels.get("trade_supports"), reference_price)
    trade_resistance = get_nearest_level_above_price(levels.get("trade_resistances"), reference_price)
    hard_support = get_nearest_level_below_price(levels.get("hard_supports"), reference_price)
    hard_resistance = get_nearest_level_above_price(levels.get("hard_resistances"), reference_price)

    if hard_support is None:
        hard_support = trade_support
    if hard_resistance is None:
        hard_resistance = trade_resistance

    return trade_support, trade_resistance, hard_support, hard_resistance


def select_target_level(
    side: str,
    reference_price: Optional[float],
    local_trade_level: Optional[float],
    higher_trade_level: Optional[float],
    min_distance_pct: float = MIN_DISTANCE_TO_TARGET_PCT,
) -> Optional[float]:
    if reference_price is None:
        return None

    candidates: List[float] = []
    for level in [local_trade_level, higher_trade_level]:
        if level is None:
            continue
        level = float(level)
        distance_pct = abs(level - float(reference_price)) / float(reference_price) * 100 if float(reference_price) else 0.0

        if side == "long" and level > float(reference_price) and distance_pct >= min_distance_pct:
            candidates.append(level)
        elif side == "short" and level < float(reference_price) and distance_pct >= min_distance_pct:
            candidates.append(level)

    if not candidates:
        return None

    return min(candidates) if side == "long" else max(candidates)


def compute_bias_from_levels(
    price: float,
    support: Optional[float],
    resistance: Optional[float],
    vol_profile: Dict[str, float | str],
    signal_key: str,
) -> str:
    if support is None or resistance is None:
        return "onbekend"

    zone_width_pct = get_zone_width_pct(vol_profile)
    raw_signal = compute_raw_market_signal(price, support, resistance, zone_width_pct)
    stable_signal = update_stable_signal(signal_key, raw_signal, confirmations_needed=3)
    return stable_signal


def determine_market_context(
    entry_structure: Dict[str, object],
    higher_structure: Dict[str, object],
    current_price: Optional[float] = None,
    support: Optional[float] = None,
    resistance: Optional[float] = None,
    vol_profile: Optional[Dict[str, float | str]] = None,
) -> Dict[str, str]:
    entry_market = str(entry_structure.get("market_structure", "unknown"))
    higher_market = str(higher_structure.get("market_structure", "unknown"))

    result = {"combined_bias": "neutraal", "context": "neutral", "label": "Neutraal"}

    near_support = False
    near_resistance = False
    if current_price is not None and support is not None and resistance is not None and resistance > support:
        zone_width_pct = get_zone_width_pct(vol_profile or {"avg_range_pct": 1.0, "vol_label": "onbekend"})
        watch_buffer_pct = zone_width_pct + 0.35
        support_zone_top = float(support) * (1 + watch_buffer_pct / 100)
        resistance_zone_bottom = float(resistance) * (1 - watch_buffer_pct / 100)
        near_support = float(current_price) <= support_zone_top
        near_resistance = float(current_price) >= resistance_zone_bottom

    bullish_entry_states = {"bullish", "mixed_bullish", "developing_bullish"}
    bearish_entry_states = {"bearish", "mixed_bearish", "developing_bearish"}
    bullish_higher_states = {"bullish", "mixed_bullish", "developing_bullish"}
    bearish_higher_states = {"bearish", "mixed_bearish", "developing_bearish"}

    if higher_market in bullish_higher_states and entry_market in bullish_entry_states:
        result.update({"combined_bias": "long", "context": "aligned_bullish", "label": "Bullish"})
    elif higher_market in bearish_higher_states and entry_market in bearish_entry_states:
        result.update({"combined_bias": "short", "context": "aligned_bearish", "label": "Bearish"})
    elif higher_market in bearish_higher_states and entry_market in bullish_entry_states:
        result.update({"combined_bias": "short", "context": "bearish_pullback", "label": "Bearish pullback"})
    elif higher_market in bullish_higher_states and entry_market in bearish_entry_states:
        result.update({"combined_bias": "long", "context": "bullish_pullback", "label": "Bullish pullback"})
    elif higher_market in bullish_higher_states and entry_market in {"mixed", "unknown"}:
        if near_support:
            result.update({"combined_bias": "long", "context": "mixed_bullish_near_support", "label": "Bullish + near support"})
        else:
            result.update({"combined_bias": "voorzichtig", "context": "bullish_wait", "label": "Bullish / wacht op pullback"})
    elif higher_market in bearish_higher_states and entry_market in {"mixed", "unknown"}:
        if near_resistance:
            result.update({"combined_bias": "short", "context": "mixed_bearish_near_resistance", "label": "Bearish + near resistance"})
        else:
            result.update({"combined_bias": "voorzichtig", "context": "bearish_wait", "label": "Bearish / wacht op retest"})
    elif higher_market == "mixed" and entry_market == "bullish":
        result.update({"combined_bias": "voorzichtig", "context": "speculative_bullish", "label": "Voorzichtig bullish"})
    elif higher_market == "mixed" and entry_market == "bearish":
        result.update({"combined_bias": "voorzichtig", "context": "speculative_bearish", "label": "Voorzichtig bearish"})
    elif higher_market == "mixed" or entry_market == "mixed":
        result.update({"combined_bias": "voorzichtig", "context": "mixed", "label": "Voorzichtig / mixed"})

    return result


def combine_multi_timeframe_bias(
    entry_structure: Dict[str, object],
    higher_structure: Dict[str, object],
    current_price: Optional[float] = None,
    support: Optional[float] = None,
    resistance: Optional[float] = None,
    vol_profile: Optional[Dict[str, float | str]] = None,
) -> str:
    return determine_market_context(
        entry_structure,
        higher_structure,
        current_price=current_price,
        support=support,
        resistance=resistance,
        vol_profile=vol_profile,
    )["combined_bias"]


# =========================================================
# Chart rendering
# =========================================================
def render_price_chart(
    df: pd.DataFrame,
    trade_supports: Optional[List[float]] = None,
    trade_resistances: Optional[List[float]] = None,
    hard_supports: Optional[List[float]] = None,
    hard_resistances: Optional[List[float]] = None,
    higher_trade_support: Optional[float] = None,
    higher_trade_resistance: Optional[float] = None,
    active_support: Optional[float] = None,
    active_resistance: Optional[float] = None,
    height: int = 650,
) -> None:
    fig = go.Figure()

    fig.add_trace(
        go.Candlestick(
            x=df["timestamp"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name="Prijs",
        )
    )

    fig.add_trace(
        go.Bar(
            x=df["timestamp"],
            y=df["volume"],
            name="Volume",
            yaxis="y2",
            opacity=0.25,
        )
    )

    def _same_level(level_a: Optional[float], level_b: Optional[float], tol_pct: float = 0.08) -> bool:
        if level_a is None or level_b is None:
            return False
        ref = abs(float(level_b)) if float(level_b) != 0 else 1.0
        return abs(float(level_a) - float(level_b)) / ref * 100 <= tol_pct

    def _unique_levels(levels: List[Optional[float]], tol_pct: float = 0.08) -> List[float]:
        cleaned: List[float] = []
        for lvl in levels:
            if lvl is None:
                continue
            if any(_same_level(lvl, existing, tol_pct=tol_pct) for existing in cleaned):
                continue
            cleaned.append(float(lvl))
        return cleaned

    support_active_color = "#3B82F6"
    support_muted_color = "rgba(59,130,246,0.45)"
    resistance_active_color = "#FACC15"
    resistance_muted_color = "rgba(250,204,21,0.45)"

    unique_hard_supports = _unique_levels(list(hard_supports or []))
    unique_hard_resistances = _unique_levels(list(hard_resistances or []))
    unique_trade_supports = _unique_levels(list(trade_supports or []))
    unique_trade_resistances = _unique_levels(list(trade_resistances or []))

    for i, lvl in enumerate(unique_hard_supports[:2], start=1):
        is_active = _same_level(lvl, active_support)
        fig.add_hline(
            y=lvl,
            line_width=3.4 if is_active else 2.6,
            line_dash="solid",
            line_color=support_active_color if is_active else "rgba(59,130,246,0.90)",
            annotation_text=f"Hard Support {i} {lvl:,.2f}",
            annotation_position="bottom left",
            annotation_font_color="white",
        )

    for i, lvl in enumerate(unique_hard_resistances[:2], start=1):
        is_active = _same_level(lvl, active_resistance)
        fig.add_hline(
            y=lvl,
            line_width=3.4 if is_active else 2.6,
            line_dash="solid",
            line_color=resistance_active_color if is_active else "rgba(250,204,21,0.90)",
            annotation_text=f"Hard Resistance {i} {lvl:,.2f}",
            annotation_position="top left",
            annotation_font_color="white",
        )

    plotted_trade_supports = []
    for lvl in unique_trade_supports[:2]:
        if any(_same_level(lvl, hs) for hs in unique_hard_supports):
            continue
        plotted_trade_supports.append(lvl)

    plotted_trade_resistances = []
    for lvl in unique_trade_resistances[:2]:
        if any(_same_level(lvl, hr) for hr in unique_hard_resistances):
            continue
        plotted_trade_resistances.append(lvl)

    for i, lvl in enumerate(plotted_trade_supports, start=1):
        is_active = _same_level(lvl, active_support)
        fig.add_hline(
            y=lvl,
            line_width=2.4 if is_active else 1.3,
            line_dash="dot",
            line_color=support_active_color if is_active else support_muted_color,
            annotation_text=f"Entry TF Support {i} {lvl:,.2f}",
            annotation_position="bottom right",
            annotation_font_color="white",
        )

    for i, lvl in enumerate(plotted_trade_resistances, start=1):
        is_active = _same_level(lvl, active_resistance)
        fig.add_hline(
            y=lvl,
            line_width=2.4 if is_active else 1.3,
            line_dash="dot",
            line_color=resistance_active_color if is_active else resistance_muted_color,
            annotation_text=f"Entry TF Resistance {i} {lvl:,.2f}",
            annotation_position="top right",
            annotation_font_color="white",
        )

    if higher_trade_support is not None:
        support_is_duplicate = any(_same_level(higher_trade_support, lvl) for lvl in (unique_hard_supports + unique_trade_supports))
        if not support_is_duplicate:
            fig.add_hline(
                y=higher_trade_support,
                line_width=1.2,
                line_dash="dot",
                line_color="rgba(96,165,250,0.45)",
                annotation_text=f"Higher TF Support {higher_trade_support:,.2f}",
                annotation_position="bottom left",
                annotation_font_color="white",
            )

    if higher_trade_resistance is not None:
        resistance_is_duplicate = any(_same_level(higher_trade_resistance, lvl) for lvl in (unique_hard_resistances + unique_trade_resistances))
        if not resistance_is_duplicate:
            fig.add_hline(
                y=higher_trade_resistance,
                line_width=1.2,
                line_dash="dot",
                line_color="rgba(250,204,21,0.35)",
                annotation_text=f"Higher TF Resistance {higher_trade_resistance:,.2f}",
                annotation_position="top left",
                annotation_font_color="white",
            )

    fig.update_layout(
        height=height,
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        margin=dict(l=10, r=10, t=20, b=10),
        yaxis=dict(title="Prijs"),
        yaxis2=dict(
            title="Volume",
            overlaying="y",
            side="right",
            showgrid=False,
            rangemode="tozero",
            position=1.0,
        ),
    )

    st.plotly_chart(fig, use_container_width=True)



# =========================================================
# Scanner helpers
# =========================================================
@st.cache_data(ttl=SCANNER_CACHE_SEC, show_spinner=False)
def get_bitvavo_all_prices() -> Dict[str, float]:
    url = f"{BASE_URL}{API_PREFIX}/ticker/price"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            return {}
        return {
            item["market"]: float(item["price"])
            for item in data
            if isinstance(item, dict) and "market" in item and "price" in item
        }
    except Exception:
        return {}


def get_live_price_for_market(market: str) -> Optional[float]:
    all_prices = get_bitvavo_all_prices()
    if market in all_prices:
        return all_prices[market]
    return get_bitvavo_price(market)


def compute_bias_snapshot(
    price: float,
    support: Optional[float],
    resistance: Optional[float],
    vol_profile: Dict[str, float | str],
) -> str:
    if support is None or resistance is None:
        return "onbekend"
    zone_width_pct = get_zone_width_pct(vol_profile)
    return compute_raw_market_signal(price, support, resistance, zone_width_pct)


def calculate_setup_score(
    best_metrics: Optional[Dict[str, float | str]],
    best_side: Optional[str],
    combined_bias: str,
    taker_fee_pct: float,
) -> float:
    if best_metrics is None or best_side is None:
        return 0.0

    conservative_net = calculate_conservative_net_profit(best_metrics, taker_fee_pct)
    if conservative_net is None:
        return 0.0

    rr = float(best_metrics["rr"])
    bias_bonus = 0
    if (best_side == "LONG" and combined_bias == "long") or (best_side == "SHORT" and combined_bias == "short"):
        bias_bonus = 18
    elif combined_bias == "voorzichtig":
        bias_bonus = 8

    fee_quality_bonus = 8 if float(best_metrics["net_profit_eur"]) > float(best_metrics["total_fees_eur"]) else 0
    score = (max(0.0, conservative_net) * 2.2) + (rr * 11.0) + bias_bonus + fee_quality_bonus
    return round(min(100.0, score), 1)



def timing_to_score(timing: str) -> float:
    return {
        "READY": 18.0,
        "NEAR": 10.0,
        "WATCH": 2.0,
        "LOW PRIORITY": -6.0,
        "MISSED": -12.0,
        "BLOCKED": -100.0,
        "geen data": -25.0,
    }.get(str(timing), -8.0)


def score_trade_candidate(
    side: str,
    metrics: Optional[Dict[str, float | str]],
    timing_label: str,
    combined_bias: str,
    market_context: str,
    taker_fee_pct: float,
) -> float:
    if metrics is None:
        return -10_000.0

    conservative_net = calculate_conservative_net_profit(metrics, taker_fee_pct)
    if conservative_net is None:
        return -10_000.0

    rr = float(metrics.get("rr", 0.0))
    score = 0.0

    score += max(-15.0, min(35.0, float(conservative_net) * 2.0))
    score += max(-10.0, min(22.0, rr * 8.0))
    score += timing_to_score(timing_label)

    if side == "LONG":
        if combined_bias == "long":
            score += 16.0
        elif combined_bias == "short":
            score -= 18.0
        elif combined_bias == "voorzichtig":
            score -= 2.0

        if market_context in {"bullish_pullback", "mixed_bullish_near_support"}:
            score += 16.0
        elif market_context == "aligned_bullish":
            score += 12.0
        elif market_context == "bearish_pullback":
            score -= 22.0
        elif market_context == "aligned_bearish":
            score -= 28.0

    else:
        if combined_bias == "short":
            score += 16.0
        elif combined_bias == "long":
            score -= 18.0
        elif combined_bias == "voorzichtig":
            score -= 2.0

        if market_context in {"bearish_pullback", "mixed_bearish_near_resistance"}:
            score += 16.0
        elif market_context == "aligned_bearish":
            score += 12.0
        elif market_context == "bullish_pullback":
            score -= 22.0
        elif market_context == "aligned_bullish":
            score -= 28.0

    if timing_label == "BLOCKED":
        score -= 200.0

    freshness = metrics.get("freshness", {}) if isinstance(metrics, dict) else {}
    score -= float(freshness.get("penalty_score", 0.0) or 0.0)

    return round(score, 2)



def classify_setup_grade(
    best_metrics: Optional[Dict[str, float | str]],
    score: float,
    status: str,
    long_hard_reason: str = "",
    short_hard_reason: str = "",
) -> str:
    if best_metrics is not None:
        if score >= 55:
            return "GOOD"
        if score >= 30:
            return "OK"
        return "WEAK"

    joined = f"{long_hard_reason} | {short_hard_reason}".lower()
    if "geen data" in status.lower():
        return "NO DATA"
    if "rr te laag" in joined or "conservatief netto te laag" in joined or "te dicht bij target" in joined:
        return "WEAK"
    if "wachten" in status.lower():
        return "OK"
    return "WEAK"

def compute_setup_freshness(
    current_price: Optional[float],
    entry: Optional[float],
    target: Optional[float],
    df: Optional[pd.DataFrame],
    side: str,
    timeframe_label: str,
) -> Dict[str, object]:
    result = {
        "progress_pct": None,
        "is_stale": False,
        "severity": "fresh",
        "penalty_score": 0.0,
        "reason": "",
    }

    if current_price is None or entry is None or target is None:
        return result

    total_move = abs(float(target) - float(entry))
    if total_move <= 0:
        return result

    if side == "long":
        progress = ((float(current_price) - float(entry)) / total_move) * 100
    else:
        progress = ((float(entry) - float(current_price)) / total_move) * 100

    result["progress_pct"] = progress

    recent_window = {
        "1m": 12,
        "5m": 8,
        "15m": 6,
        "30m": 5,
        "1h": 4,
        "4h": 3,
        "1d": 2,
    }.get(timeframe_label, 6)

    if df is not None and len(df) > 0:
        recent = df.tail(min(len(df), recent_window))
        if side == "long":
            recent_extreme_hit = float(recent["high"].max()) >= float(target)
        else:
            recent_extreme_hit = float(recent["low"].min()) <= float(target)
    else:
        recent_extreme_hit = False

    if recent_extreme_hit:
        result["is_stale"] = True
        result["severity"] = "hard_stale"
        result["penalty_score"] = 120.0
        result["reason"] = "winstdoel recent al geraakt"
    elif progress >= 75:
        result["is_stale"] = True
        result["severity"] = "hard_stale"
        result["penalty_score"] = 90.0
        result["reason"] = "move vrijwel volledig geweest"
    elif progress >= 60:
        result["is_stale"] = True
        result["severity"] = "stale"
        result["penalty_score"] = 55.0
        result["reason"] = "move al grotendeels geweest"
    elif progress >= 40:
        result["severity"] = "aging"
        result["penalty_score"] = 20.0
        result["reason"] = "move is al deels onderweg"

    return result




def choose_prelimit_zone(
    side: str,
    combined_bias: str,
    market_context: str,
    trade_level: Optional[float],
    hard_level: Optional[float],
) -> Optional[float]:
    """
    Gebruik hard support/resistance als pre-limit zone wanneer de marktcontext
    die richting ondersteunt of op z'n minst niet duidelijk tegenwerkt.
    """
    side = str(side).lower()

    bullish_contexts = {"aligned_bullish", "bullish_pullback", "bullish_wait", "speculative_bullish", "mixed", "mixed_bullish_near_support"}
    bearish_contexts = {"aligned_bearish", "bearish_pullback", "bearish_wait", "speculative_bearish", "mixed", "mixed_bearish_near_resistance"}

    if side == "long":
        allow_hard_zone = (
            hard_level is not None
            and combined_bias != "short"
            and market_context in bullish_contexts
        )
        return hard_level if allow_hard_zone else trade_level

    allow_hard_zone = (
        hard_level is not None
        and combined_bias != "long"
        and market_context in bearish_contexts
    )
    return hard_level if allow_hard_zone else trade_level

def analyze_coin_setup(
    coin: str,
    timeframe_label: str,
    account_size: float,
    min_profit_buffer_eur: float,
    target_mode: str,
    maker_fee_pct: float,
    taker_fee_pct: float,
    entry_fee_type: str,
    exit_fee_type: str,
    short_borrow_hourly_pct_map: Dict[str, float],
    expected_hold_hours: float,
    short_liquidation_fee_pct: float,
    override_price: Optional[float] = None,
    entry_mode: str = "limit",
) -> Dict[str, object]:
    market = COINS[coin]["bitvavo_market"]

    live_price = get_live_price_for_market(market)
    current_price = override_price if override_price is not None else live_price

    hierarchy_packages = get_hierarchy_packages(market, timeframe_label, reference_price=current_price)
    trigger_pkg = hierarchy_packages["trigger"]
    setup_pkg = hierarchy_packages["setup"]
    trend_pkg = hierarchy_packages["trend"]

    trigger_timeframe_label = str(trigger_pkg["label"])
    setup_timeframe_label = str(setup_pkg["label"])
    trend_timeframe_label = str(trend_pkg["label"])
    higher_timeframe_label = trend_timeframe_label

    # Trigger = exacte entry timing
    entry_df = trigger_pkg["df"]
    entry_levels = trigger_pkg["levels"]
    entry_vol_profile = trigger_pkg["vol"]

    # Setup = trade-opzet
    setup_df = setup_pkg["df"]
    setup_levels = setup_pkg["levels"]
    setup_vol_profile = setup_pkg["vol"]

    # Trend = hoofdrichting
    higher_df = trend_pkg["df"]
    higher_levels = trend_pkg["levels"]
    higher_vol_profile = trend_pkg["vol"]

    trigger_structure = detect_market_structure(entry_df, swing_window=3)
    setup_structure = detect_market_structure(setup_df, swing_window=3)
    trend_structure = detect_market_structure(higher_df, swing_window=3)

    # Tijdelijk bestaande context-functie gebruiken voor setup + trend
    entry_structure = setup_structure
    higher_structure = trend_structure

    # Kies levels rond de actuele live price i.p.v. blind het eerste level uit de lijst.
    # Hierdoor pakt de bot het dichtstbijzijnde logische level onder/boven de huidige prijs.
    entry_trade_support, entry_trade_resistance, entry_hard_support, entry_hard_resistance = select_levels_around_price(setup_levels, current_price)
    higher_trade_support, higher_trade_resistance, higher_hard_support, higher_hard_resistance = select_levels_around_price(higher_levels, current_price)

    # Stap 5: timing draait op trigger TF, ook rond de actuele prijs.
    trigger_trade_support, trigger_trade_resistance, trigger_hard_support, trigger_hard_resistance = select_levels_around_price(entry_levels, current_price)

    auto_settings = get_auto_trade_settings(coin, entry_vol_profile)
    max_risico_pct = auto_settings["max_risk_pct"]
    entry_buffer_pct = auto_settings["entry_buffer_pct"]
    stop_buffer_pct = auto_settings["stop_buffer_pct"]
    rr_target = auto_settings["rr_target"]

    entry_fee_pct = get_fee_pct_from_type(entry_fee_type, maker_fee_pct, taker_fee_pct)
    exit_fee_pct = get_fee_pct_from_type(exit_fee_type, maker_fee_pct, taker_fee_pct)
    short_borrow_hourly_pct = float(short_borrow_hourly_pct_map.get(coin, DEFAULT_SHORT_BORROW_HOURLY_PCT.get(coin, 0.01)))

    entry_bias = str(entry_structure.get("bias", "neutral"))
    higher_bias = str(higher_structure.get("bias", "neutral"))
    context_info = determine_market_context(
        entry_structure,
        higher_structure,
        current_price=current_price,
        support=entry_trade_support,
        resistance=entry_trade_resistance,
        vol_profile=entry_vol_profile,
    )
    combined_bias = context_info["combined_bias"]
    market_context = context_info["context"]
    trend_label = context_info["label"]

    long_prelimit_zone = choose_prelimit_zone(
        side="long",
        combined_bias=combined_bias,
        market_context=market_context,
        trade_level=entry_trade_support,
        hard_level=entry_hard_support,
    )
    short_prelimit_zone = choose_prelimit_zone(
        side="short",
        combined_bias=combined_bias,
        market_context=market_context,
        trade_level=entry_trade_resistance,
        hard_level=entry_hard_resistance,
    )

    long_metrics = None
    short_metrics = None
    long_valid = False
    short_valid = False
    long_limit_plan = {"status": "WAIT", "reason": "", "entry": None, "stop": None, "target": None, "metrics": None, "valid": False, "distance_to_entry_pct": None}
    short_limit_plan = {"status": "WAIT", "reason": "", "entry": None, "stop": None, "target": None, "metrics": None, "valid": False, "distance_to_entry_pct": None}
    long_doopiecash_plan = {"status": "WAIT", "reason": "", "entry": None, "stop": None, "target": None, "metrics": None, "valid": False, "distance_to_entry_pct": None}
    short_doopiecash_plan = {"status": "WAIT", "reason": "", "entry": None, "stop": None, "target": None, "metrics": None, "valid": False, "distance_to_entry_pct": None}
    long_confirmed_plan = {"status": "WAIT", "reason": "", "entry": None, "stop": None, "target": None, "metrics": None, "valid": False, "trigger": None}
    short_confirmed_plan = {"status": "WAIT", "reason": "", "entry": None, "stop": None, "target": None, "metrics": None, "valid": False, "trigger": None}
    target_long = None
    target_short = None
    long_freshness = {"progress_pct": None, "is_stale": False, "reason": ""}
    short_freshness = {"progress_pct": None, "is_stale": False, "reason": ""}
    long_location = {"quality": "UNKNOWN", "range_progress": None, "distance_to_zone_pct": None, "distance_to_target_pct": None, "reason": ""}
    short_location = {"quality": "UNKNOWN", "range_progress": None, "distance_to_zone_pct": None, "distance_to_target_pct": None, "reason": ""}
    long_prelimit_location = {"quality": "UNKNOWN", "range_progress": None, "distance_to_zone_pct": None, "distance_to_target_pct": None, "reason": ""}
    short_prelimit_location = {"quality": "UNKNOWN", "range_progress": None, "distance_to_zone_pct": None, "distance_to_target_pct": None, "reason": ""}
    long_trigger = short_trigger = None
    setup_timing = compute_setup_timing(
        current_price,
        trigger_trade_support,
        trigger_trade_resistance,
        entry_vol_profile,
        structure_bias=combined_bias,
    )

    if current_price is not None and entry_trade_support is not None and entry_trade_resistance is not None:
        long_stop_base = entry_hard_support if entry_hard_support is not None else entry_trade_support
        short_stop_base = entry_hard_resistance if entry_hard_resistance is not None else entry_trade_resistance

        stop_long = long_stop_base * (1 - stop_buffer_pct / 100)
        stop_short = short_stop_base * (1 + stop_buffer_pct / 100)

        entry_long, entry_short, long_trigger, short_trigger = choose_entry_prices(
            current_price=current_price,
            support=entry_trade_support,
            resistance=entry_trade_resistance,
            entry_buffer_pct=entry_buffer_pct,
            entry_mode=entry_mode,
            df=entry_df,
        )

        if target_mode == "Resistance/Support":
            # Target = dichtstbijzijnde logische opposing level, maar alleen als er genoeg ruimte is.
            # Geen kunstmatige 0.2%-fallback meer: zonder logisch target is er gewoon geen geldige trade.
            target_long = select_target_level(
                side="long",
                reference_price=entry_long,
                local_trade_level=entry_trade_resistance,
                higher_trade_level=higher_trade_resistance,
                min_distance_pct=MIN_DISTANCE_TO_TARGET_PCT,
            )
            target_short = select_target_level(
                side="short",
                reference_price=entry_short,
                local_trade_level=entry_trade_support,
                higher_trade_level=higher_trade_support,
                min_distance_pct=MIN_DISTANCE_TO_TARGET_PCT,
            )

            if target_long is not None:
                target_long *= 0.997

            if target_short is not None:
                target_short *= 1.003
        else:
            long_risk = max(entry_long - stop_long, 0)
            short_risk = max(stop_short - entry_short, 0)

            target_long = entry_long + long_risk * rr_target
            target_short = entry_short - short_risk * rr_target

        long_location = compute_location_quality(
            current_price=current_price,
            support_or_resistance=entry_trade_support,
            target=target_long,
            side="long",
        )
        short_location = compute_location_quality(
            current_price=current_price,
            support_or_resistance=entry_trade_resistance,
            target=target_short,
            side="short",
        )

        long_prelimit_location = compute_location_quality(
            current_price=current_price,
            support_or_resistance=long_prelimit_zone,
            target=target_long,
            side="long",
        )
        short_prelimit_location = compute_location_quality(
            current_price=current_price,
            support_or_resistance=short_prelimit_zone,
            target=target_short,
            side="short",
        )

        long_metrics = calculate_trade_metrics(
            side="long",
            entry=entry_long,
            stop=stop_long,
            target=target_long,
            account_size=account_size,
            max_risk_pct=max_risico_pct,
            coin_symbol=coin,
            entry_fee_pct=entry_fee_pct,
            exit_fee_pct=exit_fee_pct,
            short_borrow_hourly_pct=0.0,
            expected_hold_hours=0.0,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
        )
        short_metrics = calculate_trade_metrics(
            side="short",
            entry=entry_short,
            stop=stop_short,
            target=target_short,
            account_size=account_size,
            max_risk_pct=max_risico_pct,
            coin_symbol=coin,
            entry_fee_pct=entry_fee_pct,
            exit_fee_pct=exit_fee_pct,
            short_borrow_hourly_pct=short_borrow_hourly_pct,
            expected_hold_hours=expected_hold_hours,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
        )

        long_valid = is_setup_valid(long_metrics, min_profit_buffer_eur, taker_fee_pct)
        short_valid = is_setup_valid(short_metrics, min_profit_buffer_eur, taker_fee_pct)

        long_hard_pass, long_hard_reason = passes_hard_filters(
            side="LONG",
            metrics=long_metrics,
            timing_label=setup_timing["long_timing"],
            location_info=long_location,
            min_profit_buffer_eur=min_profit_buffer_eur,
            taker_fee_pct=taker_fee_pct,
        )
        short_hard_pass, short_hard_reason = passes_hard_filters(
            side="SHORT",
            metrics=short_metrics,
            timing_label=setup_timing["short_timing"],
            location_info=short_location,
            min_profit_buffer_eur=min_profit_buffer_eur,
            taker_fee_pct=taker_fee_pct,
        )

        if target_mode == "Resistance/Support" and target_long is None:
            long_hard_pass = False
            long_hard_reason = "Geen logisch opposing target boven prijs"
        if target_mode == "Resistance/Support" and target_short is None:
            short_hard_pass = False
            short_hard_reason = "Geen logisch opposing target onder prijs"

        long_valid = long_valid and long_hard_pass
        short_valid = short_valid and short_hard_pass

        long_limit_plan = build_limit_plan(
            side="long",
            current_price=current_price,
            support_or_resistance=entry_trade_support,
            hard_level=entry_hard_support,
            target=target_long,
            combined_bias=combined_bias,
            market_context=market_context,
            location_quality=str(long_prelimit_location.get("quality", "UNKNOWN")),
            entry_buffer_pct=entry_buffer_pct,
            stop_buffer_pct=stop_buffer_pct,
            account_size=account_size,
            max_risk_pct=max_risico_pct,
            coin_symbol=coin,
            entry_fee_pct=entry_fee_pct,
            exit_fee_pct=exit_fee_pct,
            min_profit_buffer_eur=min_profit_buffer_eur,
            taker_fee_pct=taker_fee_pct,
            trigger_vol_profile=entry_vol_profile,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
        )
        short_limit_plan = build_limit_plan(
            side="short",
            current_price=current_price,
            support_or_resistance=entry_trade_resistance,
            hard_level=entry_hard_resistance,
            target=target_short,
            combined_bias=combined_bias,
            market_context=market_context,
            location_quality=str(short_prelimit_location.get("quality", "UNKNOWN")),
            entry_buffer_pct=entry_buffer_pct,
            stop_buffer_pct=stop_buffer_pct,
            account_size=account_size,
            max_risk_pct=max_risico_pct,
            coin_symbol=coin,
            entry_fee_pct=entry_fee_pct,
            exit_fee_pct=exit_fee_pct,
            min_profit_buffer_eur=min_profit_buffer_eur,
            taker_fee_pct=taker_fee_pct,
            trigger_vol_profile=entry_vol_profile,
            short_borrow_hourly_pct=short_borrow_hourly_pct,
            expected_hold_hours=expected_hold_hours,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
        )

        long_doopiecash_plan = build_doopiecash_plan(
            side="long",
            current_price=current_price,
            support_or_resistance=entry_trade_support,
            hard_level=entry_hard_support,
            target=target_long,
            combined_bias=combined_bias,
            market_context=market_context,
            location_quality=str(long_location.get("quality", "UNKNOWN")),
            entry_buffer_pct=entry_buffer_pct,
            stop_buffer_pct=stop_buffer_pct,
            account_size=account_size,
            max_risk_pct=max_risico_pct,
            coin_symbol=coin,
            entry_fee_pct=entry_fee_pct,
            exit_fee_pct=exit_fee_pct,
            min_profit_buffer_eur=min_profit_buffer_eur,
            taker_fee_pct=taker_fee_pct,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
        )
        short_doopiecash_plan = build_doopiecash_plan(
            side="short",
            current_price=current_price,
            support_or_resistance=entry_trade_resistance,
            hard_level=entry_hard_resistance,
            target=target_short,
            combined_bias=combined_bias,
            market_context=market_context,
            location_quality=str(short_location.get("quality", "UNKNOWN")),
            entry_buffer_pct=entry_buffer_pct,
            stop_buffer_pct=stop_buffer_pct,
            account_size=account_size,
            max_risk_pct=max_risico_pct,
            coin_symbol=coin,
            entry_fee_pct=entry_fee_pct,
            exit_fee_pct=exit_fee_pct,
            min_profit_buffer_eur=min_profit_buffer_eur,
            taker_fee_pct=taker_fee_pct,
            short_borrow_hourly_pct=short_borrow_hourly_pct,
            expected_hold_hours=expected_hold_hours,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
        )

        long_confirmed_plan = build_confirmed_plan(
            side="long",
            current_price=current_price,
            support=entry_trade_support,
            resistance=entry_trade_resistance,
            stop_level=stop_long,
            target=target_long,
            trigger_df=entry_df,
            entry_buffer_pct=entry_buffer_pct,
            account_size=account_size,
            max_risk_pct=max_risico_pct,
            coin_symbol=coin,
            entry_fee_pct=entry_fee_pct,
            exit_fee_pct=exit_fee_pct,
            min_profit_buffer_eur=min_profit_buffer_eur,
            taker_fee_pct=taker_fee_pct,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
        )
        short_confirmed_plan = build_confirmed_plan(
            side="short",
            current_price=current_price,
            support=entry_trade_support,
            resistance=entry_trade_resistance,
            stop_level=stop_short,
            target=target_short,
            trigger_df=entry_df,
            entry_buffer_pct=entry_buffer_pct,
            account_size=account_size,
            max_risk_pct=max_risico_pct,
            coin_symbol=coin,
            entry_fee_pct=entry_fee_pct,
            exit_fee_pct=exit_fee_pct,
            min_profit_buffer_eur=min_profit_buffer_eur,
            taker_fee_pct=taker_fee_pct,
            short_borrow_hourly_pct=short_borrow_hourly_pct,
            expected_hold_hours=expected_hold_hours,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
        )

    best_side = None
    best_metrics = None
    best_targets = None
    best_reason = ""

    long_timing_label = setup_timing["long_timing"]
    short_timing_label = setup_timing["short_timing"]

    long_candidate_score = score_trade_candidate(
        side="LONG",
        metrics=long_metrics if long_valid else None,
        timing_label=long_timing_label,
        combined_bias=combined_bias,
        market_context=market_context,
        taker_fee_pct=taker_fee_pct,
    )
    short_candidate_score = score_trade_candidate(
        side="SHORT",
        metrics=short_metrics if short_valid else None,
        timing_label=short_timing_label,
        combined_bias=combined_bias,
        market_context=market_context,
        taker_fee_pct=taker_fee_pct,
    )

    if str(long_location.get("quality")) == "B_ENTRY":
        long_candidate_score -= 3.0

    if str(short_location.get("quality")) == "B_ENTRY":
        short_candidate_score -= 3.0

    candidates = []
    if long_valid:
        candidates.append(("LONG", long_metrics, target_long, long_candidate_score))
    if short_valid:
        candidates.append(("SHORT", short_metrics, target_short, short_candidate_score))

    if entry_mode == "doopiecash":
        dc_candidates = []
        if long_doopiecash_plan.get("valid") and long_doopiecash_plan.get("metrics") is not None:
            dc_score = score_trade_candidate(
                side="LONG",
                metrics=long_doopiecash_plan.get("metrics"),
                timing_label=long_timing_label,
                combined_bias=combined_bias,
                market_context=market_context,
                taker_fee_pct=taker_fee_pct,
            ) + 8.0
            dc_candidates.append(("LONG", long_doopiecash_plan.get("metrics"), target_long, dc_score))
        if short_doopiecash_plan.get("valid") and short_doopiecash_plan.get("metrics") is not None:
            dc_score = score_trade_candidate(
                side="SHORT",
                metrics=short_doopiecash_plan.get("metrics"),
                timing_label=short_timing_label,
                combined_bias=combined_bias,
                market_context=market_context,
                taker_fee_pct=taker_fee_pct,
            ) + 8.0
            dc_candidates.append(("SHORT", short_doopiecash_plan.get("metrics"), target_short, dc_score))
        if dc_candidates:
            candidates = dc_candidates

    if candidates:
        best_side, best_metrics, best_targets, best_internal_score = max(candidates, key=lambda x: x[3])

    if best_metrics is not None:
        if market_context == "bearish_pullback":
            best_reason = (
                "Grote trend is bearish, maar de entry timeframe trekt tijdelijk omhoog. "
                "De bot behandelt dit als bearish pullback en zoekt dus liever een SHORT op of vlak onder resistance."
            )
        elif market_context == "bullish_pullback":
            best_reason = (
                "Grote trend is bullish, maar de entry timeframe trekt tijdelijk omlaag. "
                "De bot behandelt dit als bullish pullback en zoekt dus liever een LONG op of vlak boven support."
            )
        elif best_side == "LONG":
            best_reason = (
                f"Marktstructuur ondersteunt LONG: trend TF {str(trend_structure.get('market_structure', 'unknown')).capitalize()}, "
                f"setup TF {str(setup_structure.get('market_structure', 'unknown')).capitalize()}, "
                f"trigger timing {setup_timing['long_timing']}. De bot zoekt dus liever een vooraf geplande limit rond de hard support-zone of anders een bevestigde reclaim."
            )
        else:
            best_reason = (
                f"Marktstructuur ondersteunt SHORT: trend TF {str(trend_structure.get('market_structure', 'unknown')).capitalize()}, "
                f"setup TF {str(setup_structure.get('market_structure', 'unknown')).capitalize()}, "
                f"trigger timing {setup_timing['short_timing']}. De bot zoekt dus liever een vooraf geplande limit rond de hard resistance-zone of anders een bevestigde rejectie."
            )

        active_location = long_location if best_side == "LONG" else short_location
        active_confirmation = long_confirmed_plan.get("confirmation", {}) if best_side == "LONG" else short_confirmed_plan.get("confirmation", {})
        best_reason += f" Location filter: {active_location.get('quality')} ({active_location.get('reason')})."
        if active_confirmation:
            best_reason += f" Bevestiging trigger: {active_confirmation.get('reason', 'geen extra bevestiging')}"

        if entry_mode == "doopiecash":
            best_reason += " DoopieCash mode gebruikt pure prijsactie: entry direct op de dichtstbijzijnde support/resistance-zone uit de structuur, zonder extra bevestiging af te wachten."
        elif entry_mode == "confirmation":
            best_reason += " Entry staat op bevestiging, zodat je minder snel achter een al gelopen move aan zit."
        elif entry_mode == "balanced":
            best_reason += " Entry staat iets hoger/lager dan pure limit, zodat de kans groter is dat je de move nog meekrijgt."
        if best_side == "SHORT":
            best_reason += f" Voor shorts rekent de bot nu ook met circa {short_borrow_hourly_pct:.3f}% borrow fee per uur over ~{expected_hold_hours:.0f} uur."

    if best_metrics is None and current_price is not None:
        reasons = []
        if long_hard_reason and not long_hard_pass:
            reasons.append(f"LONG afgekeurd: {long_hard_reason}")
        if short_hard_reason and not short_hard_pass:
            reasons.append(f"SHORT afgekeurd: {short_hard_reason}")
        if reasons:
            best_reason = "Hard filters blokkeren nu deze setup. " + " | ".join(reasons)

    conservative_best_net = calculate_conservative_net_profit(best_metrics, taker_fee_pct)
    score = calculate_setup_score(best_metrics, best_side, combined_bias, taker_fee_pct)

    if best_metrics is not None:
        longish = best_side == "LONG"
        timing_label = setup_timing["long_timing"] if longish else setup_timing["short_timing"]
        status = f"Kansrijk • {timing_label}"
    elif str(long_location.get("quality")) == "SKIP" and str(short_location.get("quality")) in {"SKIP", "UNKNOWN"}:
        status = "Te laat / overslaan"
    elif market_context in {"bearish_pullback", "bullish_pullback", "bullish_wait", "bearish_wait"}:
        status = "Wachten op pullback"
    elif market_context in {"mixed_bullish_near_support", "mixed_bearish_near_resistance"}:
        status = "Voorzichtig kansrijk"
    elif combined_bias == "voorzichtig":
        status = "Wachten"
    elif current_price is None:
        status = "Geen data"
    else:
        status = "Bijna trade"

    setup_grade = classify_setup_grade(
        best_metrics=best_metrics,
        score=score,
        status=status,
        long_hard_reason=long_hard_reason,
        short_hard_reason=short_hard_reason,
    )

    chosen_entry_variant = None
    if best_side == "LONG":
        if entry_mode == "doopiecash" and long_doopiecash_plan.get("valid"):
            chosen_entry_variant = "best_doopiecash"
        elif entry_mode == "limit" and long_limit_plan.get("valid"):
            chosen_entry_variant = "best_limit"
        elif entry_mode == "confirmation" and long_confirmed_plan.get("valid"):
            chosen_entry_variant = "best_confirmed"
        elif entry_mode == "balanced" and long_valid:
            chosen_entry_variant = "best_balanced"
        elif long_doopiecash_plan.get("valid"):
            chosen_entry_variant = "best_doopiecash"
        elif long_limit_plan.get("valid"):
            chosen_entry_variant = "best_limit"
        elif long_confirmed_plan.get("valid"):
            chosen_entry_variant = "best_confirmed"
        elif long_valid:
            chosen_entry_variant = "best_balanced"
    elif best_side == "SHORT":
        if entry_mode == "doopiecash" and short_doopiecash_plan.get("valid"):
            chosen_entry_variant = "best_doopiecash"
        elif entry_mode == "limit" and short_limit_plan.get("valid"):
            chosen_entry_variant = "best_limit"
        elif entry_mode == "confirmation" and short_confirmed_plan.get("valid"):
            chosen_entry_variant = "best_confirmed"
        elif entry_mode == "balanced" and short_valid:
            chosen_entry_variant = "best_balanced"
        elif short_doopiecash_plan.get("valid"):
            chosen_entry_variant = "best_doopiecash"
        elif short_limit_plan.get("valid"):
            chosen_entry_variant = "best_limit"
        elif short_confirmed_plan.get("valid"):
            chosen_entry_variant = "best_confirmed"
        elif short_valid:
            chosen_entry_variant = "best_balanced"

    return {
        "coin": coin,
        "market": market,
        "timeframe_label": timeframe_label,
        "higher_timeframe_label": higher_timeframe_label,
        "trigger_timeframe_label": trigger_timeframe_label,
        "setup_timeframe_label": setup_timeframe_label,
        "trend_timeframe_label": trend_timeframe_label,
        "current_price": current_price,
        "live_price": live_price,
        "entry_df": entry_df,
        "entry_levels": entry_levels,
        "higher_levels": higher_levels,
        "entry_structure": entry_structure,
        "higher_structure": higher_structure,
        "trigger_structure": trigger_structure,
        "setup_structure": setup_structure,
        "trend_structure": trend_structure,
        "entry_vol_profile": entry_vol_profile,
        "higher_vol_profile": higher_vol_profile,
        "entry_trade_support": entry_trade_support,
        "entry_trade_resistance": entry_trade_resistance,
        "trigger_trade_support": trigger_trade_support,
        "trigger_trade_resistance": trigger_trade_resistance,
        "entry_hard_support": entry_hard_support,
        "entry_hard_resistance": entry_hard_resistance,
        "higher_trade_support": higher_trade_support,
        "higher_trade_resistance": higher_trade_resistance,
        "higher_hard_support": higher_hard_support,
        "higher_hard_resistance": higher_hard_resistance,
        "entry_bias": entry_bias,
        "higher_bias": higher_bias,
        "combined_bias": combined_bias,
        "market_context": market_context,
        "trend_label": trend_label,
        "long_metrics": long_metrics,
        "short_metrics": short_metrics,
        "long_valid": long_valid,
        "short_valid": short_valid,
        "target_long": target_long,
        "target_short": target_short,
        "long_location": long_location,
        "short_location": short_location,
        "long_prelimit_zone": long_prelimit_zone,
        "short_prelimit_zone": short_prelimit_zone,
        "long_prelimit_location": long_prelimit_location,
        "short_prelimit_location": short_prelimit_location,
        "long_limit_plan": long_limit_plan,
        "short_limit_plan": short_limit_plan,
        "long_doopiecash_plan": long_doopiecash_plan,
        "short_doopiecash_plan": short_doopiecash_plan,
        "long_confirmed_plan": long_confirmed_plan,
        "short_confirmed_plan": short_confirmed_plan,
        "best_side": best_side,
        "best_metrics": best_metrics,
        "best_targets": best_targets,
        "best_reason": best_reason,
        "conservative_best_net": conservative_best_net,
        "score": score,
        "status": status,
        "setup_grade": setup_grade,
        "long_freshness": long_freshness,
        "short_freshness": short_freshness,
        "entry_mode": entry_mode,
        "chosen_entry_variant": chosen_entry_variant,
        "setup_timing": setup_timing,
        "long_candidate_score": long_candidate_score,
        "short_candidate_score": short_candidate_score,
        "long_hard_pass": long_hard_pass,
        "short_hard_pass": short_hard_pass,
        "long_hard_reason": long_hard_reason,
        "short_hard_reason": short_hard_reason,
        "long_trigger": long_trigger,
        "short_trigger": short_trigger,
    }


# =========================================================
# Session state init
# =========================================================
if "manual_override" not in st.session_state:
    st.session_state.manual_override = False
if "manual_price" not in st.session_state:
    st.session_state.manual_price = 100.0
if "selected_coin" not in st.session_state:
    st.session_state.selected_coin = "BTC"
if "account_size" not in st.session_state:
    st.session_state.account_size = 1000.0
if "min_profit_buffer_eur" not in st.session_state:
    st.session_state.min_profit_buffer_eur = 0.0
if "target_mode" not in st.session_state:
    st.session_state.target_mode = "Resistance/Support"
if "maker_fee_pct" not in st.session_state:
    st.session_state.maker_fee_pct = DEFAULT_MAKER_FEE_PCT
if "taker_fee_pct" not in st.session_state:
    st.session_state.taker_fee_pct = DEFAULT_TAKER_FEE_PCT
if "entry_fee_type" not in st.session_state:
    st.session_state.entry_fee_type = "maker"
if "exit_fee_type" not in st.session_state:
    st.session_state.exit_fee_type = "taker"
if "entry_mode_label" not in st.session_state:
    st.session_state.entry_mode_label = "DoopieCash"
if "short_liquidation_fee_pct" not in st.session_state:
    st.session_state.short_liquidation_fee_pct = DEFAULT_SHORT_LIQUIDATION_FEE_PCT
if "expected_hold_hours_override" not in st.session_state:
    st.session_state.expected_hold_hours_override = False
if "expected_hold_hours" not in st.session_state:
    st.session_state.expected_hold_hours = DEFAULT_EXPECTED_HOLD_HOURS["1h"]
for _coin, _default_rate in DEFAULT_SHORT_BORROW_HOURLY_PCT.items():
    key = f"short_borrow_hourly_pct_{_coin}"
    if key not in st.session_state:
        st.session_state[key] = _default_rate

if "scanner_results" not in st.session_state:
    st.session_state.scanner_results = []
if "scanner_signature" not in st.session_state:
    st.session_state.scanner_signature = None
if "scanner_last_updated" not in st.session_state:
    st.session_state.scanner_last_updated = None

# =========================================================
# CSS
# =========================================================
st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.2rem;
        padding-bottom: 1.5rem;
        max-width: 1450px;
    }
    div[data-testid="stMetric"] {
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 16px;
        padding: 10px 12px;
    }
    .bf-card {
        background: linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.02));
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 18px;
        padding: 16px;
        min-height: 180px;
    }
    .bf-card-title {
        font-size: 0.95rem;
        color: #9CA3AF;
        margin-bottom: 6px;
    }
    .bf-card-coin {
        font-size: 1.55rem;
        font-weight: 800;
        margin-bottom: 6px;
    }
    .bf-card-side {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 999px;
        font-size: 0.8rem;
        font-weight: 700;
        margin-bottom: 12px;
        background: rgba(34,197,94,0.18);
        color: #86EFAC;
    }
    .bf-card-side.short {
        background: rgba(239,68,68,0.18);
        color: #FCA5A5;
    }
    .bf-card-side.wait {
        background: rgba(234,179,8,0.18);
        color: #FDE68A;
    }
    .bf-card-small {
        color: #D1D5DB;
        font-size: 0.95rem;
        margin-top: 6px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# =========================================================
# Scanner universe
# =========================================================
# Bewust beperkt tot de 5 focusmunten voor validatie.

# =========================================================
# Top controls
# =========================================================
control_col1, control_col2, control_col3, control_col4 = st.columns([1, 1.0, 1.0, 2.0])
with control_col1:
    timeframe_label = st.selectbox("Scanner timeframe", list(TIMEFRAMES.keys()), index=4)
with control_col2:
    st.metric("Coins in scanner", len(COINS))
with control_col3:
    scan_now = st.button("🔄 Scan opnieuw", use_container_width=True)
    if st.session_state.scanner_last_updated:
        st.caption(f"Laatst gescand: {st.session_state.scanner_last_updated}")
with control_col4:
    with st.expander("⚙️ Scanner instellingen", expanded=False):
        s1, s2, s3 = st.columns(3)
        with s1:
            st.session_state.account_size = st.number_input(
                "Account grootte (€)",
                min_value=1.0,
                value=float(st.session_state.account_size),
                step=50.0,
            )
            st.caption("Detectie: Multi-timeframe marktstructuur")
            st.session_state.target_mode = st.selectbox(
                "Winstdoel methode",
                ["Resistance/Support", "Risk Reward"],
                index=0 if st.session_state.target_mode == "Resistance/Support" else 1,
            )
        with s2:
            st.session_state.min_profit_buffer_eur = st.number_input(
                "Extra winst boven fees (€)",
                min_value=0.0,
                value=float(st.session_state.min_profit_buffer_eur),
                step=0.5,
                help="Trade gebruikt nu een soepelere buffer: alleen een deel van dit bedrag wordt hard meegenomen, zodat je meer setups ziet.",
            )
            st.session_state.entry_mode_label = st.selectbox(
                "Entry modus",
                list(ENTRY_MODES.keys()),
                index=list(ENTRY_MODES.keys()).index(st.session_state.entry_mode_label) if st.session_state.entry_mode_label in ENTRY_MODES else 0,
                help="DoopieCash = pure prijsactie op structuurzones zonder bevestiging. Limit pakt een scherpe zone-entry. Gebalanceerd ligt iets dichter bij de huidige prijs. Bevestiging wacht op een reclaim/break terug omhoog of omlaag.",
            )
            st.session_state.maker_fee_pct = st.number_input(
                "Maker fee (%)",
                min_value=0.0,
                value=float(st.session_state.maker_fee_pct),
                step=0.01,
                format="%.2f",
            )
        with s3:
            st.session_state.taker_fee_pct = st.number_input(
                "Taker fee (%)",
                min_value=0.0,
                value=float(st.session_state.taker_fee_pct),
                step=0.01,
                format="%.2f",
            )
            st.session_state.entry_fee_type = st.selectbox(
                "Entry order type",
                ["maker", "taker"],
                index=0 if st.session_state.entry_fee_type == "maker" else 1,
            )
            st.session_state.exit_fee_type = st.selectbox(
                "Exit order type",
                ["maker", "taker"],
                index=0 if st.session_state.exit_fee_type == "maker" else 1,
            )

        st.markdown("**Short-selling kosten (Bitvavo)**")
        b1, b2 = st.columns(2)
        with b1:
            st.session_state.short_liquidation_fee_pct = st.number_input(
                "Liquidatiefee short (%)",
                min_value=0.0,
                value=float(st.session_state.short_liquidation_fee_pct),
                step=0.1,
                format="%.2f",
                help="Bitvavo noemt 2% liquidatiekosten bij automatische sluiting.",
            )
            st.session_state.expected_hold_hours_override = st.checkbox(
                "Hold hours handmatig",
                value=st.session_state.expected_hold_hours_override,
                help="Gebruik een eigen schatting voor hoe lang een short gemiddeld open blijft.",
            )
            default_hold_hours = DEFAULT_EXPECTED_HOLD_HOURS.get(timeframe_label, 24.0)
            st.session_state.expected_hold_hours = st.number_input(
                "Verwachte hold hours short",
                min_value=0.0,
                value=float(st.session_state.expected_hold_hours if st.session_state.expected_hold_hours_override else default_hold_hours),
                step=1.0,
                disabled=not st.session_state.expected_hold_hours_override,
            )
        with b2:
            borrow_coin = st.selectbox("Borrow fee coin", list(COINS.keys()), index=0, key="borrow_fee_coin_select")
            borrow_key = f"short_borrow_hourly_pct_{borrow_coin}"
            st.session_state[borrow_key] = st.number_input(
                f"Borrow fee {borrow_coin} (% per uur)",
                min_value=0.0,
                value=float(st.session_state[borrow_key]),
                step=0.001,
                format="%.3f",
                help="Leenkosten lopen per uur op en verschillen per asset.",
            )
            st.caption("Pas hier per coin de geschatte leenkosten aan. Deze worden alleen op SHORTs toegepast.")

account_size = float(st.session_state.account_size)
min_profit_buffer_eur = float(st.session_state.min_profit_buffer_eur)
target_mode = st.session_state.target_mode
entry_mode = ENTRY_MODES.get(st.session_state.entry_mode_label, "balanced")
maker_fee_pct = float(st.session_state.maker_fee_pct)
taker_fee_pct = float(st.session_state.taker_fee_pct)
entry_fee_type = st.session_state.entry_fee_type
exit_fee_type = st.session_state.exit_fee_type
short_liquidation_fee_pct = float(st.session_state.short_liquidation_fee_pct)
expected_hold_hours = float(
    st.session_state.expected_hold_hours
    if st.session_state.expected_hold_hours_override
    else DEFAULT_EXPECTED_HOLD_HOURS.get(timeframe_label, 24.0)
)
short_borrow_hourly_pct_map = {
    coin_symbol: float(st.session_state.get(f"short_borrow_hourly_pct_{coin_symbol}", DEFAULT_SHORT_BORROW_HOURLY_PCT.get(coin_symbol, 0.01)))
    for coin_symbol in COINS.keys()
}

# =========================================================
# Dashboard scan
# =========================================================
current_scan_signature = (
    timeframe_label,
    round(account_size, 2),
    round(min_profit_buffer_eur, 2),
    target_mode,
    entry_mode,
    round(maker_fee_pct, 4),
    round(taker_fee_pct, 4),
    entry_fee_type,
    exit_fee_type,
    round(short_liquidation_fee_pct, 4),
    round(expected_hold_hours, 2),
    tuple(sorted((coin_symbol, round(rate, 6)) for coin_symbol, rate in short_borrow_hourly_pct_map.items())),
    tuple(COINS.keys()),
)

should_scan = (
    scan_now
    or not st.session_state.scanner_results
    or st.session_state.scanner_signature != current_scan_signature
)

if should_scan:
    with st.spinner("BullForge scant de beste kansen..."):
        st.session_state.scanner_results = [
            analyze_coin_setup(
                coin=coin_symbol,
                timeframe_label=timeframe_label,
                account_size=account_size,
                min_profit_buffer_eur=min_profit_buffer_eur,
                target_mode=target_mode,
                maker_fee_pct=maker_fee_pct,
                taker_fee_pct=taker_fee_pct,
                entry_fee_type=entry_fee_type,
                exit_fee_type=exit_fee_type,
                short_borrow_hourly_pct_map=short_borrow_hourly_pct_map,
                expected_hold_hours=expected_hold_hours,
                short_liquidation_fee_pct=short_liquidation_fee_pct,
                entry_mode=entry_mode,
            )
            for coin_symbol in COINS.keys()
        ]
        st.session_state.scanner_signature = current_scan_signature
        st.session_state.scanner_last_updated = pd.Timestamp.now().strftime("%H:%M:%S")

scanner_results = st.session_state.scanner_results
grade_rank = {"GOOD": 3, "OK": 2, "WEAK": 1, "NO DATA": 0}
ranked_results = sorted(
    scanner_results,
    key=lambda x: (grade_rank.get(x.get("setup_grade", "NO DATA"), 0), x["best_metrics"] is not None, x["score"], x.get("conservative_best_net") or -999),
    reverse=True,
)
valid_results = [result for result in ranked_results if result["best_metrics"] is not None]
visible_results = [result for result in ranked_results if result.get("setup_grade") in {"GOOD", "OK", "WEAK"}]

if valid_results and st.session_state.selected_coin not in COINS:
    st.session_state.selected_coin = valid_results[0]["coin"]
elif valid_results and st.session_state.selected_coin not in [r["coin"] for r in ranked_results]:
    st.session_state.selected_coin = valid_results[0]["coin"]
elif st.session_state.selected_coin not in COINS:
    st.session_state.selected_coin = list(COINS.keys())[0]

best_long = next((r for r in ranked_results if r["best_side"] == "LONG"), None)
best_short = next((r for r in ranked_results if r["best_side"] == "SHORT"), None)

summary1, summary2, summary3, summary4 = st.columns(4)
summary1.metric("Zichtbare setups", len(visible_results))
summary2.metric("Beste long", best_long["coin"] if best_long else "-", f"Score {best_long['score']:.0f}" if best_long else None)
summary3.metric("Beste short", best_short["coin"] if best_short else "-", f"Score {best_short['score']:.0f}" if best_short else None)
summary4.metric("Beste timeframe", timeframe_label)
st.caption("GOOD = sterk • OK = bruikbaar • WEAK = bijna-trade / lagere kwaliteit")

st.markdown("### 🏠 Beste kansen")
st.caption("Dashboard blijft staan. Alleen de scanner-data verandert wanneer je opnieuw scant of instellingen wijzigt.")
card_cols = st.columns(3)
top_cards = ranked_results[:3]

for idx, result in enumerate(top_cards):
    with card_cols[idx]:
        side_class = "wait"
        if result["best_side"] == "LONG":
            side_class = ""
        elif result["best_side"] == "SHORT":
            side_class = "short"

        side_label = result["best_side"] if result["best_side"] is not None else f"{result.get('setup_grade', '-')} • {result['status']}".upper()
        entry_text = fmt_price_eur(float(result["best_metrics"]["entry"])) if result["best_metrics"] is not None else "-"
        net_text = fmt_eur(float(result["best_metrics"]["net_profit_eur"])) if result["best_metrics"] is not None else "-"
        st.markdown(
            f"""
            <div class="bf-card">
                <div class="bf-card-title">#{idx + 1} beste kans</div>
                <div class="bf-card-coin">{result['coin']}</div>
                <div class="bf-card-side {side_class}">{side_label}</div>
                <div class="bf-card-small">Entry: <strong>{entry_text}</strong></div>
                <div class="bf-card-small">Netto: <strong>{net_text}</strong></div>
                <div class="bf-card-small">Score: <strong>{result['score']:.0f}/100</strong></div>
                <div class="bf-card-small">Kwaliteit: <strong>{result.get('setup_grade', '-')}</strong></div>
                <div class="bf-card-small">Trend: <strong>{str(result.get('trend_label', result['combined_bias']))}</strong></div>
                <div class="bf-card-small">TFs: <strong>{result.get('trigger_timeframe_label','-')} / {result.get('setup_timeframe_label','-')} / {result.get('trend_timeframe_label','-')}</strong></div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button(f"Open {result['coin']}", key=f"open_top_card_{result['coin']}", use_container_width=True):
            st.session_state.selected_coin = result["coin"]

with st.expander("📋 Bekijk alle scanner-resultaten", expanded=False):
    overview_rows = []
    for result in ranked_results:
        best_metrics = result["best_metrics"]
        timing_text = "-"
        if result.get("best_side") == "LONG":
            timing_text = result.get("setup_timing", {}).get("long_timing", "-")
        elif result.get("best_side") == "SHORT":
            timing_text = result.get("setup_timing", {}).get("short_timing", "-")

        overview_rows.append({
            "Coin": result["coin"],
            "Status": result["status"],
            "Richting": result["best_side"] or "-",
            "Score": result["score"],
            "Kwaliteit": result.get("setup_grade"),
            "Trend TF": result.get("trend_timeframe_label"),
            "Setup TF": result.get("setup_timeframe_label"),
            "Trigger TF": result.get("trigger_timeframe_label"),
            "Context": result.get("trend_label"),
            "Timing": timing_text,
            "Netto winst": round(float(best_metrics["net_profit_eur"]), 2) if best_metrics else None,
            "Conservatief netto": round(float(result["conservative_best_net"]), 2) if result["conservative_best_net"] is not None else None,
            "RR": round(float(best_metrics["rr"]), 2) if best_metrics else None,
            "Prijs": fmt_price_eur(float(result["current_price"])) if result["current_price"] is not None else None,
        })
    st.dataframe(pd.DataFrame(overview_rows), use_container_width=True, hide_index=True)

# =========================================================
# Selected coin detail
# =========================================================
if valid_results and st.session_state.selected_coin not in COINS:
    st.session_state.selected_coin = valid_results[0]["coin"]

selected_coin = st.session_state.selected_coin if st.session_state.selected_coin in COINS else list(COINS.keys())[0]
selected_market = COINS[selected_coin]["bitvavo_market"]
live_selected_price = get_live_price_for_market(selected_market)
if live_selected_price is not None and not st.session_state.manual_override:
    st.session_state.manual_price = float(live_selected_price)

# =========================================================
# Tabs
# =========================================================
tab_market, tab_trade, tab_journal = st.tabs(["📈 Markt", "🎯 Trade", "📝 Journal"])

# =========================================================
# Markt tab
# =========================================================
with tab_market:
    chart_col, side_col = st.columns([3.6, 1.15])

    with side_col:
        st.subheader("🧭 Detail coin")
        detail_coin = st.selectbox(
            "Coin detail",
            list(COINS.keys()),
            index=list(COINS.keys()).index(selected_coin),
        )
        if detail_coin != st.session_state.selected_coin:
            st.session_state.selected_coin = detail_coin
            st.rerun()

        st.session_state.manual_override = st.checkbox("Prijs handmatig aanpassen", value=st.session_state.manual_override)

        price_input = st.number_input(
            "Huidige prijs (€)",
            min_value=0.0,
            value=float(st.session_state.manual_price),
            step=0.0001,
            format="%.8f",
            disabled=not st.session_state.manual_override,
        )
        if st.session_state.manual_override:
            st.session_state.manual_price = float(price_input)

        cached_selected_result = next((r for r in scanner_results if r["coin"] == st.session_state.selected_coin), None)
        if st.session_state.manual_override:
            selected_result = analyze_coin_setup(
                coin=st.session_state.selected_coin,
                timeframe_label=timeframe_label,
                account_size=account_size,
                min_profit_buffer_eur=min_profit_buffer_eur,
                target_mode=target_mode,
                maker_fee_pct=maker_fee_pct,
                taker_fee_pct=taker_fee_pct,
                entry_fee_type=entry_fee_type,
                exit_fee_type=exit_fee_type,
                short_borrow_hourly_pct_map=short_borrow_hourly_pct_map,
                expected_hold_hours=expected_hold_hours,
                short_liquidation_fee_pct=short_liquidation_fee_pct,
                override_price=float(st.session_state.manual_price),
                entry_mode=entry_mode,
            )
        else:
            selected_result = cached_selected_result or analyze_coin_setup(
                coin=st.session_state.selected_coin,
                timeframe_label=timeframe_label,
                account_size=account_size,
                min_profit_buffer_eur=min_profit_buffer_eur,
                target_mode=target_mode,
                maker_fee_pct=maker_fee_pct,
                taker_fee_pct=taker_fee_pct,
                entry_fee_type=entry_fee_type,
                exit_fee_type=exit_fee_type,
                short_borrow_hourly_pct_map=short_borrow_hourly_pct_map,
                expected_hold_hours=expected_hold_hours,
                short_liquidation_fee_pct=short_liquidation_fee_pct,
                entry_mode=entry_mode,
            )

        if selected_result["live_price"] is not None:
            st.success(f"Live prijs: {fmt_price_eur(float(selected_result['live_price']))}")
        else:
            st.warning("Live prijs ophalen mislukt.")

        o1, o2 = st.columns(2)
        o1.metric("Coin", selected_result["coin"])
        o2.metric("TF", timeframe_label)
        o3, o4 = st.columns(2)
        o3.metric("Volatility", str(selected_result["entry_vol_profile"]["vol_label"]).capitalize())
        o4.metric("Grote TF", selected_result["higher_timeframe_label"])
        st.caption(f"Actieve detectie: Marktstructuur • Entry: {st.session_state.entry_mode_label}")

        if selected_result["combined_bias"] == "long":
            st.success("🟢 Trendcheck: LONG")
        elif selected_result["combined_bias"] == "short":
            st.error("🔴 Trendcheck: SHORT")
        elif selected_result["combined_bias"] == "voorzichtig":
            st.warning("🟡 Trendcheck: Voorzichtig")
        else:
            st.info("⚪ Trendcheck: Neutraal")


        timing = selected_result.get("setup_timing", {})
        long_timing = timing.get("long_timing", "-")
        short_timing = timing.get("short_timing", "-")
        d1, d2 = st.columns(2)
        d1.metric("LONG timing", str(long_timing))
        d2.metric("SHORT timing", str(short_timing))

        entry_structure = selected_result.get("entry_structure", {}) or {}
        higher_structure = selected_result.get("higher_structure", {}) or {}
        entry_market_structure = str(entry_structure.get("market_structure", "unknown")).capitalize()
        higher_market_structure = str(higher_structure.get("market_structure", "unknown")).capitalize()

        s1, s2 = st.columns(2)
        s1.metric("Marktstructuur TF", entry_market_structure)
        s2.metric("Marktstructuur grote TF", higher_market_structure)

        if timing.get("distance_to_support_pct") is not None:
            st.caption(f"Afstand tot support: {abs(float(timing['distance_to_support_pct'])):.2f}%")
        if timing.get("distance_to_resistance_pct") is not None:
            st.caption(f"Afstand tot resistance: {abs(float(timing['distance_to_resistance_pct'])):.2f}%")

        with st.expander("📊 Trendcheck details", expanded=False):
            if selected_result["entry_trade_support"] is not None:
                st.write(f"Korte support: {fmt_price_eur(selected_result['entry_trade_support'])}")
            if selected_result["entry_trade_resistance"] is not None:
                st.write(f"Korte resistance: {fmt_price_eur(selected_result['entry_trade_resistance'])}")
            if selected_result["higher_trade_support"] is not None:
                st.write(f"Grote support: {fmt_price_eur(selected_result['higher_trade_support'])}")
            if selected_result["higher_trade_resistance"] is not None:
                st.write(f"Grote resistance: {fmt_price_eur(selected_result['higher_trade_resistance'])}")
            st.write(f"Korte trend status: **{selected_result['entry_bias']}**")
            st.write(f"Grote trend status: **{selected_result['higher_bias']}**")
            st.write(f"Korte marktstructuur: **{entry_market_structure}**")
            st.write(f"Grote marktstructuur: **{higher_market_structure}**")
            st.write(f"Eindoordeel: **{selected_result.get('trend_label', selected_result['combined_bias'])}**")

    with chart_col:
        st.subheader(f"📉 Markt overzicht - {selected_result['coin']} ({timeframe_label})")
        st.caption("Detectie modus: Marktstructuur")
        if selected_result["entry_df"] is not None:
            render_price_chart(
                selected_result["entry_df"],
                trade_supports=selected_result["entry_levels"].get("trade_supports", []),
                trade_resistances=selected_result["entry_levels"].get("trade_resistances", []),
                hard_supports=selected_result["entry_levels"].get("hard_supports", []),
                hard_resistances=selected_result["entry_levels"].get("hard_resistances", []),
                higher_trade_support=selected_result["higher_trade_support"],
                higher_trade_resistance=selected_result["higher_trade_resistance"],
                active_support=selected_result["entry_trade_support"],
                active_resistance=selected_result["entry_trade_resistance"],
                height=660,
            )
        else:
            st.warning("Chartdata tijdelijk niet beschikbaar.")

# =========================================================
# Trade tab
# =========================================================
with tab_trade:
    cached_selected_result = next((r for r in scanner_results if r["coin"] == st.session_state.selected_coin), None)
    if st.session_state.manual_override:
        selected_result = analyze_coin_setup(
            coin=st.session_state.selected_coin,
            timeframe_label=timeframe_label,
            account_size=account_size,
            min_profit_buffer_eur=min_profit_buffer_eur,
            target_mode=target_mode,
            maker_fee_pct=maker_fee_pct,
            taker_fee_pct=taker_fee_pct,
            entry_fee_type=entry_fee_type,
            exit_fee_type=exit_fee_type,
            short_borrow_hourly_pct_map=short_borrow_hourly_pct_map,
            expected_hold_hours=expected_hold_hours,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
            override_price=float(st.session_state.manual_price),
            entry_mode=entry_mode,
        )
    else:
        selected_result = cached_selected_result or analyze_coin_setup(
            coin=st.session_state.selected_coin,
            timeframe_label=timeframe_label,
            account_size=account_size,
            min_profit_buffer_eur=min_profit_buffer_eur,
            target_mode=target_mode,
            maker_fee_pct=maker_fee_pct,
            taker_fee_pct=taker_fee_pct,
            entry_fee_type=entry_fee_type,
            exit_fee_type=exit_fee_type,
            short_borrow_hourly_pct_map=short_borrow_hourly_pct_map,
            expected_hold_hours=expected_hold_hours,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
            entry_mode=entry_mode,
        )

    st.subheader(f"🎯 Trade advies — {selected_result['coin']}")

    if selected_result["entry_trade_support"] is None or selected_result["entry_trade_resistance"] is None:
        st.info(
            "Nog geen complete setup gevonden. "
            "Meestal betekent dit dat support of resistance op deze timeframe nog niet duidelijk genoeg is."
        )
    else:
        best_side = selected_result["best_side"]
        best_metrics = selected_result["best_metrics"]
        best_targets = selected_result["best_targets"]
        best_reason = selected_result["best_reason"]
        combined_bias = selected_result["combined_bias"]

        if best_metrics is None:
            if combined_bias == "voorzichtig":
                st.warning("🟡 **Advies: Nog even wachten**")
                st.write("De markt geeft gemixte signalen en de setup is nog niet sterk genoeg na kosten.")
            else:
                st.error("🔴 **Advies: Geen trade**")
                st.write("De verwachte winst is op dit moment te klein als de handelskosten worden meegerekend.")
        else:
            if best_side == "LONG":
                st.success("🟢 **Advies: LONG kansrijk**")
                if selected_result.get("entry_mode") == "doopiecash":
                    st.write("De long-setup is momenteel de beste keuze op basis van pure prijsactie, structuur en netto winst.")
                else:
                    st.write("De long-setup is momenteel de beste keuze op basis van trend/reactie en netto winst.")
            else:
                st.success("🟢 **Advies: SHORT kansrijk**")
                if selected_result.get("entry_mode") == "doopiecash":
                    st.write("De short-setup is momenteel de beste keuze op basis van pure prijsactie, structuur en netto winst.")
                else:
                    st.write("De short-setup is momenteel de beste keuze op basis van trend/reactie en netto winst.")

        st.markdown("---")

        if best_metrics is not None:
            c1, c2, c3 = st.columns(3)
            c1.metric("Instap", fmt_price_eur(float(best_metrics["entry"])))
            c2.metric("Veiligheidsgrens", fmt_price_eur(float(best_metrics["stop"])))
            c3.metric("Winstdoel", fmt_price_eur(float(best_metrics["target"])))

            c4, c5, c6 = st.columns(3)
            c4.metric("Netto winst", fmt_eur(float(best_metrics["net_profit_eur"])))
            c5.metric("Kosten", fmt_eur(float(best_metrics["total_fees_eur"])))
            c6.metric("Verlies als fout", fmt_eur(float(best_metrics["net_loss_if_stopped_eur"])))
            if best_side == "SHORT":
                c10, c11, c12 = st.columns(3)
                c10.metric("Borrow fee", fmt_eur(float(best_metrics.get("borrow_fee_eur", 0.0))))
                c11.metric("Hold hours", f'{float(best_metrics.get("expected_hold_hours", 0.0)):.0f}u')
                c12.metric("Liquidatiefee", fmt_eur(float(best_metrics.get("estimated_liquidation_fee_eur", 0.0))))

            c7, c8, c9 = st.columns(3)
            c7.metric("Score", f"{selected_result['score']:.0f}/100")
            c8.metric("RR", f"1 : {float(best_metrics['rr']):.2f}")
            c9.metric("Conservatief netto", fmt_eur(float(selected_result["conservative_best_net"] or 0.0)))

            entry_notional_eur = float(best_metrics["entry"]) * float(best_metrics["position_size"])
            account_usage_pct = (entry_notional_eur / account_size * 100) if account_size > 0 else 0.0
            c13, c14, c15 = st.columns(3)
            c13.metric("Inleg nodig", fmt_eur(entry_notional_eur))
            c14.metric("Positiegrootte", f'{float(best_metrics["position_size"]):.6f} {selected_result["coin"]}')
            c15.metric("Gebruikt van account", f"{account_usage_pct:.1f}%")

            st.info(best_reason)

            if float(best_metrics["net_profit_eur"]) < float(best_metrics["total_fees_eur"]):
                st.warning("De kosten nemen een groot deel van de mogelijke winst weg.")
        else:
            fallback_candidates = []
            if selected_result["long_metrics"] is not None:
                fallback_candidates.append(("LONG", selected_result["long_metrics"]))
            if selected_result["short_metrics"] is not None:
                fallback_candidates.append(("SHORT", selected_result["short_metrics"]))

            if fallback_candidates:
                _, fallback_metrics = max(fallback_candidates, key=lambda x: float(x[1]["net_profit_eur"]))
                c1, c2, c3 = st.columns(3)
                c1.metric("Instap", fmt_price_eur(float(fallback_metrics["entry"])))
                c2.metric("Veiligheidsgrens", fmt_price_eur(float(fallback_metrics["stop"])))
                c3.metric("Winstdoel", fmt_price_eur(float(fallback_metrics["target"])))

                c4, c5, c6 = st.columns(3)
                c4.metric("Netto winst", fmt_eur(float(fallback_metrics["net_profit_eur"])))
                c5.metric("Kosten", fmt_eur(float(fallback_metrics["total_fees_eur"])))
                c6.metric("Verlies als fout", fmt_eur(float(fallback_metrics["net_loss_if_stopped_eur"])))

        st.markdown("---")

        long_limit_plan = selected_result.get("long_limit_plan", {})
        short_limit_plan = selected_result.get("short_limit_plan", {})
        long_doopiecash_plan = selected_result.get("long_doopiecash_plan", {})
        short_doopiecash_plan = selected_result.get("short_doopiecash_plan", {})
        long_confirmed_plan = selected_result.get("long_confirmed_plan", {})
        short_confirmed_plan = selected_result.get("short_confirmed_plan", {})

        split_limit_plan = long_doopiecash_plan if best_side == "LONG" else short_doopiecash_plan if best_side == "SHORT" else {}
        split_confirmed_plan = long_confirmed_plan if best_side == "LONG" else short_confirmed_plan if best_side == "SHORT" else {}
        split_side = best_side if best_side in {"LONG", "SHORT"} else None
        left_title = "DoopieCash" if selected_result.get("entry_mode") == "doopiecash" else "Aggressive"
        left_caption = "Pure prijsactie op structuurzone • vroegste entry • lagere bevestiging" if selected_result.get("entry_mode") == "doopiecash" else "Limit op support/resistance zone • betere prijs • hoger fail-risico"

        if split_side is not None and (split_limit_plan.get("entry") is not None or split_confirmed_plan.get("entry") is not None):
            st.markdown("### ⚖️ Entry split — DoopieCash vs Confirmed")
            col_aggr, col_safe = st.columns(2)

            with col_aggr:
                st.markdown(f"**{left_title}**")
                st.caption(left_caption)
                if split_limit_plan.get("entry") is not None:
                    st.metric("Limit entry", fmt_price_eur(float(split_limit_plan["entry"])))
                    st.caption(
                        f"Stop {fmt_price_eur(float(split_limit_plan['stop']))} • "
                        f"Target {fmt_price_eur(float(split_limit_plan['target']))} • "
                        f"Status: {split_limit_plan.get('status')}"
                    )
                    if split_limit_plan.get("metrics") is not None:
                        lp_metrics = split_limit_plan["metrics"]
                        st.caption(
                            f"RR 1:{float(lp_metrics['rr']):.2f} • Netto {fmt_eur(float(lp_metrics['net_profit_eur']))}"
                        )
                    if split_limit_plan.get("distance_to_entry_pct") is not None:
                        st.caption(f"Afstand tot entry: {float(split_limit_plan['distance_to_entry_pct']):.2f}%")
                    st.caption(str(split_limit_plan.get("reason", "")))
                else:
                    st.caption("Geen agressieve limit-opzet beschikbaar.")

            with col_safe:
                st.markdown("**Safe**")
                st.caption("Confirmed reclaim • slechtere prijs • hogere succeskans")
                if split_confirmed_plan.get("entry") is not None:
                    st.metric("Confirmed entry", fmt_price_eur(float(split_confirmed_plan["entry"])))
                    st.caption(
                        f"Stop {fmt_price_eur(float(split_confirmed_plan['stop']))} • "
                        f"Target {fmt_price_eur(float(split_confirmed_plan['target']))} • "
                        f"Status: {split_confirmed_plan.get('status')}"
                    )
                    if split_confirmed_plan.get("metrics") is not None:
                        cp_metrics = split_confirmed_plan["metrics"]
                        st.caption(
                            f"RR 1:{float(cp_metrics['rr']):.2f} • Netto {fmt_eur(float(cp_metrics['net_profit_eur']))}"
                        )
                    if split_confirmed_plan.get("trigger") is not None:
                        st.caption(f"Trigger: {fmt_price_eur(float(split_confirmed_plan['trigger']))}")
                    st.caption(str(split_confirmed_plan.get("reason", "")))
                else:
                    st.caption("Nog geen confirmed opzet beschikbaar.")

            st.markdown("#### 📝 Log trade naar journal")
            log_col1, log_col2, log_col3 = st.columns(3)
            with log_col1:
                if best_metrics is not None and st.button("Log beste trade", key=f"log_best_{selected_result['coin']}"):
                    entry = build_journal_entry(selected_result, best_side, "best", best_metrics)
                    append_trade_journal(entry)
                    st.success("Beste trade gelogd in journal.")
            with log_col2:
                if split_side is not None and split_limit_plan.get("metrics") is not None and st.button("Log limit plan", key=f"log_limit_{selected_result['coin']}"):
                    entry = build_journal_entry(selected_result, split_side, "limit", split_limit_plan.get("metrics"))
                    append_trade_journal(entry)
                    st.success("Limit plan gelogd in journal.")
            with log_col3:
                if split_side is not None and split_confirmed_plan.get("metrics") is not None and st.button("Log confirmed plan", key=f"log_conf_{selected_result['coin']}"):
                    entry = build_journal_entry(selected_result, split_side, "confirmed", split_confirmed_plan.get("metrics"))
                    append_trade_journal(entry)
                    st.success("Confirmed plan gelogd in journal.")

        with st.expander("📘 Uitleg", expanded=True):
            if best_metrics is not None:
                if best_side == "LONG":
                    st.write(
                        "Een **LONG** betekent dat de tool verwacht dat de prijs eerder omhoog kan gaan. "
                        "Je instap is het punt waarop je zou kopen. "
                        "De veiligheidsgrens is het punt waarop je uitstapt als het idee fout blijkt. "
                        "Het winstdoel is het punt waarop je winst zou nemen."
                    )
                else:
                    st.write(
                        "Een **SHORT** betekent dat de tool verwacht dat de prijs eerder omlaag kan gaan. "
                        "Je instap is het punt waarop je zou openen. "
                        "De veiligheidsgrens is het punt waarop je uitstapt als het idee fout blijkt. "
                        "Het winstdoel is het punt waarop je winst zou nemen."
                    )
            else:
                st.write(
                    "Op dit moment ziet de tool nog **geen sterke directe trade**. "
                    "Dat betekent meestal dat de verwachte winst te klein is, de entry al te laat is, of dat een vooraf geplande limit-zone logischer is."
                )

        with st.expander("🔬 Technische details", expanded=False):
            detail_col1, detail_col2 = st.columns(2)
            with detail_col1:
                st.markdown("**LONG scenario**")
                long_metrics = selected_result["long_metrics"]
                if long_metrics:
                    st.write(f"Instap: {fmt_price_eur(float(long_metrics['entry']))}")
                    st.write(f"Veiligheidsgrens: {fmt_price_eur(float(long_metrics['stop']))}")
                    st.write(f"Winstdoel: {fmt_price_eur(float(long_metrics['target']))}")
                    st.write(f"Risico in prijs: {float(long_metrics['risk_pct_price']):.2f}%")
                    st.write(f"Location: {selected_result.get('long_location', {}).get('quality', 'UNKNOWN')}")
                    st.write(f"Risico in euro: {fmt_eur(float(long_metrics['risk_eur']))}")
                    st.write(f"Positiegrootte: {float(long_metrics['position_size']):.6f} {selected_result['coin']}")
                    st.write(f"Inleg nodig: {fmt_eur(float(long_metrics['entry']) * float(long_metrics['position_size']))}")
                    st.write(f"Gebruikt van account: {(float(long_metrics['entry']) * float(long_metrics['position_size']) / account_size * 100) if account_size > 0 else 0.0:.1f}%")
                    st.write(f"Bruto winst: {fmt_eur(float(long_metrics['gross_profit_eur']))}")
                    st.write(f"Kosten totaal: {fmt_eur(float(long_metrics['total_fees_eur']))}")
                    st.write(f"Netto winst: {fmt_eur(float(long_metrics['net_profit_eur']))}")
                    st.write(f"Geschat verlies bij stop: {fmt_eur(float(long_metrics['net_loss_if_stopped_eur']))}")
                    st.write(f"Winst/risico verhouding: 1 : {float(long_metrics['rr']):.2f}")
                    st.write(f"Interessant na kosten: {'Ja' if selected_result['long_valid'] else 'Nee'}")
            with detail_col2:
                st.markdown("**SHORT scenario**")
                short_metrics = selected_result["short_metrics"]
                if short_metrics:
                    st.write(f"Instap: {fmt_price_eur(float(short_metrics['entry']))}")
                    st.write(f"Veiligheidsgrens: {fmt_price_eur(float(short_metrics['stop']))}")
                    st.write(f"Winstdoel: {fmt_price_eur(float(short_metrics['target']))}")
                    st.write(f"Risico in prijs: {float(short_metrics['risk_pct_price']):.2f}%")
                    st.write(f"Location: {selected_result.get('short_location', {}).get('quality', 'UNKNOWN')}")
                    st.write(f"Risico in euro: {fmt_eur(float(short_metrics['risk_eur']))}")
                    st.write(f"Positiegrootte: {float(short_metrics['position_size']):.6f} {selected_result['coin']}")
                    st.write(f"Inleg nodig: {fmt_eur(float(short_metrics['entry']) * float(short_metrics['position_size']))}")
                    st.write(f"Gebruikt van account: {(float(short_metrics['entry']) * float(short_metrics['position_size']) / account_size * 100) if account_size > 0 else 0.0:.1f}%")
                    st.write(f"Bruto winst: {fmt_eur(float(short_metrics['gross_profit_eur']))}")
                    st.write(f"Handelskosten totaal: {fmt_eur(float(short_metrics['entry_fee_eur']) + float(short_metrics['exit_fee_eur']))}")
                    st.write(f"Leenkosten schatting: {fmt_eur(float(short_metrics.get('borrow_fee_eur', 0.0)))}")
                    st.write(f"Kosten totaal: {fmt_eur(float(short_metrics['total_fees_eur']))}")
                    st.write(f"Netto winst: {fmt_eur(float(short_metrics['net_profit_eur']))}")
                    st.write(f"Geschat verlies bij stop: {fmt_eur(float(short_metrics['net_loss_if_stopped_eur']))}")
                    st.write(f"Winst/risico verhouding: 1 : {float(short_metrics['rr']):.2f}")
                    st.write(f"Interessant na kosten: {'Ja' if selected_result['short_valid'] else 'Nee'}")
            st.markdown("---")
            st.markdown("**Trendcheck**")
            st.write(f"Korte trend ({timeframe_label}): **{selected_result['entry_bias']}**")
            st.write(f"Grote trend ({selected_result['higher_timeframe_label']}): **{selected_result['higher_bias']}**")
            st.write(f"Eindoordeel: **{selected_result.get('trend_label', selected_result['combined_bias'])}**")


# =========================================================
# Journal tab
# =========================================================
with tab_journal:
    st.subheader("📝 Trade journal / validatie")
    journal_df = load_trade_journal()

    with st.expander("➕ Handmatige trade toevoegen", expanded=False):
        m1, m2, m3 = st.columns(3)
        manual_coin = m1.selectbox("Coin", list(COINS.keys()), key="manual_journal_coin")
        manual_side = m2.selectbox("Side", ["LONG", "SHORT"], key="manual_journal_side")
        manual_plan_type = m3.selectbox("Plan type", ["manual_limit", "manual_confirmed", "manual_other"], key="manual_journal_plan_type")

        m4, m5, m6 = st.columns(3)
        tf_keys = list(TIMEFRAMES.keys())
        manual_scanner_tf = m4.selectbox("Scanner TF", tf_keys, index=tf_keys.index("15m") if "15m" in tf_keys else 0, key="manual_journal_scanner_tf")
        manual_trigger_tf = m5.selectbox("Trigger TF", tf_keys, index=tf_keys.index("5m") if "5m" in tf_keys else 0, key="manual_journal_trigger_tf")
        manual_setup_tf = m6.selectbox("Setup TF", tf_keys, index=tf_keys.index("15m") if "15m" in tf_keys else 0, key="manual_journal_setup_tf")

        m7, m8, m9 = st.columns(3)
        manual_trend_tf = m7.selectbox("Trend TF", tf_keys, index=tf_keys.index("1h") if "1h" in tf_keys else 0, key="manual_journal_trend_tf")
        manual_context = m8.selectbox("Context", ["aligned_bullish", "aligned_bearish", "bullish_pullback", "bearish_pullback", "mixed", "neutral"], key="manual_journal_context")
        manual_location = m9.selectbox("Location quality", ["A_ENTRY", "B_ENTRY", "LATE", "SKIP", "UNKNOWN"], index=4, key="manual_journal_location")

        m10, m11, m12 = st.columns(3)
        manual_entry = m10.number_input("Entry", min_value=0.0, value=0.0, step=0.0001, format="%.8f", key="manual_journal_entry")
        manual_stop = m11.number_input("Stop", min_value=0.0, value=0.0, step=0.0001, format="%.8f", key="manual_journal_stop")
        manual_target = m12.number_input("Target", min_value=0.0, value=0.0, step=0.0001, format="%.8f", key="manual_journal_target")

        manual_trend_label = st.text_input("Trend label", value="", key="manual_journal_trend_label")
        manual_notes = st.text_input("Notities", value="", key="manual_journal_notes")

        if st.button("Voeg handmatige trade toe", key="manual_journal_add_btn"):
            if manual_entry <= 0 or manual_stop <= 0 or manual_target <= 0:
                st.error("Vul entry, stop en target in met waarden groter dan 0.")
            elif manual_side == "LONG" and not (manual_stop < manual_entry < manual_target):
                st.error("Voor LONG moet gelden: stop < entry < target.")
            elif manual_side == "SHORT" and not (manual_target < manual_entry < manual_stop):
                st.error("Voor SHORT moet gelden: target < entry < stop.")
            else:
                manual_entry_row = build_manual_journal_entry(
                    coin=manual_coin,
                    scanner_tf=manual_scanner_tf,
                    trigger_tf=manual_trigger_tf,
                    setup_tf=manual_setup_tf,
                    trend_tf=manual_trend_tf,
                    context=manual_context,
                    trend_label=manual_trend_label,
                    side=manual_side,
                    plan_type=manual_plan_type,
                    location_quality=manual_location,
                    entry=manual_entry,
                    stop=manual_stop,
                    target=manual_target,
                    notes=manual_notes,
                )
                append_trade_journal(manual_entry_row)
                st.success("Handmatige trade toegevoegd aan journal.")
                st.rerun()

    if journal_df.empty:
        st.info("Nog geen journal entries. Log eerst een trade vanuit de Trade-tab of voeg handmatig een trade toe.")
    else:
        open_count = int((journal_df["outcome"] == "OPEN").sum())
        tp_count = int((journal_df["outcome"] == "TP").sum())
        sl_count = int((journal_df["outcome"] == "SL").sum())
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Entries", len(journal_df))
        c2.metric("Open", open_count)
        c3.metric("TP", tp_count)
        c4.metric("SL", sl_count)

        st.dataframe(journal_df.sort_values("logged_at", ascending=False), use_container_width=True, hide_index=True)

        st.markdown("### Update uitkomst")
        open_df = journal_df[journal_df["outcome"] == "OPEN"].copy()

        if open_df.empty:
            st.caption("Geen open journal trades om bij te werken.")
        else:
            open_df["label"] = open_df.apply(
                lambda r: f"{r['journal_id']} • {r['coin']} • {r['side']} • {r['plan_type']} • entry {r['entry']}",
                axis=1
            )
            selected_label = st.selectbox("Kies open trade", open_df["label"].tolist(), key="journal_select_open")
            row = open_df[open_df["label"] == selected_label].iloc[0]
            selected_id = row["journal_id"]

            outcome = st.selectbox("Uitkomst", JOURNAL_OUTCOMES[1:], key="journal_outcome_select")
            notes = st.text_input("Notitie", value=str(row.get("notes") or ""), key="journal_notes_input")

            if st.button("Sla uitkomst op", key="journal_save_btn"):
                for text_col in ["outcome", "resolved_at", "notes"]:
                    journal_df[text_col] = journal_df[text_col].astype("object")

                resolved_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                journal_df.loc[journal_df["journal_id"] == selected_id, "outcome"] = str(outcome)
                journal_df.loc[journal_df["journal_id"] == selected_id, "resolved_at"] = str(resolved_now)
                journal_df.loc[journal_df["journal_id"] == selected_id, "notes"] = str(notes)
                save_trade_journal(journal_df)
                st.success("Journal bijgewerkt.")
                st.rerun()

        st.markdown("### Snelle validatie")
        closed_df = journal_df[journal_df["outcome"] != "OPEN"].copy()
        if closed_df.empty:
            st.caption("Nog geen gesloten trades om te evalueren.")
        else:
            by_plan = closed_df.groupby("plan_type")["outcome"].value_counts().unstack(fill_value=0)
            st.markdown("**Per plan_type**")
            st.dataframe(by_plan, use_container_width=True)

            if "location_quality" in closed_df.columns:
                by_location = closed_df.groupby("location_quality")["outcome"].value_counts().unstack(fill_value=0)
                st.markdown("**Per location quality**")
                st.dataframe(by_location, use_container_width=True)

            if "context" in closed_df.columns:
                by_context = closed_df.groupby("context")["outcome"].value_counts().unstack(fill_value=0)
                st.markdown("**Per context**")
                st.dataframe(by_context, use_container_width=True)
