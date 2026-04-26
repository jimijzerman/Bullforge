import requests
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from typing import Optional, Dict, List, Tuple
from datetime import datetime
import time
from pathlib import Path

st.set_page_config(page_title="BullForge", layout="wide")

# =========================================================
# Config
# =========================================================
REFRESH_ANALYSIS_SEC = 20
SCANNER_CACHE_SEC = 60
REFRESH_UI_SEC = 2
LIVE_PRICE_CACHE_SEC = 2
AUTO_SCAN_TICK_SEC = 3

def get_auto_scan_interval_sec(timeframe_label: str) -> int:
    return {
        "1m": 5,
        "5m": 8,
        "15m": 12,
        "30m": 18,
        "1h": 30,
        "4h": 45,
        "1d": 60,
    }.get(timeframe_label, 12)

if hasattr(st, "fragment"):
    bf_fragment = st.fragment
else:
    def bf_fragment(run_every=None):
        def decorator(func):
            return func
        return decorator


BASE_URL = "https://api.bitvavo.com"
API_PREFIX = "/v2"

COINS = {
    "BTC": {"bitvavo_market": "BTC-EUR"},
    "ETH": {"bitvavo_market": "ETH-EUR"},
    "SOL": {"bitvavo_market": "SOL-EUR"},
    "TAO": {"bitvavo_market": "TAO-EUR"},
    "XRP": {"bitvavo_market": "XRP-EUR"},
    "XLM": {"bitvavo_market": "XLM-EUR"},
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
    # 15m is daytrading: 15m/1h bepalen trade-zones; 4h is alleen macro-context.
    "15m": {"trigger": "15m", "setup": "15m", "trend": "1h"},
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


LEVEL_REFINEMENT_MAP = {
    "1m": ["15m"],
    "5m": ["15m", "1h"],
    "15m": ["15m", "1h"],
    "30m": ["15m", "1h"],
    # 1h gebruikt eigen recente swing-levels + 4h context, niet 1d als actieve entry.
    "1h": ["1h", "4h"],
    "4h": ["4h"],
    "1d": ["1d"],
}

def get_refinement_timeframes(base_timeframe_label: str) -> List[str]:
    """
    Daytrading refinement:
    - 4h / 1d = hoofdstructuur
    - 1h / 15m = refinement
    - 5m / 1m = timing only
    """
    refinement = LEVEL_REFINEMENT_MAP.get(base_timeframe_label, ["1h"])
    cleaned: List[str] = []
    for tf in refinement:
        if tf in {"1m", "5m"}:
            continue
        if tf not in cleaned:
            cleaned.append(tf)
    return cleaned or ["1h"]


def is_daytrade_timeframe(timeframe_label: str) -> bool:
    return str(timeframe_label) in {"1m", "5m", "15m", "30m"}


def get_level_authority_timeframes(base_timeframe_label: str) -> List[str]:
    """
    Lage timeframes mogen niet gedomineerd worden door 1d-levels.
    15m/1h leveren intraday-zones; 4h is macro-context.
    """
    tf = str(base_timeframe_label)
    if tf in {"1m", "5m", "15m"}:
        return ["15m", "1h", "4h"]
    if tf == "30m":
        return ["15m", "1h", "4h"]
    if tf == "1h":
        # 1d blijft macro-context; actieve 1h entry/target zones komen uit 1h/4h.
        return ["1h", "4h"]
    return ["4h", "1d"]


def prepare_chart_focus_df(df: Optional[pd.DataFrame], timeframe_label: str) -> Optional[pd.DataFrame]:
    """Toon op lage TF een rustige intraday-chart in plaats van alle oude macro candles."""
    if df is None or df.empty:
        return df
    lookback = {"1m": 160, "5m": 140, "15m": 110, "30m": 100}.get(str(timeframe_label))
    if lookback is None:
        return df
    return df.tail(min(len(df), lookback)).copy()

DEFAULT_MAKER_FEE_PCT = 0.09
DEFAULT_TAKER_FEE_PCT = 0.18

DEFAULT_SHORT_LIQUIDATION_FEE_PCT = 2.0

DEFAULT_SHORT_BORROW_HOURLY_PCT = {
    "BTC": 0.01,
    "ETH": 0.01,
    "SOL": 0.012,
    "TAO": 0.02,
    "XRP": 0.012,
    "XLM": 0.012,
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

# Clean active entry architecture:
# - Early price-action
# - Retest-breakout
# Old limit/balanced routing is no longer used in active decisioning.
ENTRY_MODES = {
    "Early price-action": "doopiecash",
    "Retest-breakout": "confirmation",
}

JOURNAL_FILE = Path("bullforge_trade_journal.csv")
JOURNAL_OUTCOMES = ["OPEN", "TP", "SL", "BE", "MANUAL_EXIT", "NO_FILL"]

DAILY_RESULTS_FILE = Path("bullforge_daily_results.csv")
DAILY_RESULT_TYPES = ["WIN", "LOSS", "NO_TRADE"]



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


def load_daily_results() -> pd.DataFrame:
    columns = [
        "result_id", "date", "coin", "result_type", "pnl_eur", "pnl_pct",
        "trades_count", "notes", "logged_at"
    ]
    text_columns = ["result_id", "date", "coin", "result_type", "notes", "logged_at"]

    if DAILY_RESULTS_FILE.exists():
        try:
            df = pd.read_csv(DAILY_RESULTS_FILE)
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


def save_daily_results(df: pd.DataFrame) -> None:
    df_to_save = df.copy()
    for col in ["result_id", "date", "coin", "result_type", "notes", "logged_at"]:
        if col in df_to_save.columns:
            df_to_save[col] = df_to_save[col].astype("object")
    df_to_save.to_csv(DAILY_RESULTS_FILE, index=False)


def build_daily_result_entry(
    date_value: str,
    coin: str,
    result_type: str,
    pnl_eur: float,
    pnl_pct: float,
    trades_count: int,
    notes: str = "",
) -> Dict[str, object]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result_id = f"{date_value}-{coin}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    return {
        "result_id": result_id,
        "date": str(date_value),
        "coin": coin,
        "result_type": result_type,
        "pnl_eur": float(pnl_eur),
        "pnl_pct": float(pnl_pct),
        "trades_count": int(trades_count),
        "notes": notes,
        "logged_at": now,
    }


def append_daily_result(entry: Dict[str, object]) -> None:
    df = load_daily_results()
    df = pd.concat([df, pd.DataFrame([entry])], ignore_index=True)
    save_daily_results(df)


# =========================================================
# Learning engine helpers
# =========================================================
def _safe_pct(win_count: float, total_count: float) -> float:
    return round((float(win_count) / float(total_count) * 100.0), 1) if total_count else 0.0


def get_closed_trade_journal(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    closed = df[df["outcome"].fillna("OPEN") != "OPEN"].copy()
    if closed.empty:
        return closed
    for col in ["rr", "net_profit_eur", "conservative_net", "score", "entry", "stop", "target", "current_price"]:
        if col in closed.columns:
            closed[col] = pd.to_numeric(closed[col], errors="coerce")
    closed["is_win"] = closed["outcome"].astype(str).isin(["TP"])
    closed["is_loss"] = closed["outcome"].astype(str).isin(["SL"])
    closed["is_be"] = closed["outcome"].astype(str).isin(["BE"])
    closed["is_manual"] = closed["outcome"].astype(str).isin(["MANUAL_EXIT"])
    closed["setup_family"] = closed["entry_variant"].astype(str).replace({
        "early_price_action": "early_price_action",
        "retest_breakout": "retest_breakout",
        "manual": "manual",
    })
    closed["setup_family"] = closed["setup_family"].where(closed["setup_family"].isin(["early_price_action", "retest_breakout", "manual"]), closed["plan_type"].astype(str))
    return closed


def summarize_group_performance(df: pd.DataFrame, group_col: str, min_trades: int = 1) -> pd.DataFrame:
    if df is None or df.empty or group_col not in df.columns:
        return pd.DataFrame(columns=[group_col, "trades", "winrate_pct", "tp", "sl", "be", "manual_exit", "avg_rr", "avg_score"])
    tmp = df.copy()
    tmp[group_col] = tmp[group_col].fillna("Onbekend").astype(str)
    grouped = tmp.groupby(group_col, dropna=False).agg(
        trades=("journal_id", "count"),
        tp=("is_win", "sum"),
        sl=("is_loss", "sum"),
        be=("is_be", "sum"),
        manual_exit=("is_manual", "sum"),
        avg_rr=("rr", "mean"),
        avg_score=("score", "mean"),
    ).reset_index()
    grouped["winrate_pct"] = grouped.apply(lambda r: _safe_pct(r["tp"], r["trades"]), axis=1)
    grouped["lossrate_pct"] = grouped.apply(lambda r: _safe_pct(r["sl"], r["trades"]), axis=1)
    grouped = grouped[grouped["trades"] >= int(min_trades)].copy()
    grouped["avg_rr"] = grouped["avg_rr"].fillna(0.0).round(2)
    grouped["avg_score"] = grouped["avg_score"].fillna(0.0).round(1)
    grouped = grouped.sort_values(["winrate_pct", "trades", "avg_rr"], ascending=[False, False, False]).reset_index(drop=True)
    return grouped


def build_tp_sl_feedback(closed_df: pd.DataFrame) -> Dict[str, List[str]]:
    good: List[str] = []
    bad: List[str] = []

    if closed_df is None or closed_df.empty:
        return {"good": good, "bad": bad}

    total = len(closed_df)
    tp_count = int(closed_df["is_win"].sum())
    sl_count = int(closed_df["is_loss"].sum())
    be_count = int(closed_df["is_be"].sum())
    manual_count = int(closed_df["is_manual"].sum())

    if tp_count >= max(3, sl_count + 1):
        good.append("TP wordt relatief vaak geraakt; target-structuur lijkt bruikbaar.")
    if sl_count >= max(3, tp_count + 1):
        bad.append("SL wordt vaker geraakt dan TP; kijk kritisch naar entrykwaliteit of stopruimte.")
    if (be_count + manual_count) >= max(3, int(total * 0.35)):
        bad.append("Veel BE/manual exits; entries lijken soms goed maar exits of TP-structuur kunnen beter.")
    if tp_count >= 2 and (be_count + manual_count) >= 2:
        bad.append("Een deel van de trades komt wel op gang, maar wordt niet netjes afgerond; TP staat mogelijk te ambitieus of management is te vroeg.")

    if "location_quality" in closed_df.columns:
        loc = summarize_group_performance(closed_df, "location_quality")
        if not loc.empty:
            a_row = loc[loc["location_quality"] == "A_ENTRY"]
            late_row = loc[loc["location_quality"] == "LATE"]
            if not a_row.empty and float(a_row.iloc[0]["winrate_pct"]) >= 50:
                good.append("A_ENTRY setups presteren het best; dicht op de zone blijven lijkt goed te werken.")
            if not late_row.empty and float(late_row.iloc[0]["lossrate_pct"]) >= 45:
                bad.append("LATE entries verliezen relatief vaak; je bent daar waarschijnlijk te laat in de move.")
            b_row = loc[loc["location_quality"] == "B_ENTRY"]
            if not b_row.empty and float(b_row.iloc[0]["sl"]) >= float(b_row.iloc[0]["tp"]) and int(b_row.iloc[0]["trades"]) >= 3:
                bad.append("B_ENTRY setups raken vaak SL; je stop staat daar mogelijk te strak of je entry is minder scherp.")

    if "setup_family" in closed_df.columns:
        fam = summarize_group_performance(closed_df, "setup_family")
        if not fam.empty:
            fam = fam.set_index("setup_family")
            if "early_price_action" in fam.index and "retest_breakout" in fam.index:
                early_wr = float(fam.loc["early_price_action", "winrate_pct"])
                retest_wr = float(fam.loc["retest_breakout", "winrate_pct"])
                if early_wr >= retest_wr + 10:
                    good.append("Early price-action werkt duidelijk beter dan retest-breakout in jouw data.")
                elif retest_wr >= early_wr + 10:
                    good.append("Retest-breakout werkt duidelijk beter dan early price-action in jouw data.")

    return {"good": good[:4], "bad": bad[:4]}


def build_learning_engine(journal_df: pd.DataFrame, daily_df: pd.DataFrame) -> Dict[str, object]:
    closed_df = get_closed_trade_journal(journal_df)
    result: Dict[str, object] = {
        "closed_df": closed_df,
        "coin_perf": summarize_group_performance(closed_df, "coin"),
        "timeframe_perf": summarize_group_performance(closed_df, "scanner_tf"),
        "setup_perf": summarize_group_performance(closed_df, "setup_family"),
        "side_perf": summarize_group_performance(closed_df, "side"),
        "context_perf": summarize_group_performance(closed_df, "context"),
        "location_perf": summarize_group_performance(closed_df, "location_quality"),
        "good_insights": [],
        "bad_insights": [],
        "top_working": [],
        "top_improve": [],
    }

    if closed_df.empty:
        return result

    coin_perf = result["coin_perf"]
    timeframe_perf = result["timeframe_perf"]
    setup_perf = result["setup_perf"]
    side_perf = result["side_perf"]
    context_perf = result["context_perf"]
    location_perf = result["location_perf"]

    insights_good: List[str] = []
    insights_bad: List[str] = []

    if not coin_perf.empty:
        top_coin = coin_perf.iloc[0]
        insights_good.append(f"{top_coin['coin']} werkt nu het best ({int(top_coin['trades'])} trades, {float(top_coin['winrate_pct']):.1f}% winrate).")
        weak_coin = coin_perf.sort_values(["winrate_pct", "trades"], ascending=[True, False]).iloc[0]
        if int(weak_coin["trades"]) >= 2:
            insights_bad.append(f"{weak_coin['coin']} presteert zwakker ({int(weak_coin['trades'])} trades, {float(weak_coin['winrate_pct']):.1f}% winrate).")

    if not timeframe_perf.empty:
        best_tf = timeframe_perf.iloc[0]
        insights_good.append(f"Timeframe {best_tf['scanner_tf']} werkt het best ({float(best_tf['winrate_pct']):.1f}% winrate).")
        weak_tf = timeframe_perf.sort_values(["winrate_pct", "trades"], ascending=[True, False]).iloc[0]
        if int(weak_tf["trades"]) >= 2:
            insights_bad.append(f"Timeframe {weak_tf['scanner_tf']} blijft achter; daar moet je scherper op entries/exits letten.")

    if not setup_perf.empty:
        best_setup = setup_perf.iloc[0]
        insights_good.append(f"{best_setup['setup_family']} is je sterkste setup-type ({float(best_setup['winrate_pct']):.1f}% winrate).")
        weak_setup = setup_perf.sort_values(["winrate_pct", "trades"], ascending=[True, False]).iloc[0]
        if int(weak_setup["trades"]) >= 2:
            insights_bad.append(f"{weak_setup['setup_family']} presteert zwakker; check of deze setup minder goed bij jouw stijl past.")

    if not side_perf.empty and len(side_perf) >= 2:
        best_side = side_perf.iloc[0]
        weak_side = side_perf.sort_values(["winrate_pct", "trades"], ascending=[True, False]).iloc[0]
        insights_good.append(f"{best_side['side']} trades doen het beter ({float(best_side['winrate_pct']):.1f}% winrate).")
        if best_side["side"] != weak_side["side"] and int(weak_side["trades"]) >= 2:
            insights_bad.append(f"{weak_side['side']} trades lopen achter; misschien past de markt daar minder goed bij of blokkeer je te weinig.")

    if not context_perf.empty:
        good_context = context_perf.iloc[0]
        insights_good.append(f"Context '{good_context['context']}' werkt relatief goed ({float(good_context['winrate_pct']):.1f}% winrate).")
        weak_context = context_perf.sort_values(["winrate_pct", "trades"], ascending=[True, False]).iloc[0]
        if int(weak_context["trades"]) >= 2:
            insights_bad.append(f"Context '{weak_context['context']}' geeft zwakkere resultaten; daar beter filteren.")

    if not location_perf.empty:
        good_loc = location_perf.iloc[0]
        insights_good.append(f"Location {good_loc['location_quality']} werkt het best ({float(good_loc['winrate_pct']):.1f}% winrate).")
        weak_loc = location_perf.sort_values(["winrate_pct", "trades"], ascending=[True, False]).iloc[0]
        if int(weak_loc["trades"]) >= 2:
            insights_bad.append(f"Location {weak_loc['location_quality']} presteert zwak; entries zijn daar waarschijnlijk minder scherp.")

    tp_sl_feedback = build_tp_sl_feedback(closed_df)
    insights_good.extend(tp_sl_feedback["good"])
    insights_bad.extend(tp_sl_feedback["bad"])

    if daily_df is not None and not daily_df.empty:
        daily_tmp = daily_df.copy()
        daily_tmp["pnl_pct"] = pd.to_numeric(daily_tmp["pnl_pct"], errors="coerce").fillna(0.0)
        daily_tmp["pnl_eur"] = pd.to_numeric(daily_tmp["pnl_eur"], errors="coerce").fillna(0.0)
        if float(daily_tmp["pnl_pct"].mean()) > 0:
            insights_good.append(f"Je gemiddelde dagresultaat staat positief ({daily_tmp['pnl_pct'].mean():.2f}% per entry).")
        if int((daily_tmp["result_type"].astype(str) == "NO_TRADE").sum()) > int(len(daily_tmp) * 0.4):
            insights_bad.append("Je hebt relatief veel no-trade dagen; misschien filter je te hard of wacht je te lang.")

    # dedupe while keeping order
    def _dedupe(items: List[str]) -> List[str]:
        seen = set()
        out = []
        for item in items:
            if item and item not in seen:
                seen.add(item)
                out.append(item)
        return out

    insights_good = _dedupe(insights_good)
    insights_bad = _dedupe(insights_bad)

    result["good_insights"] = insights_good[:6]
    result["bad_insights"] = insights_bad[:6]
    result["top_working"] = insights_good[:3]
    result["top_improve"] = insights_bad[:3]
    return result


# =========================================================
# Phase 11 - Advanced Journal / Late Signal Learning
# =========================================================
ADVANCED_JOURNAL_COLUMNS = [
    "plan_mode_active",
    "plan_preplaced",
    "zone_touch_before_signal",
    "fill_status",
    "tp_miss_pct",
    "sl_too_tight_flag",
    "late_signal_flag",
]
ADVANCED_TEXT_COLUMNS = ["plan_mode_active", "plan_preplaced", "zone_touch_before_signal", "fill_status", "sl_too_tight_flag", "late_signal_flag"]
ADVANCED_NUMERIC_COLUMNS = ["tp_miss_pct"]
FILL_STATUS_OPTIONS = ["UNKNOWN", "NOT_PLACED", "PENDING", "FILLED", "PARTIAL", "MISSED", "CANCELLED"]


def _journal_base_columns_v11() -> List[str]:
    return [
        "journal_id", "logged_at", "coin", "scanner_tf", "trigger_tf", "setup_tf", "trend_tf",
        "context", "trend_label", "side", "plan_type", "entry_variant", "location_quality",
        "entry", "stop", "target", "rr", "net_profit_eur", "conservative_net", "score",
        "current_price", "outcome", "resolved_at", "notes",
        "plan_mode_active", "plan_preplaced", "zone_touch_before_signal", "fill_status",
        "tp_miss_pct", "sl_too_tight_flag", "late_signal_flag",
    ]


def _journal_text_columns_v11() -> List[str]:
    return [
        "journal_id", "logged_at", "coin", "scanner_tf", "trigger_tf", "setup_tf", "trend_tf",
        "context", "trend_label", "side", "plan_type", "entry_variant", "location_quality",
        "outcome", "resolved_at", "notes",
        "plan_mode_active", "plan_preplaced", "zone_touch_before_signal", "fill_status",
        "sl_too_tight_flag", "late_signal_flag",
    ]


def _normalize_bool_text(value: object, default: str = "UNKNOWN") -> str:
    if value is None:
        return default
    text = str(value).strip().upper()
    if text in {"TRUE", "1", "YES", "JA", "Y"}:
        return "YES"
    if text in {"FALSE", "0", "NO", "NEE", "N"}:
        return "NO"
    if text in {"YES", "NO", "UNKNOWN"}:
        return text
    return default


def _normalize_fill_status(value: object) -> str:
    text = str(value or "UNKNOWN").strip().upper().replace(" ", "_").replace("-", "_")
    return text if text in FILL_STATUS_OPTIONS else "UNKNOWN"


def _ensure_advanced_journal_columns(df: pd.DataFrame) -> pd.DataFrame:
    columns = _journal_base_columns_v11()
    for col in columns:
        if col not in df.columns:
            df[col] = None
    df = df[columns].copy()

    for col in _journal_text_columns_v11():
        if col in df.columns:
            df[col] = df[col].astype("object")

    for col in ["entry", "stop", "target", "rr", "net_profit_eur", "conservative_net", "score", "current_price", "tp_miss_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["plan_mode_active", "plan_preplaced", "zone_touch_before_signal", "sl_too_tight_flag", "late_signal_flag"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda value: _normalize_bool_text(value))

    if "fill_status" in df.columns:
        df["fill_status"] = df["fill_status"].apply(_normalize_fill_status)

    return df


def load_trade_journal() -> pd.DataFrame:
    columns = _journal_base_columns_v11()
    if JOURNAL_FILE.exists():
        try:
            df = pd.read_csv(JOURNAL_FILE)
            return _ensure_advanced_journal_columns(df)
        except Exception:
            return _ensure_advanced_journal_columns(pd.DataFrame(columns=columns))
    return _ensure_advanced_journal_columns(pd.DataFrame(columns=columns))


def save_trade_journal(df: pd.DataFrame) -> None:
    df_to_save = _ensure_advanced_journal_columns(df.copy())
    df_to_save.to_csv(JOURNAL_FILE, index=False)


def _infer_zone_touch_before_signal(selected_result: Dict[str, object], side: str, metrics: Optional[Dict[str, object]]) -> str:
    if metrics is None:
        return "UNKNOWN"
    side_u = str(side).upper()
    timing = selected_result.get("setup_timing", {}) or {}
    timing_label = str(timing.get("long_timing") if side_u == "LONG" else timing.get("short_timing"))
    location_info = selected_result.get("long_location", {}) if side_u == "LONG" else selected_result.get("short_location", {})
    location_quality = str((location_info or {}).get("quality", "UNKNOWN"))
    plan_type = str(selected_result.get("trade_opportunity", {}).get("status", ""))

    if timing_label in {"READY", "NEAR"} or location_quality in {"A_ENTRY", "B_ENTRY"} or plan_type == "PLAN":
        return "YES"
    if timing_label in {"MISSED", "HANDS_OFF"} or location_quality in {"LATE", "SKIP"}:
        return "YES"
    return "UNKNOWN"


def _infer_late_signal_flag(selected_result: Dict[str, object], side: str, metrics: Optional[Dict[str, object]]) -> str:
    if metrics is None:
        return "UNKNOWN"
    side_u = str(side).upper()
    location_info = selected_result.get("long_location", {}) if side_u == "LONG" else selected_result.get("short_location", {})
    location_quality = str((location_info or {}).get("quality", "UNKNOWN"))
    timing = selected_result.get("setup_timing", {}) or {}
    timing_label = str(timing.get("long_timing") if side_u == "LONG" else timing.get("short_timing"))
    trade_status = str((selected_result.get("trade_opportunity", {}) or {}).get("status", ""))
    if location_quality in {"LATE", "SKIP"} or timing_label == "MISSED" or trade_status == "MISSED":
        return "YES"
    if location_quality in {"A_ENTRY", "B_ENTRY"} or timing_label in {"READY", "NEAR"}:
        return "NO"
    return "UNKNOWN"


def _infer_sl_too_tight_flag(metrics: Optional[Dict[str, object]], selected_result: Optional[Dict[str, object]] = None, side: str = "") -> str:
    if not isinstance(metrics, dict):
        return "UNKNOWN"
    rr = _safe_float(metrics.get("rr"), 0.0)
    risk_pct = _safe_float(metrics.get("risk_pct_price"), 0.0)
    tf = str((selected_result or {}).get("timeframe_label", ""))
    min_risk = {"1m": 0.05, "5m": 0.09, "15m": 0.14, "30m": 0.20, "1h": 0.30, "4h": 0.50, "1d": 0.80}.get(tf, 0.10)
    if risk_pct > 0 and risk_pct < min_risk:
        return "YES"
    if rr > 4.0 and risk_pct < min_risk * 1.35:
        return "YES"
    if risk_pct > 0:
        return "NO"
    return "UNKNOWN"


def build_advanced_journal_entry(
    selected_result: Dict[str, object],
    side: str,
    plan_type: str,
    metrics: Optional[Dict[str, object]],
    plan_mode_active: Optional[object] = None,
    plan_preplaced: Optional[object] = None,
    zone_touch_before_signal: Optional[object] = None,
    fill_status: Optional[str] = None,
    tp_miss_pct: Optional[float] = None,
    sl_too_tight_flag: Optional[object] = None,
    late_signal_flag: Optional[object] = None,
) -> Dict[str, object]:
    location_info = selected_result.get("long_location", {}) if str(side).upper() == "LONG" else selected_result.get("short_location", {})
    trade_opportunity = selected_result.get("trade_opportunity", {}) or {}
    best_plan_candidate = trade_opportunity.get("best_plan_candidate") if isinstance(trade_opportunity, dict) else None
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    journal_id = f"{selected_result.get('coin','UNK')}-{side}-{plan_type}-{datetime.now().strftime('%Y%m%d%H%M%S')}"

    inferred_plan_active = plan_mode_active
    if inferred_plan_active is None:
        inferred_plan_active = "YES" if best_plan_candidate is not None or str(plan_type).lower() in {"plan", "best_plan"} else "NO"

    inferred_preplaced = plan_preplaced
    if inferred_preplaced is None:
        inferred_preplaced = "YES" if str(plan_type).lower() in {"plan", "best_plan", "preplaced"} else "NO"

    inferred_zone_touch = zone_touch_before_signal if zone_touch_before_signal is not None else _infer_zone_touch_before_signal(selected_result, side, metrics)
    inferred_late = late_signal_flag if late_signal_flag is not None else _infer_late_signal_flag(selected_result, side, metrics)
    inferred_sl_tight = sl_too_tight_flag if sl_too_tight_flag is not None else _infer_sl_too_tight_flag(metrics, selected_result, side)

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
        "location_quality": location_info.get("quality") if isinstance(location_info, dict) else None,
        "entry": float(metrics["entry"]) if metrics else None,
        "stop": float(metrics["stop"]) if metrics else None,
        "target": float(metrics["target"]) if metrics else None,
        "rr": float(metrics["rr"]) if metrics else None,
        "net_profit_eur": float(metrics["net_profit_eur"]) if metrics and metrics.get("net_profit_eur") is not None else None,
        "conservative_net": float(selected_result.get("conservative_best_net") or 0.0) if plan_type == "best" else None,
        "score": float(selected_result.get("score") or 0.0),
        "current_price": float(selected_result.get("current_price") or 0.0) if selected_result.get("current_price") is not None else None,
        "outcome": "OPEN",
        "resolved_at": None,
        "notes": "",
        "plan_mode_active": _normalize_bool_text(inferred_plan_active),
        "plan_preplaced": _normalize_bool_text(inferred_preplaced),
        "zone_touch_before_signal": _normalize_bool_text(inferred_zone_touch),
        "fill_status": _normalize_fill_status(fill_status or "UNKNOWN"),
        "tp_miss_pct": float(tp_miss_pct) if tp_miss_pct is not None else None,
        "sl_too_tight_flag": _normalize_bool_text(inferred_sl_tight),
        "late_signal_flag": _normalize_bool_text(inferred_late),
    }


def build_journal_entry(
    selected_result: Dict[str, object],
    side: str,
    plan_type: str,
    metrics: Optional[Dict[str, object]],
) -> Dict[str, object]:
    return build_advanced_journal_entry(selected_result, side, plan_type, metrics)


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
        "plan_mode_active": "UNKNOWN",
        "plan_preplaced": "NO",
        "zone_touch_before_signal": "UNKNOWN",
        "fill_status": "UNKNOWN",
        "tp_miss_pct": None,
        "sl_too_tight_flag": "UNKNOWN",
        "late_signal_flag": "UNKNOWN",
    }


def append_trade_journal(entry: Dict[str, object]) -> None:
    df = load_trade_journal()
    df = pd.concat([df, pd.DataFrame([entry])], ignore_index=True)
    save_trade_journal(df)


def get_closed_trade_journal(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = _ensure_advanced_journal_columns(df.copy())
    closed = df[df["outcome"].fillna("OPEN") != "OPEN"].copy()
    if closed.empty:
        return closed
    for col in ["rr", "net_profit_eur", "conservative_net", "score", "entry", "stop", "target", "current_price", "tp_miss_pct"]:
        if col in closed.columns:
            closed[col] = pd.to_numeric(closed[col], errors="coerce")
    closed["is_win"] = closed["outcome"].astype(str).isin(["TP"])
    closed["is_loss"] = closed["outcome"].astype(str).isin(["SL"])
    closed["is_be"] = closed["outcome"].astype(str).isin(["BE"])
    closed["is_manual"] = closed["outcome"].astype(str).isin(["MANUAL_EXIT"])
    closed["setup_family"] = closed["entry_variant"].astype(str).replace({
        "early_price_action": "early_price_action",
        "retest_breakout": "retest_breakout",
        "manual": "manual",
    })
    closed["setup_family"] = closed["setup_family"].where(closed["setup_family"].isin(["early_price_action", "retest_breakout", "manual"]), closed["plan_type"].astype(str))
    return closed


def analyze_late_signal_patterns(closed_df: pd.DataFrame) -> Dict[str, object]:
    result: Dict[str, object] = {"summary": [], "late_rate_pct": 0.0, "table": pd.DataFrame()}
    if closed_df is None or closed_df.empty:
        result["summary"].append("Nog niet genoeg gesloten journal-data om late signalen te herkennen.")
        return result
    df = _ensure_advanced_journal_columns(closed_df.copy())
    total = len(df)
    late_mask = df["late_signal_flag"].astype(str).str.upper().eq("YES")
    zone_touch_mask = df["zone_touch_before_signal"].astype(str).str.upper().eq("YES")
    fill_missed_mask = df["fill_status"].astype(str).str.upper().eq("MISSED")
    late_count = int(late_mask.sum())
    result["late_rate_pct"] = _safe_pct(late_count, total)

    if late_count:
        result["summary"].append(f"{late_count}/{total} gesloten trades zijn gemarkeerd als late signalen ({result['late_rate_pct']:.1f}%).")
    if int(zone_touch_mask.sum()):
        result["summary"].append(f"Bij {int(zone_touch_mask.sum())} trades was de zone al geraakt vóór/zonder nette entry; Plan Mode moet die situaties eerder klaarzetten.")
    if int(fill_missed_mask.sum()):
        result["summary"].append(f"{int(fill_missed_mask.sum())} geplande fills zijn gemist; check ladder-afstand of entry-zone breedte.")

    group_cols = [col for col in ["coin", "scanner_tf", "setup_family", "context", "location_quality"] if col in df.columns]
    rows: List[Dict[str, object]] = []
    for col in group_cols:
        grouped = df.groupby(col, dropna=False).agg(
            trades=("journal_id", "count"),
            late_signals=("late_signal_flag", lambda s: int(s.astype(str).str.upper().eq("YES").sum())),
            zone_touched=("zone_touch_before_signal", lambda s: int(s.astype(str).str.upper().eq("YES").sum())),
            fills_missed=("fill_status", lambda s: int(s.astype(str).str.upper().eq("MISSED").sum())),
        ).reset_index()
        for _, row in grouped.iterrows():
            trades = int(row["trades"])
            if trades <= 0:
                continue
            rows.append({
                "groep": col,
                "waarde": str(row[col]),
                "trades": trades,
                "late_signals": int(row["late_signals"]),
                "late_rate_pct": _safe_pct(row["late_signals"], trades),
                "zone_touch_before_signal": int(row["zone_touched"]),
                "fills_missed": int(row["fills_missed"]),
            })
    result["table"] = pd.DataFrame(rows).sort_values(["late_rate_pct", "trades"], ascending=[False, False]) if rows else pd.DataFrame()
    return result


def analyze_tp_sl_efficiency(closed_df: pd.DataFrame) -> Dict[str, object]:
    result: Dict[str, object] = {"summary": [], "tp_miss_avg_pct": 0.0, "sl_too_tight_count": 0, "table": pd.DataFrame()}
    if closed_df is None or closed_df.empty:
        result["summary"].append("Nog niet genoeg gesloten journal-data voor TP/SL efficiency.")
        return result
    df = _ensure_advanced_journal_columns(closed_df.copy())
    df["tp_miss_pct"] = pd.to_numeric(df["tp_miss_pct"], errors="coerce")
    tp_miss = df["tp_miss_pct"].dropna()
    sl_tight = df["sl_too_tight_flag"].astype(str).str.upper().eq("YES")
    if not tp_miss.empty:
        result["tp_miss_avg_pct"] = round(float(tp_miss.mean()), 3)
        if float(tp_miss.mean()) > 0.0:
            result["summary"].append(f"Gemiddelde TP-miss is {float(tp_miss.mean()):.2f}%; TP staat mogelijk net te scherp/ver.")
    result["sl_too_tight_count"] = int(sl_tight.sum())
    if int(sl_tight.sum()):
        result["summary"].append(f"{int(sl_tight.sum())} trades hebben SL-too-tight vlag; geef invalidatie mogelijk meer ademruimte.")

    group_cols = [col for col in ["coin", "scanner_tf", "setup_family", "side"] if col in df.columns]
    rows: List[Dict[str, object]] = []
    for col in group_cols:
        grouped = df.groupby(col, dropna=False).agg(
            trades=("journal_id", "count"),
            avg_tp_miss_pct=("tp_miss_pct", "mean"),
            sl_too_tight=("sl_too_tight_flag", lambda s: int(s.astype(str).str.upper().eq("YES").sum())),
            tp_hits=("outcome", lambda s: int(s.astype(str).eq("TP").sum())),
            sl_hits=("outcome", lambda s: int(s.astype(str).eq("SL").sum())),
        ).reset_index()
        for _, row in grouped.iterrows():
            trades = int(row["trades"])
            rows.append({
                "groep": col,
                "waarde": str(row[col]),
                "trades": trades,
                "avg_tp_miss_pct": round(float(row["avg_tp_miss_pct"]), 3) if pd.notna(row["avg_tp_miss_pct"]) else None,
                "sl_too_tight": int(row["sl_too_tight"]),
                "tp_hits": int(row["tp_hits"]),
                "sl_hits": int(row["sl_hits"]),
            })
    result["table"] = pd.DataFrame(rows).sort_values(["sl_too_tight", "trades"], ascending=[False, False]) if rows else pd.DataFrame()
    return result


def build_plan_mode_feedback(closed_df: pd.DataFrame) -> Dict[str, object]:
    result: Dict[str, object] = {"summary": [], "preplaced_rate_pct": 0.0, "table": pd.DataFrame()}
    if closed_df is None or closed_df.empty:
        result["summary"].append("Nog niet genoeg data om Plan Mode te beoordelen.")
        return result
    df = _ensure_advanced_journal_columns(closed_df.copy())
    total = len(df)
    plan_active = df["plan_mode_active"].astype(str).str.upper().eq("YES")
    preplaced = df["plan_preplaced"].astype(str).str.upper().eq("YES")
    filled = df["fill_status"].astype(str).str.upper().isin(["FILLED", "PARTIAL"])
    missed = df["fill_status"].astype(str).str.upper().eq("MISSED")
    result["preplaced_rate_pct"] = _safe_pct(int(preplaced.sum()), total)

    if int(preplaced.sum()):
        result["summary"].append(f"Plan-orders waren bij {int(preplaced.sum())}/{total} trades vooraf geplaatst ({result['preplaced_rate_pct']:.1f}%).")
    if int(plan_active.sum()) and int(preplaced.sum()) == 0:
        result["summary"].append("Plan Mode was actief, maar weinig orders waren echt vooraf geplaatst; dit verklaart late entries.")
    if int(filled.sum()):
        result["summary"].append(f"{int(filled.sum())} plan/lader fills zijn geraakt; deze data is nuttig voor zonebreedte en ladderverdeling.")
    if int(missed.sum()):
        result["summary"].append(f"{int(missed.sum())} plan/lader fills zijn gemist; entry-zone of ladder staat mogelijk te scherp.")

    rows = []
    for col in [c for c in ["coin", "scanner_tf", "setup_family", "side"] if c in df.columns]:
        grouped = df.groupby(col, dropna=False).agg(
            trades=("journal_id", "count"),
            plan_active=("plan_mode_active", lambda s: int(s.astype(str).str.upper().eq("YES").sum())),
            preplaced=("plan_preplaced", lambda s: int(s.astype(str).str.upper().eq("YES").sum())),
            filled=("fill_status", lambda s: int(s.astype(str).str.upper().isin(["FILLED", "PARTIAL"]).sum())),
            missed=("fill_status", lambda s: int(s.astype(str).str.upper().eq("MISSED").sum())),
        ).reset_index()
        for _, row in grouped.iterrows():
            trades = int(row["trades"])
            rows.append({
                "groep": col,
                "waarde": str(row[col]),
                "trades": trades,
                "plan_active": int(row["plan_active"]),
                "preplaced": int(row["preplaced"]),
                "preplaced_rate_pct": _safe_pct(row["preplaced"], trades),
                "filled": int(row["filled"]),
                "missed": int(row["missed"]),
            })
    result["table"] = pd.DataFrame(rows).sort_values(["preplaced_rate_pct", "trades"], ascending=[True, False]) if rows else pd.DataFrame()
    return result


def build_learning_engine(journal_df: pd.DataFrame, daily_df: pd.DataFrame) -> Dict[str, object]:
    closed_df = get_closed_trade_journal(journal_df)
    result: Dict[str, object] = {
        "closed_df": closed_df,
        "coin_perf": summarize_group_performance(closed_df, "coin"),
        "timeframe_perf": summarize_group_performance(closed_df, "scanner_tf"),
        "setup_perf": summarize_group_performance(closed_df, "setup_family"),
        "side_perf": summarize_group_performance(closed_df, "side"),
        "context_perf": summarize_group_performance(closed_df, "context"),
        "location_perf": summarize_group_performance(closed_df, "location_quality"),
        "late_signal_analysis": analyze_late_signal_patterns(closed_df),
        "tp_sl_efficiency": analyze_tp_sl_efficiency(closed_df),
        "plan_mode_feedback": build_plan_mode_feedback(closed_df),
        "good_insights": [],
        "bad_insights": [],
        "top_working": [],
        "top_improve": [],
    }

    if closed_df.empty:
        return result

    insights_good: List[str] = []
    insights_bad: List[str] = []

    coin_perf = result["coin_perf"]
    timeframe_perf = result["timeframe_perf"]
    setup_perf = result["setup_perf"]
    side_perf = result["side_perf"]
    context_perf = result["context_perf"]
    location_perf = result["location_perf"]

    if not coin_perf.empty:
        top_coin = coin_perf.iloc[0]
        insights_good.append(f"{top_coin['coin']} werkt nu het best ({int(top_coin['trades'])} trades, {float(top_coin['winrate_pct']):.1f}% winrate).")
        weak_coin = coin_perf.sort_values(["winrate_pct", "trades"], ascending=[True, False]).iloc[0]
        if int(weak_coin["trades"]) >= 2:
            insights_bad.append(f"{weak_coin['coin']} presteert zwakker ({int(weak_coin['trades'])} trades, {float(weak_coin['winrate_pct']):.1f}% winrate).")

    if not timeframe_perf.empty:
        best_tf = timeframe_perf.iloc[0]
        insights_good.append(f"Timeframe {best_tf['scanner_tf']} werkt het best ({float(best_tf['winrate_pct']):.1f}% winrate).")
        weak_tf = timeframe_perf.sort_values(["winrate_pct", "trades"], ascending=[True, False]).iloc[0]
        if int(weak_tf["trades"]) >= 2:
            insights_bad.append(f"Timeframe {weak_tf['scanner_tf']} blijft achter; daar moet je scherper op entries/exits letten.")

    if not setup_perf.empty:
        best_setup = setup_perf.iloc[0]
        insights_good.append(f"{best_setup['setup_family']} is je sterkste setup-type ({float(best_setup['winrate_pct']):.1f}% winrate).")
        weak_setup = setup_perf.sort_values(["winrate_pct", "trades"], ascending=[True, False]).iloc[0]
        if int(weak_setup["trades"]) >= 2:
            insights_bad.append(f"{weak_setup['setup_family']} presteert zwakker; check of deze setup minder goed bij jouw stijl past.")

    if not side_perf.empty and len(side_perf) >= 2:
        best_side = side_perf.iloc[0]
        weak_side = side_perf.sort_values(["winrate_pct", "trades"], ascending=[True, False]).iloc[0]
        insights_good.append(f"{best_side['side']} trades doen het beter ({float(best_side['winrate_pct']):.1f}% winrate).")
        if best_side["side"] != weak_side["side"] and int(weak_side["trades"]) >= 2:
            insights_bad.append(f"{weak_side['side']} trades lopen achter; misschien past de markt daar minder goed bij of blokkeer je te weinig.")

    if not context_perf.empty:
        good_context = context_perf.iloc[0]
        insights_good.append(f"Context '{good_context['context']}' werkt relatief goed ({float(good_context['winrate_pct']):.1f}% winrate).")
        weak_context = context_perf.sort_values(["winrate_pct", "trades"], ascending=[True, False]).iloc[0]
        if int(weak_context["trades"]) >= 2:
            insights_bad.append(f"Context '{weak_context['context']}' geeft zwakkere resultaten; daar beter filteren.")

    if not location_perf.empty:
        good_loc = location_perf.iloc[0]
        insights_good.append(f"Location {good_loc['location_quality']} werkt het best ({float(good_loc['winrate_pct']):.1f}% winrate).")
        weak_loc = location_perf.sort_values(["winrate_pct", "trades"], ascending=[True, False]).iloc[0]
        if int(weak_loc["trades"]) >= 2:
            insights_bad.append(f"Location {weak_loc['location_quality']} presteert zwak; entries zijn daar waarschijnlijk minder scherp.")

    tp_sl_feedback = build_tp_sl_feedback(closed_df)
    insights_good.extend(tp_sl_feedback["good"])
    insights_bad.extend(tp_sl_feedback["bad"])

    late_analysis = result["late_signal_analysis"]
    plan_feedback = result["plan_mode_feedback"]
    tp_sl_efficiency = result["tp_sl_efficiency"]

    for text in late_analysis.get("summary", [])[:3]:
        insights_bad.append(text)
    for text in plan_feedback.get("summary", [])[:3]:
        if "gevuld" in text or "geraakt" in text:
            insights_good.append(text)
        else:
            insights_bad.append(text)
    for text in tp_sl_efficiency.get("summary", [])[:3]:
        insights_bad.append(text)

    if daily_df is not None and not daily_df.empty:
        daily_tmp = daily_df.copy()
        daily_tmp["pnl_pct"] = pd.to_numeric(daily_tmp["pnl_pct"], errors="coerce").fillna(0.0)
        daily_tmp["pnl_eur"] = pd.to_numeric(daily_tmp["pnl_eur"], errors="coerce").fillna(0.0)
        if float(daily_tmp["pnl_pct"].mean()) > 0:
            insights_good.append(f"Je gemiddelde dagresultaat staat positief ({daily_tmp['pnl_pct'].mean():.2f}% per entry).")
        if int((daily_tmp["result_type"].astype(str) == "NO_TRADE").sum()) > int(len(daily_tmp) * 0.4):
            insights_bad.append("Je hebt relatief veel no-trade dagen; misschien filter je te hard of wacht je te lang.")

    def _dedupe(items: List[str]) -> List[str]:
        seen = set()
        out = []
        for item in items:
            if item and item not in seen:
                seen.add(item)
                out.append(item)
        return out

    insights_good = _dedupe(insights_good)
    insights_bad = _dedupe(insights_bad)
    result["good_insights"] = insights_good[:8]
    result["bad_insights"] = insights_bad[:8]
    result["top_working"] = insights_good[:3]
    result["top_improve"] = insights_bad[:3]
    return result

# =========================================================
# Header
# =========================================================
st.markdown(
    """
    <div style='width:100%; text-align:center; padding: 6px 0 14px 0;'>
        <div style='font-size: 26px; line-height:1; margin-bottom: 2px;'>🐂</div>
        <div style='font-size: 34px; font-weight: 900; letter-spacing: 0.4px; line-height:1.0; margin-bottom: 6px;'>BullForge</div>
        <div style='font-size: 14px; color: #9CA3AF;'>Smarter Trading • Price Action Driven</div>
    </div>
    """,
    unsafe_allow_html=True
)
st.caption(
    f"📊 Scanner cache {SCANNER_CACHE_SEC} sec • Candle-data cache {REFRESH_ANALYSIS_SEC} sec"
)
st.caption("⚡ Rustige live prijzen actief • markt/trade-tabs blijven stabiel zonder dubbele blokken")

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

ZONE_BASE_WIDTH_PCT = {
    # v7.12.4: basis-zones kleiner gemaakt.
    # De oude waarden maakten vooral coins met weinig volatiliteit (XLM/XRP) veel te brede zones.
    "BTC": 0.12,
    "ETH": 0.15,
    "SOL": 0.22,
    "TAO": 0.34,
    "XRP": 0.16,
    "XLM": 0.14,
}

def get_coin_zone_width_pct(coin: str, vol_profile: Optional[Dict[str, float | str]] = None, zone_kind: str = "entry") -> float:
    """
    Adaptive zone-width.
    Belangrijk: lage volatiliteit moet smallere zones krijgen, niet grotere of vaste brede zones.
    De vorige minimum clamp van 0.10% + hoge coin-bases maakte intraday fills te vaag.
    """
    coin = str(coin).upper()
    base = float(ZONE_BASE_WIDTH_PCT.get(coin, 0.18))
    avg_range_pct = float((vol_profile or {}).get("avg_range_pct", 0.8) or 0.8)

    # Volatiliteit schaalt de zone: rustig = dunner, wild = iets breder.
    vol_factor = max(0.45, min(1.35, avg_range_pct / 1.15))
    width = base * vol_factor

    if zone_kind == "target":
        width *= 0.45
    elif zone_kind == "invalidation":
        width *= 0.55

    min_width = {"entry": 0.035, "target": 0.025, "invalidation": 0.030}.get(zone_kind, 0.035)
    max_width = {"entry": 0.55, "target": 0.30, "invalidation": 0.38}.get(zone_kind, 0.55)
    return round(max(min_width, min(max_width, width)), 3)

def build_price_zone(center: Optional[float], width_pct: float) -> Optional[Dict[str, float]]:
    if center is None:
        return None
    center = float(center)
    width_pct = max(0.0, float(width_pct))
    half = width_pct / 100.0
    return {
        "center": center,
        "low": center * (1 - half),
        "high": center * (1 + half),
        "width_pct": width_pct,
    }


def _build_zone_meta(
    zone: Optional[Dict[str, float]],
    zone_type: str,
    side: str,
    source_level: Optional[float] = None,
    source_timeframe: Optional[str] = None,
    role: str = "",
) -> Optional[Dict[str, object]]:
    """Maak van een gewone price-zone een rijkere trade-zone met metadata."""
    if zone is None:
        return None
    return {
        "zone_type": zone_type,
        "side": side.upper(),
        "role": role or zone_type,
        "center": float(zone["center"]),
        "low": float(zone["low"]),
        "high": float(zone["high"]),
        "width_pct": float(zone.get("width_pct", 0.0)),
        "source_level": float(source_level) if source_level is not None else float(zone["center"]),
        "source_timeframe": source_timeframe or "unknown",
    }


def split_entry_zone_into_ladder(
    zone: Optional[Dict[str, float]],
    side: str,
    steps: int = 3,
    weights: Optional[List[float]] = None,
) -> List[Dict[str, object]]:
    """
    Fase 4: splitst een entry-zone in top/mid/deep fills.

    LONG:
    - top fill  = eerste aanraking support, meeste kans op fill
    - mid fill  = midden van de zone
    - deep fill = beste prijs, maar grotere kans op geen fill

    SHORT is exact andersom:
    - top fill  = eerste aanraking resistance vanaf onderen
    - mid fill  = midden van de zone
    - deep fill = beste shortprijs hoger in de zone, maar grotere kans op geen fill
    """
    if zone is None or steps <= 0:
        return []

    low = float(zone["low"])
    high = float(zone["high"])
    center = float(zone.get("center", (low + high) / 2.0))
    if high <= low:
        return []

    if weights is None:
        weights = [40.0, 35.0, 25.0] if steps == 3 else [round(100.0 / steps, 2)] * steps
    if len(weights) != steps:
        weights = [round(100.0 / steps, 2)] * steps

    labels = ["top fill", "mid fill", "deep fill"] if steps == 3 else [f"fill {i+1}" for i in range(steps)]
    fill_types = ["top", "mid", "deep"] if steps == 3 else [f"fill_{i+1}" for i in range(steps)]
    descriptions_long = [
        "eerste aanraking support; hoogste fill-kans",
        "midden van de zone; balans tussen fill-kans en prijs",
        "diepe support-fill; beste prijs maar kans op geen fill",
    ]
    descriptions_short = [
        "eerste aanraking resistance; hoogste fill-kans",
        "midden van de zone; balans tussen fill-kans en prijs",
        "diepe resistance-fill; beste shortprijs maar kans op geen fill",
    ]
    probabilities = ["hoog", "gemiddeld", "lager"] if steps == 3 else ["gemiddeld"] * steps

    prices: List[float] = []
    if steps == 1:
        prices = [center]
    else:
        for i in range(steps):
            ratio = i / max(steps - 1, 1)
            if str(side).lower() == "long":
                price = high - (high - low) * ratio
            else:
                price = low + (high - low) * ratio
            prices.append(float(price))

    descriptions = descriptions_long if str(side).lower() == "long" else descriptions_short
    return [
        {
            "label": labels[i] if i < len(labels) else f"fill {i+1}",
            "fill_type": fill_types[i] if i < len(fill_types) else f"fill_{i+1}",
            "price": prices[i],
            "weight_pct": float(weights[i]),
            "side": side.upper(),
            "fill_probability": probabilities[i] if i < len(probabilities) else "gemiddeld",
            "description": descriptions[i] if i < len(descriptions) else "ladder fill",
            "order_type": "limit",
            "intent": "entry_fill",
        }
        for i in range(len(prices))
    ]
def build_target_zone_map(
    side: str,
    target_level: Optional[float],
    coin_symbol: str,
    vol_profile: Optional[Dict[str, float | str]] = None,
    source_timeframe: Optional[str] = None,
) -> Optional[Dict[str, object]]:
    width_pct = get_coin_zone_width_pct(coin_symbol, vol_profile, zone_kind="target")
    zone = build_price_zone(target_level, width_pct) if target_level is not None else None
    return _build_zone_meta(zone, "target", side, target_level, source_timeframe, role="take_profit")


def build_invalidation_zone_map(
    side: str,
    stop_level: Optional[float],
    coin_symbol: str,
    vol_profile: Optional[Dict[str, float | str]] = None,
    source_timeframe: Optional[str] = None,
) -> Optional[Dict[str, object]]:
    width_pct = get_coin_zone_width_pct(coin_symbol, vol_profile, zone_kind="invalidation")
    zone = build_price_zone(stop_level, width_pct) if stop_level is not None else None
    return _build_zone_meta(zone, "invalidation", side, stop_level, source_timeframe, role="stop_invalidatie")


def build_trade_zone_map(
    side: str,
    entry_level: Optional[float],
    target_level: Optional[float],
    stop_level: Optional[float],
    coin_symbol: str,
    vol_profile: Optional[Dict[str, float | str]] = None,
    entry_source_timeframe: Optional[str] = None,
    target_source_timeframe: Optional[str] = None,
    invalidation_source_timeframe: Optional[str] = None,
) -> Dict[str, object]:
    """
    Centrale zone-map voor één trade-plan.
    Hierdoor denkt de bot niet meer in één lijn, maar in entry/target/invalidation zones + ladder fills.
    """
    entry_width_pct = get_coin_zone_width_pct(coin_symbol, vol_profile, zone_kind="entry")
    entry_zone_raw = build_price_zone(entry_level, entry_width_pct) if entry_level is not None else None
    entry_zone = _build_zone_meta(
        entry_zone_raw,
        "entry",
        side,
        source_level=entry_level,
        source_timeframe=entry_source_timeframe,
        role="limit_entry_zone",
    )
    target_zone = build_target_zone_map(
        side=side,
        target_level=target_level,
        coin_symbol=coin_symbol,
        vol_profile=vol_profile,
        source_timeframe=target_source_timeframe,
    )
    invalidation_zone = build_invalidation_zone_map(
        side=side,
        stop_level=stop_level,
        coin_symbol=coin_symbol,
        vol_profile=vol_profile,
        source_timeframe=invalidation_source_timeframe,
    )
    ladder = split_entry_zone_into_ladder(entry_zone_raw, side=side, steps=3)
    limit_order_ladder = build_limit_order_ladder(entry_zone, side=side, steps=3)
    scale_out_plan = build_scale_out_plan(
        side=side,
        entry_price=entry_level,
        target_zone=target_zone,
        target_level=target_level,
    )
    compound_hint = build_portfolio_compound_plan_hint(None, scale_out_plan)

    return {
        "side": side.upper(),
        "entry_zone": entry_zone,
        "target_zone": target_zone,
        "invalidation_zone": invalidation_zone,
        "ladder": ladder,
        "limit_order_ladder": limit_order_ladder,
        "scale_out_plan": scale_out_plan,
        "compound_hint": compound_hint,
        "zone_center": entry_zone.get("center") if entry_zone else None,
        "zone_type": "trade_zone_map",
    }

def zone_edge(zone: Optional[Dict[str, float]], side: str, purpose: str) -> Optional[float]:
    if zone is None:
        return None
    if purpose == "entry":
        return zone["high"] if side.lower() == "long" else zone["low"]
    if purpose == "target":
        return zone["low"] if side.lower() == "long" else zone["high"]
    if purpose == "stop":
        return zone["low"] if side.lower() == "long" else zone["high"]
    return zone["center"]

def fmt_zone(zone: Optional[Dict[str, float]]) -> str:
    if zone is None:
        return "-"
    return f"{fmt_price_eur(zone['low'])} → {fmt_price_eur(zone['high'])}"

def distance_to_zone_pct(price: Optional[float], zone: Optional[Dict[str, float]]) -> Optional[float]:
    if price is None or zone is None or float(price) == 0:
        return None
    price = float(price)
    if zone["low"] <= price <= zone["high"]:
        return 0.0
    nearest = zone["high"] if price < zone["low"] else zone["low"]
    return abs(price - nearest) / price * 100.0


def calculate_range_progress_pct(
    current_price: Optional[float],
    support_zone: Optional[Dict[str, object]],
    resistance_zone: Optional[Dict[str, object]],
) -> Optional[float]:
    """Live positie van prijs tussen support en resistance, 0% = support, 100% = resistance."""
    if current_price is None:
        return None
    support_plain = _plain_zone(support_zone) if "_plain_zone" in globals() else support_zone
    resistance_plain = _plain_zone(resistance_zone) if "_plain_zone" in globals() else resistance_zone
    if not isinstance(support_plain, dict) or not isinstance(resistance_plain, dict):
        return None
    try:
        cp = float(current_price)
        support_center = float(support_plain.get("center", (float(support_plain["low"]) + float(support_plain["high"])) / 2.0))
        resistance_center = float(resistance_plain.get("center", (float(resistance_plain["low"]) + float(resistance_plain["high"])) / 2.0))
    except Exception:
        return None
    range_size = resistance_center - support_center
    if range_size <= 0:
        return None
    return round(max(0.0, min(100.0, ((cp - support_center) / range_size) * 100.0)), 1)


def recalculate_speelveld_for_live_price(speelveld: Optional[Dict[str, object]], current_price: Optional[float]) -> Dict[str, object]:
    """Werk alleen live prijs/marker/reden bij, zonder een volledige candle-scan te forceren."""
    if not isinstance(speelveld, dict):
        return {}
    updated = dict(speelveld)
    if current_price is None:
        return updated

    cp = float(current_price)
    support_zone = _plain_zone(updated.get("support_zone")) if "_plain_zone" in globals() else updated.get("support_zone")
    resistance_zone = _plain_zone(updated.get("resistance_zone")) if "_plain_zone" in globals() else updated.get("resistance_zone")
    updated["current_price"] = cp
    updated["nearest_support_distance_pct"] = distance_to_zone_pct(cp, support_zone) if support_zone else None
    updated["nearest_resistance_distance_pct"] = distance_to_zone_pct(cp, resistance_zone) if resistance_zone else None

    progress = calculate_range_progress_pct(cp, support_zone, resistance_zone)
    if progress is None:
        return updated
    updated["range_progress_pct"] = progress

    try:
        support_center = float(support_zone["center"])
        resistance_center = float(resistance_zone["center"])
        range_size = resistance_center - support_center
        updated["midrange_zone"] = {
            "low": support_center + range_size * 0.40,
            "high": support_center + range_size * 0.60,
            "center": support_center + range_size * 0.50,
            "width_pct": 0.0,
        }
        in_support = float(support_zone["low"]) <= cp <= float(support_zone["high"])
        in_resistance = float(resistance_zone["low"]) <= cp <= float(resistance_zone["high"])
    except Exception:
        return updated

    context = str(updated.get("context") or "range")
    if context in {"choppy", "hands_off", "compression"}:
        updated.update({"trade_allowed": False})
        return updated
    if in_support:
        updated.update({
            "position_label": "IN_SUPPORT_ZONE",
            "action_label": "Long-plan voorbereiden",
            "trade_allowed": True,
            "long_plan_allowed": True,
            "short_plan_allowed": False,
            "reason": "Prijs zit live in de koopzone. Timing/confirmatie komt pas daarna.",
        })
    elif in_resistance:
        updated.update({
            "position_label": "IN_RESISTANCE_ZONE",
            "action_label": "Short-plan voorbereiden",
            "trade_allowed": True,
            "long_plan_allowed": False,
            "short_plan_allowed": True,
            "reason": "Prijs zit live in de verkoopzone. Timing/confirmatie komt pas daarna.",
        })
    elif 40.0 <= progress <= 60.0:
        updated.update({
            "position_label": "NO_TRADE_MIDRANGE",
            "action_label": "Wachten",
            "trade_allowed": False,
            "reason": "Prijs zit live midden in de range. Geen voordeelzone; niet forceren.",
        })
    elif progress < 40.0:
        updated.update({
            "position_label": "NEAR_SUPPORT_SIDE",
            "action_label": "Wachten op support-zone",
            "trade_allowed": False,
            "long_plan_allowed": True,
            "short_plan_allowed": False,
            "reason": "Prijs zit live aan de goedkope kant van de range, maar nog niet netjes in de koopzone.",
        })
    else:
        updated.update({
            "position_label": "NEAR_RESISTANCE_SIDE",
            "action_label": "Wachten op resistance-zone",
            "trade_allowed": False,
            "long_plan_allowed": False,
            "short_plan_allowed": True,
            "reason": "Prijs zit live aan de dure kant van de range, maar nog niet netjes in de verkoopzone.",
        })
    return updated


def inject_live_price_into_selected_result(
    selected_result: Dict[str, object],
    live_price: Optional[float],
) -> Dict[str, object]:
    """Maak cached scanner-resultaten live voor UI-elementen zoals de range-balk."""
    result = dict(selected_result or {})
    if live_price is None:
        return result
    result["live_price"] = float(live_price)
    result["current_price"] = float(live_price)
    if isinstance(result.get("speelveld"), dict):
        result["speelveld"] = recalculate_speelveld_for_live_price(result.get("speelveld"), float(live_price))
        result["range_bar_live_updated"] = True
    return result


# =========================================================
# V3.3 - Speelveld Engine
# =========================================================
def _plain_zone(zone: Optional[Dict[str, object]]) -> Optional[Dict[str, float]]:
    """Normaliseert trade-zone/meta-zone naar low/high/center voor simpele UI-logica."""
    if not isinstance(zone, dict):
        return None
    try:
        return {
            "low": float(zone["low"]),
            "high": float(zone["high"]),
            "center": float(zone.get("center", (float(zone["low"]) + float(zone["high"])) / 2)),
            "width_pct": float(zone.get("width_pct", 0.0) or 0.0),
        }
    except Exception:
        return None


def classify_speelveld_context(context_engine: Optional[Dict[str, object]], trend_label: Optional[str] = None) -> str:
    """Vertaalt de bestaande context-engine naar één rustige speelveld-context."""
    ctx = context_engine or {}
    state = str(ctx.get("market_state") or "").lower()
    if bool(ctx.get("hands_off")):
        return "hands_off"
    if bool(ctx.get("choppy")) or state == "choppy":
        return "choppy"
    if state in {"bullish_trend", "bullish"}:
        return "bullish"
    if state in {"bearish_trend", "bearish"}:
        return "bearish"
    if state in {"compressie", "compression"}:
        return "compression"
    if state == "range" or bool(ctx.get("range_bound")):
        return "range"
    label = str(trend_label or "").lower()
    if "bull" in label:
        return "bullish"
    if "bear" in label:
        return "bearish"
    return "range"


def build_speelveld_engine(
    current_price: Optional[float],
    support_level: Optional[float],
    resistance_level: Optional[float],
    coin_symbol: str,
    timeframe_label: str,
    vol_profile: Optional[Dict[str, float | str]],
    context_engine: Optional[Dict[str, object]],
    trend_label: Optional[str] = None,
    support_meta: Optional[Dict[str, object]] = None,
    resistance_meta: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    """
    Plan-first speelveld: eerst chart logisch maken, daarna pas trade/timing.
    """
    width_pct = get_coin_zone_width_pct(coin_symbol, vol_profile, zone_kind="entry")
    if is_daytrade_timeframe(timeframe_label):
        width_pct = round(max(0.025, width_pct * 0.65), 3)

    support_zone = _plain_zone(build_price_zone(support_level, width_pct) if support_level is not None else None)
    resistance_zone = _plain_zone(build_price_zone(resistance_level, width_pct) if resistance_level is not None else None)
    context = classify_speelveld_context(context_engine, trend_label)
    cp = float(current_price) if current_price is not None else None

    result: Dict[str, object] = {
        "version": "V3.3",
        "active": True,
        "current_price": cp,
        "support_zone": support_zone,
        "resistance_zone": resistance_zone,
        "support_meta": support_meta or {},
        "resistance_meta": resistance_meta or {},
        "midrange_zone": None,
        "range_progress_pct": None,
        "position_label": "geen_data",
        "action_label": "Geen data",
        "context": context,
        "trade_allowed": False,
        "long_plan_allowed": False,
        "short_plan_allowed": False,
        "reason": "Onvoldoende data om speelveld te bouwen.",
        "nearest_support_distance_pct": distance_to_zone_pct(cp, support_zone) if cp is not None else None,
        "nearest_resistance_distance_pct": distance_to_zone_pct(cp, resistance_zone) if cp is not None else None,
    }

    if cp is None:
        return result
    if context in {"choppy", "hands_off", "compression"}:
        result.update({
            "position_label": "CHOPPY_SKIP" if context == "choppy" else ("HANDS_OFF" if context == "hands_off" else "COMPRESSION_WAIT"),
            "action_label": "Overslaan" if context == "choppy" else "Wachten",
            "reason": "Marktcontext is niet schoon genoeg voor een nieuw plan.",
            "trade_allowed": False,
        })
        return result
    if support_zone is None or resistance_zone is None:
        result.update({
            "position_label": "NO_LEVELS",
            "action_label": "Wachten",
            "reason": "Support of resistance ontbreekt; speelveld is nog niet compleet.",
        })
        return result

    support_center = float(support_zone["center"])
    resistance_center = float(resistance_zone["center"])
    range_size = resistance_center - support_center
    if range_size <= 0:
        result.update({
            "position_label": "INVALID_RANGE",
            "action_label": "Wachten",
            "reason": "Support/resistance liggen niet logisch om de prijs heen.",
        })
        return result

    progress = max(0.0, min(100.0, ((cp - support_center) / range_size) * 100.0))
    result["range_progress_pct"] = round(progress, 1)
    mid_low = support_center + range_size * 0.40
    mid_high = support_center + range_size * 0.60
    result["midrange_zone"] = {"low": mid_low, "high": mid_high, "center": (mid_low + mid_high) / 2, "width_pct": 0.0}

    in_support = support_zone["low"] <= cp <= support_zone["high"]
    in_resistance = resistance_zone["low"] <= cp <= resistance_zone["high"]

    if in_support:
        result.update({
            "position_label": "IN_SUPPORT_ZONE",
            "action_label": "Long-plan voorbereiden",
            "trade_allowed": True,
            "long_plan_allowed": True,
            "reason": "Prijs zit in de koopzone. Timing/confirmatie komt pas daarna.",
        })
    elif in_resistance:
        result.update({
            "position_label": "IN_RESISTANCE_ZONE",
            "action_label": "Short-plan voorbereiden",
            "trade_allowed": True,
            "short_plan_allowed": True,
            "reason": "Prijs zit in de verkoopzone. Timing/confirmatie komt pas daarna.",
        })
    elif 40.0 <= progress <= 60.0:
        result.update({
            "position_label": "NO_TRADE_MIDRANGE",
            "action_label": "Wachten",
            "trade_allowed": False,
            "reason": "Prijs zit midden in de range. Geen voordeelzone; niet forceren.",
        })
    elif progress < 40.0:
        result.update({
            "position_label": "NEAR_SUPPORT_SIDE",
            "action_label": "Wachten op support-zone",
            "long_plan_allowed": True,
            "reason": "Prijs zit aan de goedkope kant van de range, maar nog niet netjes in de koopzone.",
        })
    else:
        result.update({
            "position_label": "NEAR_RESISTANCE_SIDE",
            "action_label": "Wachten op resistance-zone",
            "short_plan_allowed": True,
            "reason": "Prijs zit aan de dure kant van de range, maar nog niet netjes in de verkoopzone.",
        })
    return result


# =========================================================
# V4.4 - Range positie balk zonder duplicatie/fragment-error
# =========================================================
def _range_bar_text(progress: float, side: str) -> str:
    """Korte menselijke uitleg bij de range-balk."""
    side_u = str(side or "LONG").upper()
    if side_u == "SHORT":
        if progress >= 75:
            return "Prijs zit dichtbij resistance: goede kant voor een mogelijke short."
        if progress <= 25:
            return "Prijs zit dichtbij support: minder gunstig voor short; oppassen met najagen."
        return "Prijs zit midden in de range: liever wachten op een duidelijke rand."

    if progress <= 25:
        return "Prijs zit dichtbij support: goede kant voor een mogelijke long."
    if progress >= 75:
        return "Prijs zit dichtbij resistance: minder gunstig voor long; oppassen met najagen."
    if progress < 40:
        return "Prijs zit aan de goedkope kant van de range, maar nog niet netjes in de koopzone."
    if progress > 60:
        return "Prijs zit aan de dure kant van de range; liever wachten op betere locatie."
    return "Prijs zit midden in de range: liever wachten op support of resistance."


def render_range_position_bar(selected_result: Dict[str, object], side: Optional[str] = None) -> None:
    """
    Visuele 0-100% range-balk:
    - LONG: links groen = dichtbij support, midden geel, rechts rood = dichtbij resistance.
    - SHORT: links rood = dichtbij support, midden geel, rechts groen = dichtbij resistance.
    """
    speelveld = selected_result.get("speelveld") or {}
    progress = speelveld.get("range_progress_pct")

    side_u = str(side or selected_result.get("best_side") or selected_result.get("primary_side") or selected_result.get("context_preferred_side") or "LONG").upper()
    if side_u not in {"LONG", "SHORT"}:
        side_u = "LONG"

    # Fallback voor 30m/15m/5m/1m: gebruik actieve entry/target-zones als speelveld-progress ontbreekt.
    # LONG: support = entry, resistance = target. SHORT: support = target, resistance = entry.
    if progress is None:
        current_price = selected_result.get("live_price") or selected_result.get("current_price")
        if side_u == "SHORT":
            support_zone = (selected_result.get("short_target_zone") or speelveld.get("support_zone"))
            resistance_zone = (selected_result.get("short_entry_zone") or speelveld.get("resistance_zone"))
        else:
            support_zone = (selected_result.get("long_entry_zone") or speelveld.get("support_zone"))
            resistance_zone = (selected_result.get("long_target_zone") or speelveld.get("resistance_zone"))
        progress = calculate_range_progress_pct(current_price, support_zone, resistance_zone)

    if progress is None:
        return

    try:
        progress_f = max(0.0, min(100.0, float(progress)))
    except Exception:
        return

    if side_u == "SHORT":
        gradient = "linear-gradient(90deg, #ef4444 0%, #facc15 50%, #22c55e 100%)"
        left_color = "#fca5a5"
        mid_color = "#facc15"
        right_color = "#4ade80"
        title_pct_color = "#4ade80" if progress_f >= 65 else ("#facc15" if progress_f >= 35 else "#f87171")
    else:
        gradient = "linear-gradient(90deg, #22c55e 0%, #facc15 50%, #ef4444 100%)"
        left_color = "#4ade80"
        mid_color = "#facc15"
        right_color = "#f87171"
        title_pct_color = "#4ade80" if progress_f <= 35 else ("#facc15" if progress_f <= 65 else "#f87171")

    reason = _range_bar_text(progress_f, side_u)
    marker_left = max(0.0, min(100.0, progress_f))

    st.markdown(
        f"""
        <div class="bf-range-card">
            <div class="bf-range-head">
                <div class="bf-range-title">Range positie ({side_u})</div>
                <div class="bf-range-pct" style="color:{title_pct_color};">{progress_f:.1f}% van support naar resistance</div>
            </div>
            <div class="bf-range-labels">
                <span style="color:{left_color};">🛡️ Dichtbij support</span>
                <span style="color:{mid_color};">◎ In range</span>
                <span style="color:{right_color};">🛡️ Dichtbij resistance</span>
            </div>
            <div class="bf-range-track" style="background:{gradient};">
                <div class="bf-range-marker" style="left:{marker_left:.2f}%;"></div>
            </div>
            <div class="bf-range-scale">
                <span>0%</span>
                <span>100%</span>
            </div>
            <div class="bf-range-reason">{reason}</div>
        </div>
        <style>
            .bf-range-card {{
                width: 100%;
                border: 1px solid rgba(148, 163, 184, 0.22);
                border-radius: 14px;
                padding: 18px 20px 14px 20px;
                margin: 12px 0 16px 0;
                background: radial-gradient(circle at 50% 50%, rgba(250,204,21,0.08), transparent 32%), rgba(17, 24, 39, 0.58);
                box-shadow: 0 8px 26px rgba(0,0,0,0.18);
            }}
            .bf-range-head {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 12px;
                margin-bottom: 14px;
                font-weight: 800;
            }}
            .bf-range-title {{ color: #F8FAFC; font-size: 1.02rem; }}
            .bf-range-pct {{ font-size: 1.00rem; font-weight: 900; text-align: right; }}
            .bf-range-labels {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 10px;
                margin-bottom: 12px;
                font-size: 0.88rem;
                font-weight: 700;
            }}
            .bf-range-track {{
                position: relative;
                height: 16px;
                border-radius: 999px;
                box-shadow: 0 0 18px rgba(250,204,21,0.18);
            }}
            .bf-range-marker {{
                position: absolute;
                top: 50%;
                width: 22px;
                height: 22px;
                transform: translate(-50%, -50%);
                border-radius: 999px;
                background: #E0F2FE;
                border: 4px solid rgba(255,255,255,0.95);
                box-shadow: 0 0 0 2px rgba(15,23,42,0.75), 0 0 18px rgba(255,255,255,0.45);
            }}
            .bf-range-scale {{
                display: flex;
                justify-content: space-between;
                color: #CBD5E1;
                font-size: 0.82rem;
                font-weight: 800;
                margin-top: 10px;
            }}
            .bf-range-reason {{ color: #AAB4C3; font-size: 0.86rem; margin-top: 10px; }}
            @media (max-width: 760px) {{
                .bf-range-head, .bf-range-labels {{ flex-direction: column; align-items: flex-start; }}
                .bf-range-pct {{ text-align: left; }}
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_speelveld_panel(selected_result: Dict[str, object], compact: bool = False) -> None:
    """Rustige V3.3 UI: eerst chart-speelveld, niet direct een trade-signaal."""
    speelveld = selected_result.get("speelveld") or {}
    if not speelveld:
        st.info("Speelveld Engine: nog geen data beschikbaar.")
        return

    if compact:
        st.markdown("**🧭 Speelveld Engine V3.3**")
    else:
        st.subheader("🧭 Speelveld Engine V3.3")

    status = str(speelveld.get("position_label") or "-")
    action = str(speelveld.get("action_label") or "-")
    reason = str(speelveld.get("reason") or "")
    context = str(speelveld.get("context") or "-")

    c1, c2, c3, c4 = st.columns(4)
    price = speelveld.get("current_price")
    c1.metric("Huidige prijs", fmt_price_eur(float(price)) if price is not None else "-")
    c2.metric("Context", context)
    c3.metric("Locatie", status)
    c4.metric("Actie", action)

    z1, z2, z3 = st.columns(3)
    z1.write(f"**Koopzone / support**  \n{fmt_zone(speelveld.get('support_zone'))}")
    z2.write(f"**No-trade / midrange**  \n{fmt_zone(speelveld.get('midrange_zone'))}")
    z3.write(f"**Verkoopzone / resistance**  \n{fmt_zone(speelveld.get('resistance_zone'))}")

    render_range_position_bar(selected_result)
    if reason:
        st.caption(reason)



# =========================================================
# V3.4 - Plan Mode Standaard
# =========================================================
def _zone_entry_price(zone: Optional[Dict[str, object]], side: str) -> Optional[float]:
    """Conservatieve plan-entry aan de eerste raak-kant van de zone."""
    z = _plain_zone(zone)
    if not z:
        return None
    return float(z["high"] if str(side).upper() == "LONG" else z["low"])


def _zone_target_price(zone: Optional[Dict[str, object]], side: str) -> Optional[float]:
    z = _plain_zone(zone)
    if not z:
        return None
    return float(z["low"] if str(side).upper() == "LONG" else z["high"])


def _zone_stop_price(zone: Optional[Dict[str, object]], side: str, coin_symbol: str, vol_profile: Optional[Dict[str, float | str]]) -> Optional[float]:
    z = _plain_zone(zone)
    if not z:
        return None
    buffer_pct = max(get_coin_zone_width_pct(coin_symbol, vol_profile, "invalidation"), float(z.get("width_pct", 0.0) or 0.0) * 0.75)
    if str(side).upper() == "LONG":
        return float(z["low"] * (1 - buffer_pct / 100.0))
    return float(z["high"] * (1 + buffer_pct / 100.0))


def detect_standard_plan_status_from_speelveld(speelveld: Optional[Dict[str, object]], side: str) -> Tuple[str, str, str]:
    """PLAN bestaat altijd zodra het speelveld klopt; READY alleen in de entry-zone."""
    if not isinstance(speelveld, dict):
        return "WAIT", "Wachten", "Geen speelveld beschikbaar."
    position = str(speelveld.get("position_label") or "")
    context = str(speelveld.get("context") or "")
    side_u = str(side).upper()

    if context in {"choppy", "hands_off", "compression"} or position in {"CHOPPY_SKIP", "HANDS_OFF", "COMPRESSION_WAIT"}:
        return "HANDS_OFF", "Overslaan", "Markt is niet schoon genoeg; geen nieuw plan forceren."
    if side_u == "LONG" and position == "IN_SUPPORT_ZONE":
        return "READY", "Entry-zone actief", "Prijs zit in de vooraf bepaalde koopzone; timing/confirmatie mag nu meedoen."
    if side_u == "SHORT" and position == "IN_RESISTANCE_ZONE":
        return "READY", "Entry-zone actief", "Prijs zit in de vooraf bepaalde verkoopzone; timing/confirmatie mag nu meedoen."
    if position == "NO_TRADE_MIDRANGE":
        return "PLAN", "Plan klaarzetten / wachten", "Prijs zit midden in de range; plan staat klaar maar entry is nog niet actief."
    if side_u == "LONG":
        return "PLAN", "Wacht op koopzone", "Long-plan staat vooraf klaar bij support; nog niet najagen."
    return "PLAN", "Wacht op verkoopzone", "Short-plan staat vooraf klaar bij resistance; nog niet najagen."


def build_standard_plan_candidate_from_speelveld(
    speelveld: Optional[Dict[str, object]],
    side: str,
    coin_symbol: str,
    timeframe_label: str,
    current_price: Optional[float],
    vol_profile: Optional[Dict[str, float | str]],
    account_size: float,
    max_risk_pct: float,
    entry_fee_pct: float,
    exit_fee_pct: float,
    taker_fee_pct: float,
    short_borrow_hourly_pct: float = 0.0,
    expected_hold_hours: float = 0.0,
    short_liquidation_fee_pct: float = DEFAULT_SHORT_LIQUIDATION_FEE_PCT,
    allowed_by_context: bool = True,
    context_reason: str = "",
) -> Optional[Dict[str, object]]:
    """
    V3.4: bouw ALTIJD een vooraf-plan vanuit het speelveld.
    Geen candle-confirmation nodig. Confirmatie is later alleen timing/score.
    """
    if not isinstance(speelveld, dict):
        return None

    side_u = str(side).upper()
    support_zone = _plain_zone(speelveld.get("support_zone"))
    resistance_zone = _plain_zone(speelveld.get("resistance_zone"))
    if not support_zone or not resistance_zone:
        return None

    entry_zone = support_zone if side_u == "LONG" else resistance_zone
    target_zone = resistance_zone if side_u == "LONG" else support_zone
    entry = _zone_entry_price(entry_zone, side_u)
    target = _zone_target_price(target_zone, side_u)
    stop = _zone_stop_price(entry_zone, side_u, coin_symbol, vol_profile)

    metrics = calculate_trade_metrics(
        side=side_u.lower(),
        entry=entry,
        stop=stop,
        target=target,
        account_size=account_size,
        max_risk_pct=max_risk_pct,
        coin_symbol=coin_symbol,
        entry_fee_pct=entry_fee_pct,
        exit_fee_pct=exit_fee_pct,
        short_borrow_hourly_pct=short_borrow_hourly_pct if side_u == "SHORT" else 0.0,
        expected_hold_hours=expected_hold_hours if side_u == "SHORT" else 0.0,
        short_liquidation_fee_pct=short_liquidation_fee_pct,
    )
    if metrics is None:
        return None

    status, action, reason = detect_standard_plan_status_from_speelveld(speelveld, side_u)
    if not allowed_by_context and status != "READY":
        status = "BLOCKED"
        action = "Geblokkeerd"
        reason = context_reason or "Context blokkeert dit plan."

    distance_pct = distance_to_zone_pct(current_price, entry_zone) if current_price is not None else None
    base_score = 45.0
    if status == "READY":
        base_score += 35.0
    elif status == "PLAN":
        base_score += 18.0
    if distance_pct is not None:
        base_score -= min(float(distance_pct) * 2.0, 20.0)
    if not allowed_by_context:
        base_score -= 80.0

    entry_meta = _build_zone_meta(entry_zone, "entry", side_u, entry, timeframe_label, role="plan_first_entry_zone")
    invalidation_raw = build_price_zone(stop, get_coin_zone_width_pct(coin_symbol, vol_profile, "invalidation"))
    invalidation_meta = _build_zone_meta(invalidation_raw, "invalidation", side_u, stop, timeframe_label, role="onder/boven_entry_zone")
    ladder = build_limit_order_ladder(
        entry_meta,
        side_u,
        steps=3,
        target=target,
        stop=stop,
        account_size=account_size,
        max_risk_pct=max_risk_pct,
        coin_symbol=coin_symbol,
        entry_fee_pct=entry_fee_pct,
        exit_fee_pct=exit_fee_pct,
        short_borrow_hourly_pct=short_borrow_hourly_pct,
        expected_hold_hours=expected_hold_hours,
        short_liquidation_fee_pct=short_liquidation_fee_pct,
    )

    return {
        "side": side_u,
        "mode": "plan",
        "variant": "speelveld_plan",
        "setup_family": "speelveld_plan",
        "setup_label": "Plan-first speelveld",
        "setup_detection": {"confirmed": False, "reason": "Geen confirmatie nodig om plan te tonen."},
        "status": status,
        "timing_label": status,
        "reason": reason,
        "score": round(float(base_score), 2),
        "metrics": metrics,
        "target": target,
        "location_quality": "A_ENTRY" if status == "READY" else "PLAN_ZONE",
        "allowed_by_context": bool(allowed_by_context) or status == "READY",
        "conservative_net": calculate_conservative_net_profit(metrics, taker_fee_pct),
        "pre_trade_plan": {
            "active": status in {"PLAN", "READY"},
            "side": side_u,
            "action": action,
            "entry_zone": entry_zone,
            "target_zone": target_zone,
            "invalidation_zone": invalidation_meta,
            "status": status,
            "reason": reason,
            "distance_to_entry_pct": distance_pct,
            "distance_to_target_pct": distance_to_zone_pct(current_price, target_zone) if current_price is not None else None,
            "limit_order_ladder": ladder,
        },
        "plan_mode_standard": True,
        "plan_created_before_confirmation": True,
        "planner_rank_score": round(float(base_score), 2),
    }


def add_standard_plan_mode_candidates(
    existing_candidates: List[Dict[str, object]],
    speelveld: Optional[Dict[str, object]],
    coin_symbol: str,
    timeframe_label: str,
    current_price: Optional[float],
    vol_profile: Optional[Dict[str, float | str]],
    account_size: float,
    max_risk_pct: float,
    entry_fee_pct: float,
    exit_fee_pct: float,
    taker_fee_pct: float,
    context_allow_long: bool,
    context_allow_short: bool,
    context_long_reason: str,
    context_short_reason: str,
    short_borrow_hourly_pct: float = 0.0,
    expected_hold_hours: float = 0.0,
    short_liquidation_fee_pct: float = DEFAULT_SHORT_LIQUIDATION_FEE_PCT,
) -> List[Dict[str, object]]:
    """Voegt LONG/SHORT plan-first candidates toe als standaardlaag."""
    out = list(existing_candidates or [])
    for side, allowed, reason in [
        ("LONG", context_allow_long, context_long_reason),
        ("SHORT", context_allow_short, context_short_reason),
    ]:
        candidate = build_standard_plan_candidate_from_speelveld(
            speelveld=speelveld,
            side=side,
            coin_symbol=coin_symbol,
            timeframe_label=timeframe_label,
            current_price=current_price,
            vol_profile=vol_profile,
            account_size=account_size,
            max_risk_pct=max_risk_pct,
            entry_fee_pct=entry_fee_pct,
            exit_fee_pct=exit_fee_pct,
            taker_fee_pct=taker_fee_pct,
            short_borrow_hourly_pct=short_borrow_hourly_pct,
            expected_hold_hours=expected_hold_hours,
            short_liquidation_fee_pct=short_liquidation_fee_pct,
            allowed_by_context=allowed,
            context_reason=reason,
        )
        if candidate is not None:
            out.append(candidate)
    return sorted(out, key=lambda x: (bool(x.get("allowed_by_context", False)), _status_rank(x.get("status")), float(x.get("score", 0.0) or 0.0)), reverse=True)

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

    entry_zone = build_price_zone(zone, get_coin_zone_width_pct(coin_symbol, None, zone_kind="entry"))
    target_zone = build_price_zone(tgt, get_coin_zone_width_pct(coin_symbol, None, zone_kind="target"))
    invalidation_zone = build_price_zone(hard, get_coin_zone_width_pct(coin_symbol, None, zone_kind="invalidation"))

    if side == "long":
        entry = zone_edge(entry_zone, "long", "entry")
        stop = zone_edge(invalidation_zone, "long", "stop")
        tgt = zone_edge(target_zone, "long", "target")
        distance_to_entry_pct = distance_to_zone_pct(cp, entry_zone)
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
        entry = zone_edge(entry_zone, "short", "entry")
        stop = zone_edge(invalidation_zone, "short", "stop")
        tgt = zone_edge(target_zone, "short", "target")
        distance_to_entry_pct = distance_to_zone_pct(cp, entry_zone)
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
        result["reason"] = "Pure prijsactie-opzet: entry in de structuurzone en winstdoel in de eerstvolgende target-zone, zonder bevestiging af te wachten."

    result["valid"] = valid
    result["status"] = "DOOPIECASH_READY" if valid else "WAIT"
    return result

def compute_setup_timing(
    current_price: Optional[float],
    support: Optional[float],
    resistance: Optional[float],
    vol_profile: Dict[str, float | str],
    structure_bias: str = "neutral",
    coin_symbol: Optional[str] = None,
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

    zone_width_pct = get_zone_width_pct(vol_profile, coin=coin_symbol, zone_kind="entry")
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






def _normalize_timing_label(label: Optional[str]) -> str:
    label = str(label or "geen data")
    if label in {"READY", "NEAR", "BLOCKED", "HANDS OFF"}:
        return label
    return "WAIT"


def apply_context_to_timing(
    raw_timing: Dict[str, Optional[float] | str],
    context_engine: Dict[str, object],
) -> Dict[str, Optional[float] | str]:
    """
    Stap 4D:
    maak timinglabels context-aware zonder de rest van de bot omver te trekken.
    De context-engine bepaalt hier welke labels logisch zijn:
    - bullish trend -> LONG READY/NEAR/WAIT, SHORT BLOCKED
    - bearish trend -> SHORT READY/NEAR/WAIT, LONG BLOCKED
    - range-mid -> geen READY
    - compressie -> WAIT op breakout/retest
    - hands off / impuls -> HANDS OFF
    """
    result = dict(raw_timing or {})

    raw_long = _normalize_timing_label(result.get("long_timing"))
    raw_short = _normalize_timing_label(result.get("short_timing"))
    market_state = str((context_engine or {}).get("market_state", "range"))
    sub_state = str((context_engine or {}).get("sub_state", "neutral"))
    hands_off = bool((context_engine or {}).get("hands_off", False))
    impulse_active = bool((context_engine or {}).get("impulse_active", False))

    long_reason = ""
    short_reason = ""

    if hands_off or impulse_active or market_state == "hands_off":
        result["long_timing"] = "HANDS OFF"
        result["short_timing"] = "HANDS OFF"
        long_reason = "Impuls bezig / hands off"
        short_reason = "Impuls bezig / hands off"
    elif market_state == "bullish_trend":
        result["long_timing"] = raw_long if raw_long in {"READY", "NEAR"} else "WAIT"
        result["short_timing"] = "BLOCKED"
        long_reason = "Bullish trend: LONG alleen actief bij support/pullback"
        short_reason = "Bullish trend blokkeert SHORT hoofdsetup"
    elif market_state == "bearish_trend":
        result["long_timing"] = "BLOCKED"
        result["short_timing"] = raw_short if raw_short in {"READY", "NEAR"} else "WAIT"
        long_reason = "Bearish trend blokkeert LONG hoofdsetup"
        short_reason = "Bearish trend: SHORT alleen actief bij resistance/pullback"
    elif market_state == "range":
        if sub_state == "range_low":
            result["long_timing"] = raw_long if raw_long in {"READY", "NEAR"} else "WAIT"
            result["short_timing"] = "BLOCKED"
            long_reason = "Range low: LONG aan onderkant van de range"
            short_reason = "Range low blokkeert SHORT"
        elif sub_state == "range_high":
            result["long_timing"] = "BLOCKED"
            result["short_timing"] = raw_short if raw_short in {"READY", "NEAR"} else "WAIT"
            long_reason = "Range high blokkeert LONG"
            short_reason = "Range high: SHORT aan bovenkant van de range"
        else:
            result["long_timing"] = "WAIT"
            result["short_timing"] = "WAIT"
            long_reason = "Mid-range: liever wachten op een rand van de range"
            short_reason = "Mid-range: liever wachten op een rand van de range"
    elif market_state == "compressie":
        result["long_timing"] = "WAIT"
        result["short_timing"] = "WAIT"
        long_reason = "Compressie: wacht op breakout/retest"
        short_reason = "Compressie: wacht op breakout/retest"
    elif market_state == "choppy":
        result["long_timing"] = "BLOCKED"
        result["short_timing"] = "BLOCKED"
        long_reason = "Choppy markt: liever overslaan"
        short_reason = "Choppy markt: liever overslaan"
    else:
        result["long_timing"] = raw_long
        result["short_timing"] = raw_short
        long_reason = "Timing volgt zones"
        short_reason = "Timing volgt zones"

    result["long_timing_reason"] = long_reason
    result["short_timing_reason"] = short_reason
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

    zone_ref = build_price_zone(zone, 0.30)
    target_ref = build_price_zone(tgt, 0.18)

    if side.lower() == "long":
        zone_anchor = zone_ref["high"] if zone_ref else zone
        target_anchor = target_ref["low"] if target_ref else tgt
        denom = target_anchor - zone_anchor
        if denom <= 0:
            return result
        range_progress = (cp - zone_anchor) / denom
        zone_distance_pct = distance_to_zone_pct(cp, zone_ref)
        target_distance_pct = distance_to_zone_pct(cp, target_ref)
    else:
        zone_anchor = zone_ref["low"] if zone_ref else zone
        target_anchor = target_ref["high"] if target_ref else tgt
        denom = zone_anchor - target_anchor
        if denom <= 0:
            return result
        range_progress = (zone_anchor - cp) / denom
        zone_distance_pct = distance_to_zone_pct(cp, zone_ref)
        target_distance_pct = distance_to_zone_pct(cp, target_ref)

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
        "distance_to_zone_pct": round(zone_distance_pct, 4) if zone_distance_pct is not None else None,
        "distance_to_target_pct": round(target_distance_pct, 4) if target_distance_pct is not None else None,
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
    """
    Cleaned:
    - only 2 real entry families remain:
      1) doopiecash = early price-action
      2) confirmation = retest-breakout
    - old legacy limit/balanced routing removed from active logic
    """
    long_trigger = compute_reclaim_trigger(df, "long")
    short_trigger = compute_reclaim_trigger(df, "short")

    confirm_buffer_pct = max(0.03, entry_buffer_pct * 0.35)
    confirmation_long = (long_trigger * (1 + confirm_buffer_pct / 100)) if long_trigger is not None else support
    confirmation_short = (short_trigger * (1 - confirm_buffer_pct / 100)) if short_trigger is not None else resistance

    if entry_mode == "confirmation":
        entry_long = max(current_price, confirmation_long)
        entry_short = min(current_price, confirmation_short)
    else:
        # default = early price-action
        entry_long = support
        entry_short = resistance

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
        "XLM": {"max_risk_pct": 0.85, "entry_buffer_pct": 0.22, "stop_buffer_pct": 0.45, "rr_target": 2.0},
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
def get_zone_width_pct(vol_profile: Dict[str, float | str], coin: Optional[str] = None, zone_kind: str = "entry") -> float:
    if coin is not None:
        return get_coin_zone_width_pct(coin, vol_profile, zone_kind=zone_kind)
    avg_range_pct = float((vol_profile or {}).get("avg_range_pct", 0.8) or 0.8)
    width = avg_range_pct * 0.16
    if zone_kind == "target":
        width *= 0.45
    elif zone_kind == "invalidation":
        width *= 0.55
    return round(max(0.035, min(0.55, width)), 3)


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
        "account_dependent_metrics": True,
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
MIN_DISTANCE_TO_TARGET_PCT = 0.12
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
    # Setup-validatie is bewust losgekoppeld van account size.
    # Accountgrootte bepaalt nog wel positieomvang / €-uitkomst, maar niet of een setup technisch goed is.
    if metrics is None:
        return False, "Geen metrics"

    rr = float(metrics.get("rr", 0.0))

    if rr < min_rr:
        return False, f"RR te laag ({rr:.2f} < {min_rr:.2f})"

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
    # Setup-validatie puur op trade-kwaliteit, niet op accountgrootte of €-winst.
    if metrics is None:
        return False

    rr = float(metrics.get("rr", 0.0))
    reward_pct_price = float(metrics.get("reward_pct_price", 0.0))

    # Minimale technische kwaliteit:
    # - voldoende RR
    # - target niet extreem dicht op entry
    # Voor lower timeframe scalps mag target dichterbij liggen; fees blijven via metrics zichtbaar.
    return rr > 0.75 and reward_pct_price >= 0.06


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

    # V3.6 / Fase 5: confirmatie is NIET meer de poortwachter.
    # Het plan is technisch geldig als RR/target/fees kloppen. Candle-confirmatie verhoogt alleen score/confidence.
    technical_valid = is_setup_valid(metrics, min_profit_buffer_eur, taker_fee_pct)
    confirmed_now = bool(confirmation_info.get("confirmed", False))

    ready_reason = str(confirmation_info.get("reason", "Prijsactie bevestigt de move."))
    wait_reason = "Technisch plan staat klaar; prijsactie-confirmatie is extra confluence, geen blokkade."
    if metrics is None:
        wait_reason = "Confirmed plan haalt netto eisen nog niet."

    result.update({
        "status": "CONFIRMED_READY" if technical_valid and confirmed_now else ("PLAN_READY" if technical_valid else "WAIT"),
        "reason": ready_reason if confirmed_now else wait_reason,
        "entry": entry,
        "stop": stop,
        "target": float(target),
        "metrics": metrics,
        "valid": technical_valid,
        "trigger": trigger_value,
        "confirmation": confirmation_info,
        "confirmation_as_bonus": True,
        "confirmation_score": 100.0 if confirmed_now else 0.0,
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



def filter_noise_levels(
    levels: Optional[List[float]],
    reference_price: Optional[float],
    min_distance_pct: float = 0.18,
    merge_threshold_pct: float = 0.14,
    max_levels: int = 4,
) -> List[float]:
    """
    Houd alleen logische niveaus over:
    - weg met doublures / bijna-doublures
    - weg met mini-levels te dicht op de huidige prijs
    - beperkt aantal niveaus per kant
    """
    if not levels:
        return []

    cleaned: List[float] = []
    numeric = sorted([float(lvl) for lvl in levels if lvl is not None])

    for lvl in numeric:
        if reference_price is not None and float(reference_price) != 0:
            dist_pct = abs(float(lvl) - float(reference_price)) / float(reference_price) * 100.0
            if dist_pct < float(min_distance_pct):
                continue

        if cleaned:
            ref = cleaned[-1]
            pct_diff = abs(float(lvl) - float(ref)) / max(abs(float(ref)), 1e-9) * 100.0
            if pct_diff <= float(merge_threshold_pct):
                cleaned[-1] = (cleaned[-1] + float(lvl)) / 2.0
                continue

        cleaned.append(float(lvl))

    if reference_price is None:
        return cleaned[:max_levels]

    below = sorted([lvl for lvl in cleaned if lvl < float(reference_price)], reverse=True)
    above = sorted([lvl for lvl in cleaned if lvl > float(reference_price)])
    return (below[:max_levels] + above[:max_levels])


def extract_higher_timeframe_levels(
    authority_packages: Dict[str, Dict[str, object]],
    reference_price: Optional[float],
    allowed_timeframes: Optional[List[str]] = None,
) -> Dict[str, Dict[str, List[float]]]:
    """
    Haal alleen de level-bronnen op die mogen meepraten:
    - 1d / 4h = hoofdlevels
    - 1h / 15m = refinement
    - 1m / 5m worden hier bewust nooit level-authority
    """
    extracted: Dict[str, Dict[str, List[float]]] = {}
    timeframes = allowed_timeframes or ["15m", "1h", "4h", "1d"]

    for tf in timeframes:
        pkg = authority_packages.get(tf) or {}
        levels = pkg.get("levels") or {}
        extracted[tf] = {
            "trade_supports": [lvl for lvl in filter_noise_levels(levels.get("trade_supports"), reference_price) if reference_price is None or lvl < float(reference_price)],
            "trade_resistances": [lvl for lvl in filter_noise_levels(levels.get("trade_resistances"), reference_price) if reference_price is None or lvl > float(reference_price)],
            "hard_supports": [lvl for lvl in filter_noise_levels(levels.get("hard_supports"), reference_price) if reference_price is None or lvl < float(reference_price)],
            "hard_resistances": [lvl for lvl in filter_noise_levels(levels.get("hard_resistances"), reference_price) if reference_price is None or lvl > float(reference_price)],
        }

    return extracted


def weight_levels_by_timeframe(
    extracted_levels: Dict[str, Dict[str, List[float]]],
    reference_price: Optional[float],
) -> Dict[str, List[Dict[str, object]]]:
    """
    Geef 1d/4h de meeste autoriteit.
    1h/15m mogen verfijnen, maar niet overheersen.
    """
    timeframe_weights = {
        "1d": 4.2,
        "4h": 3.3,
        "1h": 2.0,
        "15m": 1.55,
        "30m": 1.2,
        "5m": 0.55,
        "1m": 0.35,
    }
    kind_bonus = {
        "trade_supports": 0.85,
        "trade_resistances": 0.85,
        "hard_supports": 1.00,
        "hard_resistances": 1.00,
    }

    weighted: Dict[str, List[Dict[str, object]]] = {
        "trade_supports": [],
        "trade_resistances": [],
        "hard_supports": [],
        "hard_resistances": [],
    }

    for tf, level_map in (extracted_levels or {}).items():
        tf_weight = float(timeframe_weights.get(tf, 1.0))
        for kind, levels in (level_map or {}).items():
            for level in levels or []:
                dist_pct = None
                if reference_price is not None and float(reference_price) != 0:
                    dist_pct = abs(float(level) - float(reference_price)) / float(reference_price) * 100.0

                score = tf_weight * 10.0 + float(kind_bonus.get(kind, 0.0)) * 2.5

                if dist_pct is not None:
                    if dist_pct < 0.22:
                        score -= 8.5
                    elif dist_pct < 0.45:
                        score -= 3.0
                    elif dist_pct > 10.0:
                        score -= 1.5

                weighted[kind].append({
                    "level": float(level),
                    "timeframe": tf,
                    "kind": kind,
                    "distance_pct": round(float(dist_pct), 4) if dist_pct is not None else None,
                    "score": round(float(score), 4),
                    "weight": tf_weight,
                })

    for kind in weighted:
        weighted[kind] = sorted(
            weighted[kind],
            key=lambda x: (float(x.get("score", 0.0)), float(x.get("weight", 0.0))),
            reverse=True,
        )

    return weighted


def select_primary_trade_zones(
    weighted_levels: Dict[str, List[Dict[str, object]]],
    reference_price: Optional[float],
    refinement_timeframes: Optional[List[str]] = None,
    base_timeframe_label: Optional[str] = None,
) -> Dict[str, object]:
    """
    Kies eerst HTF ankers uit 1d/4h.
    Laat 1h/15m daarna de trade-zone verfijnen voor daytrading,
    zonder dat 1m/5m ooit hoofdlevels worden.
    """
    refinement_timeframes = refinement_timeframes or ["1h"]

    daytrade_mode = str(base_timeframe_label) in {"1m", "5m", "15m", "30m"}
    one_hour_mode = str(base_timeframe_label) == "1h"
    if daytrade_mode:
        anchor_timeframes = {"15m", "1h"}
        backup_anchor_timeframes = {"4h"}
    elif one_hour_mode:
        # 1h trades mogen niet wachten op te verre 1d-zones.
        # 1h = actieve trade-zones, 4h = context/backup.
        anchor_timeframes = {"1h", "4h"}
        backup_anchor_timeframes = {"4h"}
    else:
        anchor_timeframes = {"1d", "4h"}
        backup_anchor_timeframes = {"1d", "4h"}

    def _pick_anchor(kind: str, side: str) -> Optional[Dict[str, object]]:
        candidates = []
        for item in weighted_levels.get(kind, []):
            tf = str(item.get("timeframe"))
            if tf not in anchor_timeframes:
                continue
            lvl = float(item["level"])
            if reference_price is not None:
                if side == "below" and lvl >= float(reference_price):
                    continue
                if side == "above" and lvl <= float(reference_price):
                    continue
            candidates.append((float(item.get("score", 0.0)), item))
        if not candidates:
            return None
        if str(base_timeframe_label) in {"1m", "5m", "15m", "30m", "1h"} and reference_price is not None:
            candidates.sort(key=lambda x: (float(x[1].get("distance_pct") or 999.0), -float(x[1].get("score", 0.0))))
            return candidates[0][1]
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    def _pick_backup(kind: str, chosen: Optional[Dict[str, object]], side: str) -> Optional[Dict[str, object]]:
        chosen_level = None if chosen is None else float(chosen["level"])
        for item in weighted_levels.get(kind, []):
            tf = str(item.get("timeframe"))
            if tf not in backup_anchor_timeframes:
                continue
            lvl = float(item["level"])
            if reference_price is not None:
                if side == "below" and lvl >= float(reference_price):
                    continue
                if side == "above" and lvl <= float(reference_price):
                    continue
            if chosen_level is not None:
                diff_pct = abs(lvl - chosen_level) / max(abs(chosen_level), 1e-9) * 100.0
                if diff_pct < 0.18:
                    continue
            return item
        return None

    def _pick_refined(kind: str, side: str, anchor: Optional[Dict[str, object]]) -> Optional[Dict[str, object]]:
        if reference_price is None:
            return None

        anchor_level = None if anchor is None else float(anchor["level"])
        candidates = []
        for item in weighted_levels.get(kind, []):
            tf = str(item.get("timeframe"))
            if tf not in refinement_timeframes:
                continue
            lvl = float(item["level"])

            if side == "below" and lvl >= float(reference_price):
                continue
            if side == "above" and lvl <= float(reference_price):
                continue

            dist_pct = abs(lvl - float(reference_price)) / max(abs(float(reference_price)), 1e-9) * 100.0

            if anchor_level is not None:
                anchor_gap_pct = abs(lvl - anchor_level) / max(abs(anchor_level), 1e-9) * 100.0
                # refinement moet in de buurt van het HTF-anker blijven
                max_anchor_gap_pct = 3.5
                if base_timeframe_label in {"1m", "5m", "15m", "30m"}:
                    max_anchor_gap_pct = 2.4
                elif base_timeframe_label == "1h":
                    max_anchor_gap_pct = 7.0
                if anchor_gap_pct > max_anchor_gap_pct:
                    continue
                # voor support liever niet ruim onder het HTF-anker
                if side == "below" and lvl < anchor_level * 0.992:
                    continue
                # voor resistance liever niet ruim boven het HTF-anker
                if side == "above" and lvl > anchor_level * 1.008:
                    continue
            else:
                anchor_gap_pct = 0.0

            score = float(item.get("score", 0.0))
            score += max(0.0, 7.0 - dist_pct * 3.0)  # intraday iets dichter bij prijs = fijner

            if tf == "15m":
                score += 1.5
                if base_timeframe_label in {"1m", "5m", "15m"}:
                    score += 2.0
            elif tf == "1h":
                score += 1.0
                if base_timeframe_label in {"5m", "15m", "30m"}:
                    score += 0.6

            if base_timeframe_label in {"1m", "5m", "15m", "30m"} and dist_pct > 2.2:
                score -= 2.5

            score -= anchor_gap_pct * 0.8

            candidates.append((score, item))

        if not candidates:
            return None

        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    hard_support = _pick_anchor("hard_supports", "below") or _pick_anchor("trade_supports", "below")
    hard_resistance = _pick_anchor("hard_resistances", "above") or _pick_anchor("trade_resistances", "above")

    # Alleen als intraday geen bruikbaar 15m/1h level heeft, mag 4h als noodanker dienen.
    if daytrade_mode and (hard_support is None or hard_resistance is None):
        old_anchor_timeframes = anchor_timeframes
        anchor_timeframes = {"4h"}
        hard_support = hard_support or _pick_anchor("hard_supports", "below") or _pick_anchor("trade_supports", "below")
        hard_resistance = hard_resistance or _pick_anchor("hard_resistances", "above") or _pick_anchor("trade_resistances", "above")
        anchor_timeframes = old_anchor_timeframes

    trade_support_anchor = _pick_anchor("trade_supports", "below") or hard_support
    trade_resistance_anchor = _pick_anchor("trade_resistances", "above") or hard_resistance

    refined_trade_support = _pick_refined("trade_supports", "below", trade_support_anchor)
    refined_trade_resistance = _pick_refined("trade_resistances", "above", trade_resistance_anchor)

    trade_support = refined_trade_support or trade_support_anchor or hard_support
    trade_resistance = refined_trade_resistance or trade_resistance_anchor or hard_resistance

    backup_trade_support = _pick_backup("trade_supports", trade_support_anchor, "below")
    backup_trade_resistance = _pick_backup("trade_resistances", trade_resistance_anchor, "above")

    return {
        "trade_support": None if trade_support is None else float(trade_support["level"]),
        "trade_resistance": None if trade_resistance is None else float(trade_resistance["level"]),
        "hard_support": None if hard_support is None else float(hard_support["level"]),
        "hard_resistance": None if hard_resistance is None else float(hard_resistance["level"]),
        "backup_trade_support": None if backup_trade_support is None else float(backup_trade_support["level"]),
        "backup_trade_resistance": None if backup_trade_resistance is None else float(backup_trade_resistance["level"]),
        "trade_support_meta": trade_support,
        "trade_resistance_meta": trade_resistance,
        "hard_support_meta": hard_support,
        "hard_resistance_meta": hard_resistance,
        "backup_trade_support_meta": backup_trade_support,
        "backup_trade_resistance_meta": backup_trade_resistance,
        "trade_support_anchor_meta": trade_support_anchor,
        "trade_resistance_anchor_meta": trade_resistance_anchor,
        "refined_trade_support_meta": refined_trade_support,
        "refined_trade_resistance_meta": refined_trade_resistance,
        "refinement_timeframes": list(refinement_timeframes),
    }



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


def select_target_level_doopiecash(
    side: str,
    reference_price: Optional[float],
    local_trade_level: Optional[float],
    higher_trade_level: Optional[float],
    min_distance_pct: float = MIN_DISTANCE_TO_TARGET_PCT,
) -> Optional[float]:
    """
    DoopieCash = pak eerstvolgende logische level.
    Eerst entry timeframe level, alleen fallback naar higher timeframe
    als local level ontbreekt of te dicht is.
    """
    if reference_price is None:
        return None

    local = float(local_trade_level) if local_trade_level is not None else None
    higher = float(higher_trade_level) if higher_trade_level is not None else None

    def far_enough(level: Optional[float]) -> bool:
        if level is None:
            return False
        dist = abs(level - float(reference_price)) / float(reference_price) * 100 if float(reference_price) else 0.0
        return dist >= min_distance_pct

    if side == "long":
        if local is not None and local > float(reference_price) and far_enough(local):
            return local
        if higher is not None and higher > float(reference_price) and far_enough(higher):
            return higher
    else:
        if local is not None and local < float(reference_price) and far_enough(local):
            return local
        if higher is not None and higher < float(reference_price) and far_enough(higher):
            return higher

    return None

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




def _calc_directional_efficiency(df: Optional[pd.DataFrame], window: int = 20) -> Dict[str, float]:
    if df is None or len(df) < 6:
        return {"efficiency": 0.0, "up_ratio": 0.5, "body_ratio": 1.0}

    recent = df.tail(min(len(df), window)).copy()
    recent["body"] = (recent["close"] - recent["open"]).abs()
    net_move = abs(float(recent["close"].iloc[-1]) - float(recent["close"].iloc[0]))
    gross_move = float((recent["close"].diff().abs().sum()) or 0.0)
    efficiency = (net_move / gross_move) if gross_move > 0 else 0.0

    bullish_bodies = int((recent["close"] > recent["open"]).sum())
    up_ratio = bullish_bodies / max(len(recent), 1)

    avg_body = float(recent["body"].mean()) if len(recent) else 0.0
    avg_range = float((recent["high"] - recent["low"]).mean()) if len(recent) else 0.0
    body_ratio = (avg_body / avg_range) if avg_range > 0 else 0.0

    return {
        "efficiency": round(float(efficiency), 4),
        "up_ratio": round(float(up_ratio), 4),
        "body_ratio": round(float(body_ratio), 4),
    }


def _calc_compression_state(df: Optional[pd.DataFrame], window: int = 24) -> Dict[str, object]:
    result: Dict[str, object] = {
        "is_compression": False,
        "compression_score": 0.0,
        "range_now_pct": None,
        "range_prev_pct": None,
    }
    if df is None or len(df) < max(12, window):
        return result

    recent = df.tail(window).copy()
    half = max(6, window // 2)
    prev = recent.head(half)
    now = recent.tail(half)

    prev_high = float(prev["high"].max())
    prev_low = float(prev["low"].min())
    now_high = float(now["high"].max())
    now_low = float(now["low"].min())

    prev_mid = max(abs(float(prev["close"].mean())), 1e-9)
    now_mid = max(abs(float(now["close"].mean())), 1e-9)

    prev_range_pct = ((prev_high - prev_low) / prev_mid) * 100
    now_range_pct = ((now_high - now_low) / now_mid) * 100

    lower_high = now_high <= prev_high * 1.002
    higher_low = now_low >= prev_low * 0.998
    contraction_ratio = (now_range_pct / prev_range_pct) if prev_range_pct > 0 else 1.0

    compression_score = 0.0
    if lower_high:
        compression_score += 0.5
    if higher_low:
        compression_score += 0.5
    if contraction_ratio < 0.8:
        compression_score += 0.7
    elif contraction_ratio < 0.92:
        compression_score += 0.35

    result.update({
        "is_compression": compression_score >= 1.0,
        "compression_score": round(float(compression_score), 3),
        "range_now_pct": round(float(now_range_pct), 3),
        "range_prev_pct": round(float(prev_range_pct), 3),
    })
    return result


def _calc_impulse_state(
    trigger_df: Optional[pd.DataFrame],
    setup_df: Optional[pd.DataFrame],
    trigger_structure: Dict[str, object],
    setup_structure: Dict[str, object],
    current_price: Optional[float],
    support: Optional[float],
    resistance: Optional[float],
) -> Dict[str, object]:
    result: Dict[str, object] = {
        "hands_off": False,
        "impulse_active": False,
        "impulse_side": "none",
        "reason": "",
    }

    active_df = trigger_df if trigger_df is not None and len(trigger_df) >= 6 else setup_df
    active_structure = trigger_structure if active_df is trigger_df else setup_structure

    if active_df is None or len(active_df) < 6:
        return result

    recent = active_df.tail(min(len(active_df), 20)).copy()
    recent["body"] = (recent["close"] - recent["open"]).abs()
    avg_body = float(recent["body"].iloc[:-1].mean()) if len(recent) > 1 else 0.0
    last = recent.iloc[-1]
    last_body = float(abs(last["close"] - last["open"]))
    body_ratio = (last_body / avg_body) if avg_body > 0 else 1.0

    displacement_bullish = bool(active_structure.get("displacement_bullish", False))
    displacement_bearish = bool(active_structure.get("displacement_bearish", False))

    far_from_support = False
    far_from_resistance = False
    if current_price is not None and support is not None and current_price != 0:
        far_from_support = ((float(current_price) - float(support)) / float(current_price) * 100) > 0.8
    if current_price is not None and resistance is not None and current_price != 0:
        far_from_resistance = ((float(resistance) - float(current_price)) / float(current_price) * 100) > 0.8

    if displacement_bullish and body_ratio >= 1.8 and far_from_support:
        result.update({
            "hands_off": True,
            "impulse_active": True,
            "impulse_side": "bullish",
            "reason": "Bullish impuls bezig; prijs is al hard uit support vertrokken.",
        })
    elif displacement_bearish and body_ratio >= 1.8 and far_from_resistance:
        result.update({
            "hands_off": True,
            "impulse_active": True,
            "impulse_side": "bearish",
            "reason": "Bearish impuls bezig; prijs is al hard uit resistance vertrokken.",
        })

    return result



# =========================================================
# Phase 4 - Leading context engine wrappers
# =========================================================
def derive_market_phase(
    trigger_df: Optional[pd.DataFrame],
    setup_df: Optional[pd.DataFrame],
    trend_df: Optional[pd.DataFrame],
    trigger_structure: Dict[str, object],
    setup_structure: Dict[str, object],
    trend_structure: Dict[str, object],
    current_price: Optional[float],
    support: Optional[float],
    resistance: Optional[float],
    trigger_vol_profile: Optional[Dict[str, float | str]] = None,
    setup_vol_profile: Optional[Dict[str, float | str]] = None,
    trend_vol_profile: Optional[Dict[str, float | str]] = None,
) -> Dict[str, object]:
    """Fase 4: bepaal eerst de markt-fase voordat entries/plannen gekozen worden."""
    phase = classify_market_context_engine(
        trigger_df=trigger_df,
        setup_df=setup_df,
        trend_df=trend_df,
        trigger_structure=trigger_structure,
        setup_structure=setup_structure,
        trend_structure=trend_structure,
        trigger_vol_profile=trigger_vol_profile,
        setup_vol_profile=setup_vol_profile,
        trend_vol_profile=trend_vol_profile,
        current_price=current_price,
        support=support,
        resistance=resistance,
        setup_freshness={"penalty_score": 0.0},
    )
    phase["engine_version"] = "v7_phase4_leading_context"
    return phase


def derive_context_priority(context_engine: Dict[str, object]) -> Dict[str, object]:
    """Geef de context een duidelijke prioriteit voor sorting/gating."""
    market_state = str((context_engine or {}).get("market_state", "range"))
    sub_state = str((context_engine or {}).get("sub_state", "neutral"))
    hands_off = bool((context_engine or {}).get("hands_off", False))
    impulse_active = bool((context_engine or {}).get("impulse_active", False))

    if hands_off or impulse_active or market_state == "hands_off":
        return {"priority": 0, "risk_state": "no_trade", "action_bias": "hands_off", "reason": "Impuls/hands off heeft hoogste blokkade-prioriteit."}
    if market_state == "choppy":
        return {"priority": 1, "risk_state": "no_trade", "action_bias": "skip", "reason": "Choppy markt: geen duidelijke edge."}
    if market_state == "compressie":
        return {"priority": 2, "risk_state": "wait", "action_bias": "wait_breakout", "reason": "Compressie: eerst breakout/retest afwachten."}
    if market_state == "range" and sub_state == "mid_range":
        return {"priority": 3, "risk_state": "wait", "action_bias": "wait_range_edge", "reason": "Midden in range: wachten op range high/low."}
    if market_state == "range" and sub_state in {"range_low", "range_high"}:
        return {"priority": 6, "risk_state": "selective", "action_bias": sub_state, "reason": "Range-rand is tradebaar, maar alleen richting de overkant."}
    if market_state in {"bullish_trend", "bearish_trend"}:
        return {"priority": 7, "risk_state": "tradeable", "action_bias": market_state, "reason": "Trendcontext is leidend; alleen in trendrichting plannen."}
    return {"priority": 4, "risk_state": "neutral", "action_bias": "wait", "reason": "Context is nog niet sterk genoeg; voorzichtig."}


def derive_trade_permissions(
    context_engine: Dict[str, object],
    current_price: Optional[float],
    support: Optional[float],
    resistance: Optional[float],
    long_timing_label: str = "WAIT",
    short_timing_label: str = "WAIT",
) -> Dict[str, object]:
    """Context bepaalt permissies. Timing mag daarna alleen nog extra blokkeren."""
    permissions = apply_context_trade_permissions(
        context_engine=context_engine,
        current_price=current_price,
        support=support,
        resistance=resistance,
        long_timing_label=long_timing_label,
        short_timing_label=short_timing_label,
    )
    priority = derive_context_priority(context_engine)
    permissions.update({
        "context_priority": priority.get("priority"),
        "context_risk_state": priority.get("risk_state"),
        "context_action_bias": priority.get("action_bias"),
        "context_priority_reason": priority.get("reason"),
    })
    return permissions


def build_market_context_engine(
    trigger_df: Optional[pd.DataFrame],
    setup_df: Optional[pd.DataFrame],
    trend_df: Optional[pd.DataFrame],
    trigger_structure: Dict[str, object],
    setup_structure: Dict[str, object],
    trend_structure: Dict[str, object],
    current_price: Optional[float],
    support: Optional[float],
    resistance: Optional[float],
    trigger_vol_profile: Optional[Dict[str, float | str]] = None,
    setup_vol_profile: Optional[Dict[str, float | str]] = None,
    trend_vol_profile: Optional[Dict[str, float | str]] = None,
) -> Dict[str, object]:
    """Centrale context-engine voor fase 4."""
    engine = derive_market_phase(
        trigger_df=trigger_df,
        setup_df=setup_df,
        trend_df=trend_df,
        trigger_structure=trigger_structure,
        setup_structure=setup_structure,
        trend_structure=trend_structure,
        current_price=current_price,
        support=support,
        resistance=resistance,
        trigger_vol_profile=trigger_vol_profile,
        setup_vol_profile=setup_vol_profile,
        trend_vol_profile=trend_vol_profile,
    )
    priority = derive_context_priority(engine)
    engine.update({
        "context_priority": priority.get("priority"),
        "context_risk_state": priority.get("risk_state"),
        "context_action_bias": priority.get("action_bias"),
        "context_priority_reason": priority.get("reason"),
    })
    return engine

def classify_market_context_engine(
    trigger_df: Optional[pd.DataFrame],
    setup_df: Optional[pd.DataFrame],
    trend_df: Optional[pd.DataFrame],
    trigger_structure: Dict[str, object],
    setup_structure: Dict[str, object],
    trend_structure: Dict[str, object],
    trigger_vol_profile: Optional[Dict[str, float | str]] = None,
    setup_vol_profile: Optional[Dict[str, float | str]] = None,
    trend_vol_profile: Optional[Dict[str, float | str]] = None,
    current_price: Optional[float] = None,
    support: Optional[float] = None,
    resistance: Optional[float] = None,
    setup_freshness: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    """
    Nieuwe centrale context-classificatie.
    Nog geen trade-gating: alleen context bepalen en opslaan.
    """
    result: Dict[str, object] = {
        "market_state": "range",
        "sub_state": "neutral",
        "label_main": "Range",
        "label_sub": "Neutral",
        "reason": "",
        "hands_off": False,
        "impulse_active": False,
        "impulse_side": "none",
        "compression_active": False,
        "choppy": False,
        "range_bound": False,
        "directional_efficiency": 0.0,
        "compression_score": 0.0,
        "freshness_penalty": 0.0,
        "trigger_efficiency": 0.0,
        "trend_efficiency": 0.0,
    }

    freshness = setup_freshness or {}
    freshness_penalty = float(freshness.get("penalty_score", 0.0) or 0.0)
    result["freshness_penalty"] = freshness_penalty

    impulse_state = _calc_impulse_state(
        trigger_df=trigger_df,
        setup_df=setup_df,
        trigger_structure=trigger_structure,
        setup_structure=setup_structure,
        current_price=current_price,
        support=support,
        resistance=resistance,
    )
    result.update({
        "hands_off": bool(impulse_state.get("hands_off", False)),
        "impulse_active": bool(impulse_state.get("impulse_active", False)),
        "impulse_side": str(impulse_state.get("impulse_side", "none")),
    })

    if result["hands_off"]:
        result.update({
            "market_state": "hands_off",
            "sub_state": "impuls_bezig",
            "label_main": "Hands off",
            "label_sub": "Impuls bezig",
            "reason": str(impulse_state.get("reason", "Move is al bezig; liever niet chasen.")),
        })
        return result

    setup_eff = _calc_directional_efficiency(setup_df, window=20)
    trigger_eff = _calc_directional_efficiency(trigger_df, window=12)
    trend_eff = _calc_directional_efficiency(trend_df, window=24)

    directional_efficiency = float(setup_eff["efficiency"])
    result["directional_efficiency"] = directional_efficiency
    result["trigger_efficiency"] = float(trigger_eff["efficiency"])
    result["trend_efficiency"] = float(trend_eff["efficiency"])

    compression = _calc_compression_state(setup_df, window=24)
    result["compression_active"] = bool(compression.get("is_compression", False))
    result["compression_score"] = float(compression.get("compression_score", 0.0) or 0.0)

    trend_market = str(trend_structure.get("market_structure", "unknown"))
    setup_market = str(setup_structure.get("market_structure", "unknown"))
    trigger_market = str(trigger_structure.get("market_structure", "unknown"))

    bullish_states = {"bullish", "mixed_bullish", "developing_bullish"}
    bearish_states = {"bearish", "mixed_bearish", "developing_bearish"}

    is_range = False
    if current_price is not None and support is not None and resistance is not None and resistance > support:
        range_width_pct = ((float(resistance) - float(support)) / max(abs(float(current_price)), 1e-9)) * 100
        if 0.35 <= range_width_pct <= 4.5 and setup_market in {"mixed", "unknown"}:
            is_range = True

    choppy = False
    if setup_market in {"mixed", "unknown"} and compression.get("is_compression") is False:
        if directional_efficiency < 0.26 and 0.35 < float(trigger_eff["up_ratio"]) < 0.65 and float(trigger_eff["body_ratio"]) < 0.52:
            choppy = True
    result["choppy"] = choppy
    result["range_bound"] = is_range

    if compression.get("is_compression") and setup_market in {"mixed", "unknown", "developing_bullish", "developing_bearish"}:
        result.update({
            "market_state": "compressie",
            "sub_state": "breakout_loading",
            "label_main": "Compressie",
            "label_sub": "Breakout loading",
            "reason": "Prijs trekt samen; wacht liever op expansion of retest van de uitbraak.",
        })
        return result

    if (trend_market in bullish_states and setup_market in bullish_states) or (
        trend_market in bullish_states and setup_market == "mixed_bullish"
    ):
        result.update({
            "market_state": "bullish_trend",
            "sub_state": "pullback" if trigger_market in bearish_states or trigger_market == "mixed_bearish" else "continuation",
            "label_main": "Bullish trend",
            "label_sub": "Pullback" if trigger_market in bearish_states or trigger_market == "mixed_bearish" else "Continuation",
            "reason": "Trend TF en setup TF wijzen overwegend omhoog.",
        })
        return result

    if (trend_market in bearish_states and setup_market in bearish_states) or (
        trend_market in bearish_states and setup_market == "mixed_bearish"
    ):
        result.update({
            "market_state": "bearish_trend",
            "sub_state": "pullback" if trigger_market in bullish_states or trigger_market == "mixed_bullish" else "continuation",
            "label_main": "Bearish trend",
            "label_sub": "Pullback" if trigger_market in bullish_states or trigger_market == "mixed_bullish" else "Continuation",
            "reason": "Trend TF en setup TF wijzen overwegend omlaag.",
        })
        return result

    if is_range:
        mid_bias = "range_low" if current_price is not None and support is not None and resistance is not None and float(current_price) <= (float(support) + (float(resistance) - float(support)) * 0.35) else (
            "range_high" if current_price is not None and support is not None and resistance is not None and float(current_price) >= (float(support) + (float(resistance) - float(support)) * 0.65) else "mid_range"
        )
        sub_label_map = {
            "range_low": "Range low",
            "range_high": "Range high",
            "mid_range": "Mid range",
        }
        result.update({
            "market_state": "range",
            "sub_state": mid_bias,
            "label_main": "Range",
            "label_sub": sub_label_map[mid_bias],
            "reason": "Geen duidelijke trend; prijs beweegt tussen duidelijke support- en resistancezones.",
        })
        return result

    if choppy:
        result.update({
            "market_state": "choppy",
            "sub_state": "noisy_overlap",
            "label_main": "Choppy",
            "label_sub": "Veel overlap",
            "reason": "Veel overlap, weinig follow-through en geen nette structuur.",
        })
        return result

    if trend_market in bullish_states:
        result.update({
            "market_state": "bullish_trend",
            "sub_state": "pullback",
            "label_main": "Bullish trend",
            "label_sub": "Pullback",
            "reason": "Grotere trend blijft bullish, ook al is de setup nog niet perfect schoon.",
        })
    elif trend_market in bearish_states:
        result.update({
            "market_state": "bearish_trend",
            "sub_state": "pullback",
            "label_main": "Bearish trend",
            "label_sub": "Pullback",
            "reason": "Grotere trend blijft bearish, ook al is de setup nog niet perfect schoon.",
        })
    else:
        result.update({
            "market_state": "range",
            "sub_state": "neutral",
            "label_main": "Range",
            "label_sub": "Neutral",
            "reason": "Nog geen duidelijke context; behandel de markt voorlopig als range.",
        })

    return result





def apply_context_trade_permissions(
    context_engine: Dict[str, object],
    current_price: Optional[float],
    support: Optional[float],
    resistance: Optional[float],
    long_timing_label: str,
    short_timing_label: str,
) -> Dict[str, object]:
    """
    Laat de context-engine bepalen welke trade-richting überhaupt nog mag.
    Deze laag komt bovenop bestaande bias/timing/hard-filters.
    """
    market_state = str(context_engine.get("market_state", "range"))
    sub_state = str(context_engine.get("sub_state", "neutral"))
    impulse_active = bool(context_engine.get("impulse_active", False))
    hands_off = bool(context_engine.get("hands_off", False))

    allow_long = True
    allow_short = True
    preferred_side = None
    long_reason = "LONG toegestaan"
    short_reason = "SHORT toegestaan"

    if hands_off or impulse_active or market_state == "hands_off":
        allow_long = False
        allow_short = False
        long_reason = "Geblokkeerd: impuls bezig / hands off"
        short_reason = "Geblokkeerd: impuls bezig / hands off"
    elif market_state == "bullish_trend":
        allow_long = True
        allow_short = False
        preferred_side = "LONG"
        long_reason = "Toegestaan: bullish trend"
        short_reason = "Geblokkeerd: bullish trend laat vooral LONG toe"
    elif market_state == "bearish_trend":
        allow_long = False
        allow_short = True
        preferred_side = "SHORT"
        long_reason = "Geblokkeerd: bearish trend laat vooral SHORT toe"
        short_reason = "Toegestaan: bearish trend"
    elif market_state == "range":
        if sub_state == "range_low":
            allow_long = True
            allow_short = False
            preferred_side = "LONG"
            long_reason = "Toegestaan: range low"
            short_reason = "Geblokkeerd: in range liever alleen LONG aan de onderkant"
        elif sub_state == "range_high":
            allow_long = False
            allow_short = True
            preferred_side = "SHORT"
            long_reason = "Geblokkeerd: in range liever alleen SHORT aan de bovenkant"
            short_reason = "Toegestaan: range high"
        else:
            allow_long = False
            allow_short = False
            long_reason = "Geblokkeerd: midden in de range"
            short_reason = "Geblokkeerd: midden in de range"
    elif market_state == "choppy":
        allow_long = False
        allow_short = False
        long_reason = "Geblokkeerd: choppy markt"
        short_reason = "Geblokkeerd: choppy markt"
    elif market_state == "compressie":
        allow_long = False
        allow_short = False
        long_reason = "Geblokkeerd: compressie, wacht op breakout/retest"
        short_reason = "Geblokkeerd: compressie, wacht op breakout/retest"

    # Timing kan context niet overrulen, maar wel verder blokkeren.
    if allow_long and str(long_timing_label) == "BLOCKED":
        allow_long = False
        long_reason = "Geblokkeerd: timing blocked"
    if allow_short and str(short_timing_label) == "BLOCKED":
        allow_short = False
        short_reason = "Geblokkeerd: timing blocked"

    return {
        "allow_long": allow_long,
        "allow_short": allow_short,
        "preferred_side": preferred_side,
        "long_reason": long_reason,
        "short_reason": short_reason,
    }


def render_context_badges(selected_result: Dict[str, object]) -> None:
    main_label = str(selected_result.get("context_label_main") or "Onbekend")
    sub_label = str(selected_result.get("context_label_sub") or "-")
    reason = str(selected_result.get("context_reason") or "")

    state = str(selected_result.get("context_market_state") or "")
    hands_off = bool(selected_result.get("context_hands_off", False))

    if hands_off or state == "hands_off":
        st.error(f"Context: {main_label}")
    elif state == "bullish_trend":
        st.success(f"Context: {main_label}")
    elif state == "bearish_trend":
        st.error(f"Context: {main_label}")
    elif state == "compressie":
        st.warning(f"Context: {main_label}")
    elif state == "choppy":
        st.warning(f"Context: {main_label}")
    else:
        st.info(f"Context: {main_label}")

    if sub_label and sub_label != "-":
        st.caption(f"Sublabel: {sub_label}")
    if reason:
        st.caption(reason)



def build_trade_tab_story(selected_result: Dict[str, object]) -> Dict[str, object]:
    """
    Stap 4E: Trade-tab vertelt exact hetzelfde hoofdverhaal als de context-engine.
    """
    market_state = str(selected_result.get("context_market_state") or "")
    sub_state = str(selected_result.get("context_sub_state") or "")
    main_label = str(selected_result.get("context_label_main") or "Onbekend")
    sub_label = str(selected_result.get("context_label_sub") or "-")
    context_reason = str(selected_result.get("context_reason") or "")
    allow_long = bool(selected_result.get("context_allow_long", True))
    allow_short = bool(selected_result.get("context_allow_short", True))
    long_reason = str(selected_result.get("context_long_reason") or "")
    short_reason = str(selected_result.get("context_short_reason") or "")
    best_side = selected_result.get("best_side")
    best_metrics = selected_result.get("best_metrics")
    long_timing = str((selected_result.get("setup_timing") or {}).get("long_timing", "-"))
    short_timing = str((selected_result.get("setup_timing") or {}).get("short_timing", "-"))

    display_best_side = best_side
    display_best_metrics = best_metrics
    if best_side == "LONG" and not allow_long:
        display_best_side = None
        display_best_metrics = None
    if best_side == "SHORT" and not allow_short:
        display_best_side = None
        display_best_metrics = None

    story: Dict[str, object] = {
        "status_kind": "info",
        "headline": "Advies: Wachten",
        "summary": "De context-engine laat nog geen duidelijke hoofdsetup toe.",
        "detail": context_reason,
        "plan_text": "",
        "footer": context_reason,
        "display_best_side": display_best_side,
        "display_best_metrics": display_best_metrics,
    }

    if market_state == "hands_off":
        story.update({
            "status_kind": "warning",
            "headline": "Advies: HANDS OFF",
            "summary": "De markt is al in een impuls bezig. Niet achter prijs aan jagen; wacht op een nieuwe structuur of retest.",
            "detail": context_reason or "Impuls bezig / hands off.",
            "plan_text": "Nu geen nieuwe entry. Wacht tot prijs weer een nieuwe zone opbouwt.",
            "footer": context_reason or "Hands off: impuls bezig.",
            "display_best_side": None,
            "display_best_metrics": None,
        })
        return story

    if market_state == "compressie":
        story.update({
            "status_kind": "warning",
            "headline": "Advies: Wachten op breakout / retest",
            "summary": "De markt zit in compressie. Nog geen hoofdsetup; wacht op expansion en daarna pas een retest of reclaim.",
            "detail": context_reason or "Compressie: de markt trekt samen.",
            "plan_text": "Geen entry midden in compressie. Laat eerst de uitbraak komen en kijk daarna pas naar timing.",
            "footer": context_reason or "Compressie: wacht op breakout/retest.",
            "display_best_side": None,
            "display_best_metrics": None,
        })
        return story

    if market_state == "choppy":
        story.update({
            "status_kind": "warning",
            "headline": "Advies: Overslaan",
            "summary": "De markt is choppy en overlapt te veel. Dat geeft weinig nette follow-through voor een hoofdsetup.",
            "detail": context_reason or "Choppy markt.",
            "plan_text": "Liever geen trade totdat er weer een duidelijkere structuur ontstaat.",
            "footer": context_reason or "Choppy markt: liever overslaan.",
            "display_best_side": None,
            "display_best_metrics": None,
        })
        return story

    if market_state == "range" and sub_state == "mid_range":
        story.update({
            "status_kind": "warning",
            "headline": "Advies: Wachten",
            "summary": "Prijs zit midden in de range. De randen van de range zijn interessanter dan het midden.",
            "detail": context_reason or "Mid range.",
            "plan_text": "Nog geen hoofdsetup. Wacht op range low voor LONG of range high voor SHORT.",
            "footer": context_reason or "Range midden: liever wachten op een rand van de range.",
            "display_best_side": None,
            "display_best_metrics": None,
        })
        return story

    if display_best_metrics is not None and display_best_side == "LONG":
        mode_text = "early price-action" if selected_result.get("chosen_entry_variant") == "early_price_action" or selected_result.get("entry_mode") == "doopiecash" else "retest-breakout"
        story.update({
            "status_kind": "success",
            "headline": "Advies: LONG kansrijk",
            "summary": f"De Trade-tab volgt de context-engine: {main_label.lower()} • {sub_label.lower()}. Daarom is LONG nu de hoofdsetup.",
            "detail": long_reason or context_reason,
            "plan_text": f"LONG is toegestaan en de timing staat op {long_timing}. De gekozen opzet is {mode_text}.",
            "footer": f"Beste LONG setup op basis van context, timing en netto winst. {long_reason}",
        })
        return story

    if display_best_metrics is not None and display_best_side == "SHORT":
        mode_text = "early price-action" if selected_result.get("chosen_entry_variant") == "early_price_action" or selected_result.get("entry_mode") == "doopiecash" else "retest-breakout"
        story.update({
            "status_kind": "success",
            "headline": "Advies: SHORT kansrijk",
            "summary": f"De Trade-tab volgt de context-engine: {main_label.lower()} • {sub_label.lower()}. Daarom is SHORT nu de hoofdsetup.",
            "detail": short_reason or context_reason,
            "plan_text": f"SHORT is toegestaan en de timing staat op {short_timing}. De gekozen opzet is {mode_text}.",
            "footer": f"Beste SHORT setup op basis van context, timing en netto winst. {short_reason}",
        })
        return story

    if market_state == "bullish_trend":
        story.update({
            "status_kind": "info",
            "headline": "Advies: Wachten op LONG",
            "summary": f"De context is {main_label.lower()} • {sub_label.lower()}, dus LONG heeft voorrang. Er is alleen nog geen geldige hoofdsetup.",
            "detail": long_reason or context_reason,
            "plan_text": f"LONG timing: {long_timing}. SHORT blijft geblokkeerd zolang de bullish context actief is.",
            "footer": long_reason or context_reason,
            "display_best_side": None,
            "display_best_metrics": None,
        })
        return story

    if market_state == "bearish_trend":
        story.update({
            "status_kind": "info",
            "headline": "Advies: Wachten op SHORT",
            "summary": f"De context is {main_label.lower()} • {sub_label.lower()}, dus SHORT heeft voorrang. Er is alleen nog geen geldige hoofdsetup.",
            "detail": short_reason or context_reason,
            "plan_text": f"SHORT timing: {short_timing}. LONG blijft geblokkeerd zolang de bearish context actief is.",
            "footer": short_reason or context_reason,
            "display_best_side": None,
            "display_best_metrics": None,
        })
        return story

    if market_state == "range" and sub_state == "range_low":
        story.update({
            "status_kind": "info",
            "headline": "Advies: Wachten op LONG aan range low",
            "summary": "Deze markt is range-bound. LONG is alleen logisch aan de onderkant van de range.",
            "detail": long_reason or context_reason,
            "plan_text": f"LONG timing: {long_timing}. SHORT blijft geblokkeerd zolang prijs aan de range low zit.",
            "footer": long_reason or context_reason,
            "display_best_side": None,
            "display_best_metrics": None,
        })
        return story

    if market_state == "range" and sub_state == "range_high":
        story.update({
            "status_kind": "info",
            "headline": "Advies: Wachten op SHORT aan range high",
            "summary": "Deze markt is range-bound. SHORT is alleen logisch aan de bovenkant van de range.",
            "detail": short_reason or context_reason,
            "plan_text": f"SHORT timing: {short_timing}. LONG blijft geblokkeerd zolang prijs aan de range high zit.",
            "footer": short_reason or context_reason,
            "display_best_side": None,
            "display_best_metrics": None,
        })
        return story

    return story

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
    support_zone: Optional[Dict[str, float]] = None,
    resistance_zone: Optional[Dict[str, float]] = None,
    target_zone: Optional[Dict[str, float]] = None,
    invalidation_zone: Optional[Dict[str, float]] = None,
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
            opacity=0.18,
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

    def _add_zone(zone: Optional[Dict[str, float]], fill: str, line: str, label: Optional[str] = None, label_pos: str = "right"):
        if zone is None:
            return
        fig.add_hrect(
            y0=zone["low"],
            y1=zone["high"],
            line_width=0,
            fillcolor=fill,
        )
        fig.add_hline(
            y=zone["center"],
            line_width=2.0,
            line_dash="dot",
            line_color=line,
            annotation_text=label,
            annotation_position=f"top {label_pos}" if label else None,
            annotation_font_color="white" if label else None,
        )

    # Alleen actieve zones zichtbaar maken: veel rustiger dan zowel vlakken als alle losse lijnen.
    _add_zone(support_zone, "rgba(59,130,246,0.12)", "rgba(96,165,250,0.95)", "Entry zone" if support_zone else None)
    _add_zone(resistance_zone, "rgba(250,204,21,0.12)", "rgba(250,204,21,0.95)", "Entry zone" if resistance_zone else None)
    _add_zone(target_zone, "rgba(34,197,94,0.10)", "rgba(74,222,128,0.90)", "Target zone" if target_zone else None)
    _add_zone(invalidation_zone, "rgba(239,68,68,0.10)", "rgba(248,113,113,0.90)", "Invalidation" if invalidation_zone else None)

    unique_hard_supports = _unique_levels(list(hard_supports or []))
    unique_hard_resistances = _unique_levels(list(hard_resistances or []))

    # Alleen extra contextlijnen tonen als ze NIET samenvallen met de actieve zone.
    for lvl in unique_hard_supports[:2]:
        if _same_level(lvl, active_support):
            continue
        fig.add_hline(
            y=lvl,
            line_width=1.0,
            line_dash="dot",
            line_color="rgba(59,130,246,0.28)",
        )

    for lvl in unique_hard_resistances[:2]:
        if _same_level(lvl, active_resistance):
            continue
        fig.add_hline(
            y=lvl,
            line_width=1.0,
            line_dash="dot",
            line_color="rgba(250,204,21,0.28)",
        )

    if higher_trade_support is not None and not _same_level(higher_trade_support, active_support):
        fig.add_hline(
            y=higher_trade_support,
            line_width=0.9,
            line_dash="dot",
            line_color="rgba(96,165,250,0.22)",
        )

    if higher_trade_resistance is not None and not _same_level(higher_trade_resistance, active_resistance):
        fig.add_hline(
            y=higher_trade_resistance,
            line_width=0.9,
            line_dash="dot",
            line_color="rgba(250,204,21,0.22)",
        )

    # Rustiger overzicht: losse wicks/spikes kunnen de lage TF-chart platdrukken.
    y_range = None
    try:
        q_low = float(df["low"].quantile(0.03))
        q_high = float(df["high"].quantile(0.97))
        last_close = float(df["close"].iloc[-1])
        candidates = [q_low, q_high, last_close]
        for z in [support_zone, resistance_zone, target_zone, invalidation_zone]:
            if isinstance(z, dict):
                zl = float(z.get("low", last_close))
                zh = float(z.get("high", last_close))
                if abs(zl - last_close) / max(abs(last_close), 1e-9) * 100 <= 4.0:
                    candidates.append(zl)
                if abs(zh - last_close) / max(abs(last_close), 1e-9) * 100 <= 4.0:
                    candidates.append(zh)
        y_low = min(candidates)
        y_high = max(candidates)
        pad = max((y_high - y_low) * 0.18, abs(last_close) * 0.004)
        if y_high > y_low:
            y_range = [y_low - pad, y_high + pad]
    except Exception:
        y_range = None

    fig.update_layout(
        height=height,
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        margin=dict(l=10, r=10, t=20, b=10),
        yaxis=dict(title="Prijs", showgrid=True, gridcolor="rgba(255,255,255,0.06)", range=y_range),
        yaxis2=dict(
            title="Volume",
            overlaying="y",
            side="right",
            showgrid=False,
            rangemode="tozero",
            position=1.0,
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.01,
            xanchor="right",
            x=1.0,
            bgcolor="rgba(0,0,0,0)",
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


@st.cache_data(ttl=LIVE_PRICE_CACHE_SEC, show_spinner=False)
def get_bitvavo_all_prices_live(cache_buster: int = 0) -> Dict[str, float]:
    url = f"{BASE_URL}{API_PREFIX}/ticker/price"
    try:
        response = requests.get(
            url,
            params={"_": cache_buster},
            timeout=5,
            headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
        )
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


def get_prices_safe(force_refresh: bool = False) -> Dict[str, float]:
    cache_buster = int(time.time()) if force_refresh else int(time.time() // max(1, LIVE_PRICE_CACHE_SEC))

    prices: Dict[str, float] = {}
    source = "live"
    try:
        prices = get_bitvavo_all_prices_live(cache_buster=cache_buster)
    except Exception:
        prices = {}

    if not prices:
        source = "fallback"
        try:
            prices = get_bitvavo_all_prices()
        except Exception:
            prices = {}

    if prices:
        st.session_state.last_good_price_map = prices.copy()
        st.session_state.last_price_source = source
        st.session_state.last_price_fetch_ts = time.time()
        return prices

    cached_prices = st.session_state.get("last_good_price_map", {}) or {}
    if cached_prices:
        st.session_state.last_price_source = "cached"
        return cached_prices.copy()

    st.session_state.last_price_source = "unavailable"
    return {}


def get_live_price_for_market(market: str) -> Optional[float]:
    all_prices = get_prices_safe(force_refresh=False)
    if market in all_prices:
        return all_prices[market]

    direct_price = get_bitvavo_price(market)
    if direct_price is not None:
        last_good = st.session_state.get("last_good_price_map", {}) or {}
        last_good[market] = float(direct_price)
        st.session_state.last_good_price_map = last_good
        st.session_state.last_price_source = "direct"
        st.session_state.last_price_fetch_ts = time.time()
        return float(direct_price)

    cached_prices = st.session_state.get("last_good_price_map", {}) or {}
    return cached_prices.get(market)


def build_shared_market_snapshot(force_refresh: bool = False) -> Dict[str, object]:
    price_map = get_prices_safe(force_refresh=force_refresh)
    snapshot = {
        "created_at": time.time(),
        "price_map": price_map.copy() if price_map else {},
        "price_source": st.session_state.get("last_price_source", "unknown"),
        "fetch_ts": float(st.session_state.get("last_price_fetch_ts", 0.0) or 0.0),
    }
    st.session_state.shared_market_snapshot = snapshot
    return snapshot


def get_shared_market_snapshot(force_refresh: bool = False) -> Dict[str, object]:
    snapshot = st.session_state.get("shared_market_snapshot")
    if force_refresh or not isinstance(snapshot, dict) or not snapshot.get("price_map"):
        return build_shared_market_snapshot(force_refresh=force_refresh)
    return snapshot


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
    return round(min(95.0, score), 1)



def timing_to_score(timing: str) -> float:
    normalized = normalize_trader_status(timing) if "normalize_trader_status" in globals() else str(timing)
    return {
        "READY": 18.0,
        "PLAN": 10.0,
        "WAIT": 0.0,
        "SCALE_OUT": 14.0,
        "MISSED": -35.0,
        "HANDS_OFF": -80.0,
        "BLOCKED": -100.0,
        "NEAR": 10.0,
        "WATCH": 0.0,
        "LOW PRIORITY": -6.0,
        "geen data": -25.0,
    }.get(str(normalized), -8.0)




# =========================================================
# Phase 6B - Trader Status Engine
# =========================================================
TRADER_STATUSES = {"PLAN", "READY", "WAIT", "HANDS_OFF", "MISSED", "SCALE_OUT", "BLOCKED"}


def normalize_trader_status(status: Optional[str]) -> str:
    raw = str(status or "WAIT").strip().upper().replace(" ", "_").replace("-", "_")
    mapping = {
        "NEAR": "PLAN",
        "WATCH": "WAIT",
        "DOOPIECASH_READY": "PLAN",
        "PLAN_READY": "PLAN",
        "PLAN_NEAR": "PLAN",
        "ENTRY_READY": "READY",
        "CONFIRMED_READY": "READY",
        "LOW_PRIORITY": "WAIT",
        "NO_DATA": "WAIT",
        "SKIP": "HANDS_OFF",
        "HANDS_OFF": "HANDS_OFF",
        "BLOCKED": "BLOCKED",
    }
    raw = mapping.get(raw, raw)
    return raw if raw in TRADER_STATUSES else "WAIT"


def compute_plan_status(
    current_price: Optional[float],
    entry_zone: Optional[Dict[str, float]],
    timing_label: str,
    context_engine: Optional[Dict[str, object]],
    location_info: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    context_engine = context_engine or {}
    market_state = str(context_engine.get("market_state", "range"))
    hands_off = bool(context_engine.get("hands_off", False) or context_engine.get("impulse_active", False))
    location_quality = str((location_info or {}).get("quality", "UNKNOWN"))

    if hands_off or market_state in {"hands_off", "choppy"}:
        return {"status": "HANDS_OFF", "reason": "Context zegt: niet doen / niet chasen."}
    if str(timing_label) == "BLOCKED":
        return {"status": "BLOCKED", "reason": "Richting geblokkeerd door context."}
    normalized_timing = normalize_trader_status(timing_label)
    if normalized_timing == "HANDS_OFF":
        return {"status": "HANDS_OFF", "reason": "Impuls actief: niet achter prijs aanrennen."}
    if location_quality == "SKIP" or normalized_timing == "MISSED":
        return {"status": "MISSED", "reason": "Move is al te ver voorbij de zone."}
    if current_price is not None and entry_zone is not None and distance_to_zone_pct(current_price, entry_zone) == 0.0:
        return {"status": "READY", "reason": "Prijs zit in de entry-zone."}
    if str(timing_label) in {"READY", "NEAR"}:
        return {"status": "PLAN", "reason": "Zone is dichtbij: order/plan voorbereiden."}
    return {"status": "PLAN", "reason": "Plan vooraf klaarzetten rond de relevante zone."}


def compute_entry_status(
    timing_label: str,
    plan_valid: bool,
    confirmed: bool,
    setup_family: str,
    context_allowed: bool,
    location_info: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    location_quality = str((location_info or {}).get("quality", "UNKNOWN"))
    normalized_timing = normalize_trader_status(timing_label)
    if not context_allowed or normalized_timing == "BLOCKED":
        return {"status": "BLOCKED", "reason": "Entry geblokkeerd door context/timing."}
    if normalized_timing == "HANDS_OFF":
        return {"status": "HANDS_OFF", "reason": "Impuls actief: niet achter prijs aanrennen."}
    if location_quality == "SKIP" or normalized_timing == "MISSED":
        return {"status": "MISSED", "reason": "Te laat voor een nette entry."}
    if not plan_valid:
        return {"status": "WAIT", "reason": "Setup is nog niet technisch valide."}
    # V3.6 / Fase 5: confirmatie blokkeert de entry niet meer.
    # Confirmatie geeft alleen extra confidence/score. De zone + timing blijven leidend.
    if str(timing_label) == "READY":
        if setup_family == "retest_breakout" and not confirmed:
            return {"status": "READY", "reason": "Entry is technisch handelbaar; confirmatie ontbreekt nog maar blokkeert niet."}
        return {"status": "READY", "reason": "Entry is nu handelbaar."}
    if str(timing_label) == "NEAR":
        return {"status": "PLAN", "reason": "Bijna bij de zone: plan klaarzetten, nog niet market chasen."}
    return {"status": "WAIT", "reason": "Wachten op betere timing."}


def compute_exit_status(
    current_price: Optional[float],
    target_zone: Optional[Dict[str, float]],
    side: str,
) -> Dict[str, object]:
    if current_price is None or target_zone is None:
        return {"status": "WAIT", "reason": "Geen target-zone beschikbaar."}
    if distance_to_zone_pct(current_price, target_zone) == 0.0:
        return {"status": "SCALE_OUT", "reason": "Prijs zit in target-zone: winst nemen / scale-out."}
    return {"status": "WAIT", "reason": "Target-zone nog niet bereikt."}


def compute_trade_status(
    mode: str,
    timing_label: str,
    plan_valid: bool,
    setup_family: str,
    context_allowed: bool,
    context_engine: Optional[Dict[str, object]],
    current_price: Optional[float] = None,
    entry_zone: Optional[Dict[str, float]] = None,
    target_zone: Optional[Dict[str, float]] = None,
    side: str = "LONG",
    location_info: Optional[Dict[str, object]] = None,
    confirmed: bool = False,
) -> Dict[str, object]:
    exit_status = compute_exit_status(current_price, target_zone, side)
    if exit_status.get("status") == "SCALE_OUT":
        return exit_status
    if str(mode).lower() == "plan":
        return compute_plan_status(current_price, entry_zone, timing_label, context_engine, location_info)
    return compute_entry_status(timing_label, plan_valid, confirmed, setup_family, context_allowed, location_info)



# =========================================================
# Phase 12.6 - Pre-Plan Engine
# =========================================================
def detect_plan_zone_status(
    current_price: Optional[float],
    entry_zone: Optional[Dict[str, float]],
    target_zone: Optional[Dict[str, float]],
    side: str,
) -> Dict[str, object]:
    """
    Bepaalt of de bot NU moet handelen, vooraf een limit-plan moet klaarzetten,
    of de move al te ver richting target is.
    """
    if current_price is None or entry_zone is None:
        return {"status": "WAIT", "action": "Geen plan", "reason": "Geen entry-zone beschikbaar."}

    cp = float(current_price)
    in_entry = distance_to_zone_pct(cp, entry_zone) == 0.0
    dist_entry = distance_to_zone_pct(cp, entry_zone)
    dist_target = distance_to_zone_pct(cp, target_zone) if target_zone is not None else None

    if in_entry:
        return {
            "status": "READY",
            "action": "KOOP NU" if str(side).upper() == "LONG" else "VERKOOP NU",
            "reason": "Prijs zit nu in de vooraf bepaalde entry-zone.",
            "distance_to_entry_pct": 0.0,
            "distance_to_target_pct": dist_target,
        }

    if dist_target is not None and dist_target <= 0.18:
        return {
            "status": "MISSED",
            "action": "Niet chasen",
            "reason": "Prijs zit al dicht bij de target-zone; wacht op nieuwe pullback/retest.",
            "distance_to_entry_pct": dist_entry,
            "distance_to_target_pct": dist_target,
        }

    return {
        "status": "PLAN",
        "action": "Limit-zone klaarzetten",
        "reason": "Vooraf plan klaarzetten rond de support/resistance-zone; niet pas wachten op reactie.",
        "distance_to_entry_pct": dist_entry,
        "distance_to_target_pct": dist_target,
    }


def suppress_countertrend_plan(
    candidate: Dict[str, object],
    preferred_side: Optional[str],
    context_engine: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    """Onderdrukt oude short/long countertrend-plannen in duidelijke trendcontext."""
    item = dict(candidate)
    side = str(item.get("side", "")).upper()
    preferred = str(preferred_side or "").upper()
    if not preferred or side == preferred:
        return item

    market_state = str((context_engine or {}).get("market_state", ""))
    is_trend = market_state in {"bullish_trend", "bearish_trend"}
    confirmed = bool((item.get("setup_detection") or {}).get("confirmed", False))
    status = normalize_trader_status(item.get("status"))

    if is_trend and not (status == "READY" and confirmed):
        item["allowed_by_context"] = False
        item["status"] = "BLOCKED"
        item["reason"] = (
            "Countertrend onderdrukt: in bullish trend is resistance eerst target, geen short-entry."
            if preferred == "LONG"
            else "Countertrend onderdrukt: in bearish trend is support eerst target, geen long-entry."
        )
        item["score"] = float(item.get("score", 0.0) or 0.0) - 250.0
    return item


def build_pre_trade_plan(
    candidate: Optional[Dict[str, object]],
    current_price: Optional[float],
    coin_symbol: str,
) -> Optional[Dict[str, object]]:
    """Voegt expliciet vooraf-plan toe aan een bestaande plan-candidate."""
    if not isinstance(candidate, dict):
        return None
    item = dict(candidate)
    metrics = item.get("metrics") or {}
    if not isinstance(metrics, dict) or metrics.get("entry") is None:
        return item

    side = str(item.get("side", "LONG")).upper()
    entry_zone = build_price_zone(metrics.get("entry"), get_coin_zone_width_pct(coin_symbol, None, "entry"))
    target_zone = build_price_zone(metrics.get("target"), get_coin_zone_width_pct(coin_symbol, None, "target")) if metrics.get("target") is not None else None
    invalidation_zone = build_price_zone(metrics.get("stop"), get_coin_zone_width_pct(coin_symbol, None, "invalidation")) if metrics.get("stop") is not None else None
    zone_status = detect_plan_zone_status(current_price, entry_zone, target_zone, side)

    item["pre_trade_plan"] = {
        "active": zone_status.get("status") in {"READY", "PLAN"},
        "side": side,
        "action": zone_status.get("action"),
        "entry_zone": entry_zone,
        "target_zone": target_zone,
        "invalidation_zone": invalidation_zone,
        "status": zone_status.get("status"),
        "reason": zone_status.get("reason"),
        "distance_to_entry_pct": zone_status.get("distance_to_entry_pct"),
        "distance_to_target_pct": zone_status.get("distance_to_target_pct"),
    }

    old_status = normalize_trader_status(item.get("status"))
    if old_status not in {"BLOCKED", "HANDS_OFF"}:
        item["status"] = zone_status.get("status", "PLAN")
        item["reason"] = zone_status.get("reason") or item.get("reason")
        if item["status"] == "READY":
            item["score"] = float(item.get("score", 0.0) or 0.0) + 35.0
        elif item["status"] == "PLAN":
            item["score"] = float(item.get("score", 0.0) or 0.0) + 18.0
    return item


def show_plan_before_reaction(
    candidates: List[Dict[str, object]],
    current_price: Optional[float],
    coin_symbol: str,
    preferred_side: Optional[str],
    context_engine: Optional[Dict[str, object]],
) -> List[Dict[str, object]]:
    """Pipeline: eerst vooraf-plan maken, daarna countertrend-plannen dempen."""
    out: List[Dict[str, object]] = []
    for candidate in candidates or []:
        planned = build_pre_trade_plan(candidate, current_price, coin_symbol) or candidate
        planned = suppress_countertrend_plan(planned, preferred_side, context_engine)
        out.append(planned)
    return sorted(out, key=lambda x: (bool(x.get("allowed_by_context", False)), _status_rank(x.get("status")), float(x.get("score", 0.0) or 0.0)), reverse=True)


# =========================================================
# Phase 7 - Anti Chase Engine
# =========================================================
def detect_impulse_chase_risk(
    df: Optional[pd.DataFrame],
    current_price: Optional[float],
    entry_level: Optional[float],
    target_level: Optional[float],
    side: str,
    timeframe_label: str,
    vol_profile: Optional[Dict[str, float | str]] = None,
) -> Dict[str, object]:
    """
    Herkent of de bot te laat is:
    - prijs is al ver van entry-zone doorgelopen richting target
    - laatste candle is een impuls/displacement candle
    - target is al bijna geraakt of recent geraakt
    Price action only: candles, range, afstand tot zone en swing-progress.
    """
    result: Dict[str, object] = {
        "chase_risk": False,
        "impulse_active": False,
        "extended_from_zone": False,
        "target_almost_hit": False,
        "target_recently_hit": False,
        "status": "OK",
        "reason": "Geen anti-chase blokkade.",
        "body_ratio": 1.0,
        "extension_pct": None,
        "progress_pct": None,
    }

    if current_price is None or entry_level is None:
        return result

    cp = float(current_price)
    entry = float(entry_level)
    if cp <= 0 or entry <= 0:
        return result

    side_l = str(side).lower()
    tf = str(timeframe_label)
    avg_range_pct = float((vol_profile or {}).get("avg_range_pct", 1.0) or 1.0)

    # Lower TF mag strakker zijn; hogere TF iets ruimer.
    max_extension_pct = {"1m": 0.22, "5m": 0.35, "15m": 0.55, "30m": 0.75, "1h": 1.10, "4h": 1.80, "1d": 2.80}.get(tf, 0.75)
    max_extension_pct = max(max_extension_pct, avg_range_pct * 0.45)
    impulse_body_ratio_limit = {"1m": 1.75, "5m": 1.85, "15m": 2.00, "30m": 2.10, "1h": 2.20, "4h": 2.35, "1d": 2.50}.get(tf, 2.0)

    if side_l == "long":
        extension_pct = ((cp - entry) / cp) * 100.0
    else:
        extension_pct = ((entry - cp) / cp) * 100.0
    result["extension_pct"] = round(float(extension_pct), 4)

    progress_pct = None
    if target_level is not None:
        target = float(target_level)
        total_move = abs(target - entry)
        if total_move > 0:
            if side_l == "long":
                progress_pct = ((cp - entry) / total_move) * 100.0
                target_almost_hit = cp >= entry + total_move * 0.75
            else:
                progress_pct = ((entry - cp) / total_move) * 100.0
                target_almost_hit = cp <= entry - total_move * 0.75
            result["progress_pct"] = round(float(progress_pct), 2)
            result["target_almost_hit"] = bool(target_almost_hit)

    if df is not None and len(df) >= 6:
        recent = df.tail(min(len(df), 20)).copy()
        bodies = (recent["close"] - recent["open"]).abs()
        avg_body = float(bodies.iloc[:-1].mean()) if len(bodies) > 1 else 0.0
        last = recent.iloc[-1]
        last_body = abs(float(last["close"]) - float(last["open"]))
        body_ratio = (last_body / avg_body) if avg_body > 0 else 1.0
        result["body_ratio"] = round(float(body_ratio), 3)

        bullish_impulse = bool(float(last["close"]) > float(last["open"]) and body_ratio >= impulse_body_ratio_limit)
        bearish_impulse = bool(float(last["close"]) < float(last["open"]) and body_ratio >= impulse_body_ratio_limit)
        result["impulse_active"] = bool((side_l == "long" and bullish_impulse) or (side_l == "short" and bearish_impulse))

        if target_level is not None:
            recent_window = {"1m": 12, "5m": 10, "15m": 8, "30m": 6, "1h": 5}.get(tf, 8)
            r = df.tail(min(len(df), recent_window))
            if side_l == "long":
                result["target_recently_hit"] = bool(float(r["high"].max()) >= float(target_level))
            else:
                result["target_recently_hit"] = bool(float(r["low"].min()) <= float(target_level))

    result["extended_from_zone"] = bool(extension_pct > max_extension_pct)

    if bool(result["target_recently_hit"]):
        result.update({
            "chase_risk": True,
            "status": "MISSED",
            "reason": "Target is recent al geraakt; setup is voorbij.",
        })
    elif bool(result["impulse_active"]) and bool(result["extended_from_zone"]):
        result.update({
            "chase_risk": True,
            "status": "HANDS_OFF",
            "reason": "Impuls-candle actief en prijs is al te ver van de zone; niet chasen.",
        })
    elif bool(result["target_almost_hit"]) or (progress_pct is not None and progress_pct >= 70):
        result.update({
            "chase_risk": True,
            "status": "MISSED",
            "reason": "Move is al grotendeels richting target gegaan.",
        })
    elif bool(result["extended_from_zone"]):
        result.update({
            "chase_risk": True,
            "status": "MISSED",
            "reason": "Prijs is te ver van de entry-zone doorgelopen.",
        })

    return result


def mark_setup_as_missed_if_extended(
    plan: Optional[Dict[str, object]],
    chase_risk: Dict[str, object],
) -> Optional[Dict[str, object]]:
    if isinstance(plan, dict) and str((chase_risk or {}).get("status")) == "MISSED":
        plan["status"] = "MISSED"
        plan["valid"] = False
        plan["anti_chase"] = chase_risk
        plan["reason"] = str(chase_risk.get("reason") or "Setup gemist: prijs is te ver doorgelopen.")
    return plan


def mark_setup_as_hands_off_if_impulsive(
    plan: Optional[Dict[str, object]],
    chase_risk: Dict[str, object],
) -> Optional[Dict[str, object]]:
    if isinstance(plan, dict) and str((chase_risk or {}).get("status")) == "HANDS_OFF":
        plan["status"] = "HANDS_OFF"
        plan["valid"] = False
        plan["anti_chase"] = chase_risk
        plan["reason"] = str(chase_risk.get("reason") or "Hands off: impuls actief, niet chasen.")
    return plan


def apply_anti_chase_to_plan(
    plan: Optional[Dict[str, object]],
    chase_risk: Dict[str, object],
) -> Optional[Dict[str, object]]:
    plan = mark_setup_as_missed_if_extended(plan, chase_risk)
    plan = mark_setup_as_hands_off_if_impulsive(plan, chase_risk)
    return plan


# =========================================================
# Phase 12.5 - Zone Flip / Reclaim Engine
# =========================================================
def _zone_from_any(zone: Optional[Dict[str, object]]) -> Optional[Dict[str, float]]:
    if not isinstance(zone, dict):
        return None
    try:
        return {"low": float(zone["low"]), "high": float(zone["high"]), "center": float(zone.get("center", (float(zone["low"]) + float(zone["high"])) / 2.0)), "width_pct": float(zone.get("width_pct", 0.0))}
    except Exception:
        return None


def detect_zone_break_and_acceptance(
    df: Optional[pd.DataFrame],
    zone: Optional[Dict[str, object]],
    direction: str,
    confirm_closes: int = 2,
    lookback: int = 8,
) -> Dict[str, object]:
    """
    Detecteer price-action acceptance door een zone heen.
    LONG flip: meerdere closes boven oude target/resistance-zone.
    SHORT flip: meerdere closes onder oude support/target-zone.
    """
    result: Dict[str, object] = {
        "accepted": False,
        "direction": str(direction).lower(),
        "accepted_closes": 0,
        "last_close": None,
        "break_edge": None,
        "reason": "Geen duidelijke acceptance door zone.",
    }
    z = _zone_from_any(zone)
    if df is None or len(df) < max(3, confirm_closes) or z is None:
        return result

    recent = df.tail(min(len(df), max(lookback, confirm_closes))).copy()
    closes = recent["close"].astype(float)
    last_close = float(closes.iloc[-1])
    result["last_close"] = last_close

    if str(direction).lower() in {"long", "up", "above"}:
        edge = float(z["high"])
        close_mask = closes > edge
        accepted_closes = int(close_mask.tail(confirm_closes).sum())
        # Binnen de recente candles moet de zone ook daadwerkelijk gebroken zijn.
        broke_zone = bool(float(recent["high"].max()) > edge)
        accepted = bool(broke_zone and accepted_closes >= confirm_closes and last_close > edge)
        result.update({
            "accepted": accepted,
            "accepted_closes": accepted_closes,
            "break_edge": edge,
            "reason": f"{accepted_closes} closes boven oude resistance/target-zone." if accepted else "Nog geen acceptance boven oude resistance/target-zone.",
        })
        return result

    edge = float(z["low"])
    close_mask = closes < edge
    accepted_closes = int(close_mask.tail(confirm_closes).sum())
    broke_zone = bool(float(recent["low"].min()) < edge)
    accepted = bool(broke_zone and accepted_closes >= confirm_closes and last_close < edge)
    result.update({
        "accepted": accepted,
        "accepted_closes": accepted_closes,
        "break_edge": edge,
        "reason": f"{accepted_closes} closes onder oude support/target-zone." if accepted else "Nog geen acceptance onder oude support/target-zone.",
    })
    return result


def detect_flipped_zone(
    df: Optional[pd.DataFrame],
    zone: Optional[Dict[str, object]],
    direction: str,
    confirm_closes: int = 2,
    retest_lookback: int = 6,
) -> Dict[str, object]:
    """
    Bepaal of een oude target/resistance/support-zone geflipt is.
    - LONG: oude target/resistance wordt support.
    - SHORT: oude support/target wordt resistance.
    """
    z = _zone_from_any(zone)
    acceptance = detect_zone_break_and_acceptance(df, z, direction, confirm_closes=confirm_closes)
    result: Dict[str, object] = {
        "active": False,
        "direction": str(direction).lower(),
        "flipped_role": None,
        "zone": z,
        "acceptance": acceptance,
        "retest_active": False,
        "retest_state": "WAIT",
        "reason": acceptance.get("reason", "Geen zone flip."),
    }
    if not acceptance.get("accepted") or df is None or z is None or len(df) < 2:
        return result

    recent = df.tail(min(len(df), retest_lookback)).copy()
    last_close = float(recent["close"].iloc[-1])
    direction_l = str(direction).lower()

    if direction_l in {"long", "up", "above"}:
        touched_retest = bool(float(recent["low"].min()) <= float(z["high"]) and float(recent["high"].max()) >= float(z["low"]))
        currently_in_zone = bool(float(z["low"]) <= last_close <= float(z["high"]))
        held_above = bool(last_close >= float(z["low"]))
        retest_active = bool(touched_retest and held_above)
        state = "READY" if currently_in_zone else ("PLAN" if retest_active or held_above else "WAIT")
        result.update({
            "active": True,
            "flipped_role": "support",
            "retest_active": retest_active,
            "retest_state": state,
            "reason": "Oude target/resistance is geaccepteerd boven prijs en fungeert nu als mogelijke support.",
        })
        return result

    touched_retest = bool(float(recent["high"].max()) >= float(z["low"]) and float(recent["low"].min()) <= float(z["high"]))
    currently_in_zone = bool(float(z["low"]) <= last_close <= float(z["high"]))
    held_below = bool(last_close <= float(z["high"]))
    retest_active = bool(touched_retest and held_below)
    state = "READY" if currently_in_zone else ("PLAN" if retest_active or held_below else "WAIT")
    result.update({
        "active": True,
        "flipped_role": "resistance",
        "retest_active": retest_active,
        "retest_state": state,
        "reason": "Oude support/target is geaccepteerd onder prijs en fungeert nu als mogelijke resistance.",
    })
    return result


def _next_level_after_flip(
    side: str,
    entry_level: float,
    precision_levels: Optional[Dict[str, object]],
    fallback_level: Optional[float],
    timeframe_label: str,
) -> Optional[float]:
    """Pak na een flip niet opnieuw dezelfde zone als target, maar de volgende micro-zone."""
    tf = str(timeframe_label)
    side_l = str(side).lower()
    micro = (precision_levels or {}).get("micro_structure", {}) if isinstance(precision_levels, dict) else {}
    min_gap = {"1m": 0.025, "5m": 0.045, "15m": 0.070, "30m": 0.10, "1h": 0.18}.get(tf, 0.07)
    max_gap = {"1m": 0.55, "5m": 0.85, "15m": 1.30, "30m": 1.90, "1h": 3.00}.get(tf, 1.30)

    candidates: List[float] = []
    if isinstance(micro, dict):
        if side_l == "long":
            candidates.extend([float(x) for x in list(micro.get("trade_resistances", [])) + list(micro.get("hard_resistances", [])) if x is not None and float(x) > entry_level])
        else:
            candidates.extend([float(x) for x in list(micro.get("trade_supports", [])) + list(micro.get("hard_supports", [])) if x is not None and float(x) < entry_level])

    if fallback_level is not None:
        candidates.append(float(fallback_level))

    valid: List[float] = []
    for lvl in candidates:
        gap = abs(float(lvl) - float(entry_level)) / max(abs(float(entry_level)), 1e-9) * 100.0
        if min_gap <= gap <= max_gap:
            valid.append(float(lvl))
    if not valid:
        return None
    return min(valid) if side_l == "long" else max(valid)


def build_retest_entry_from_flipped_zone(
    side: str,
    flipped_zone_info: Dict[str, object],
    current_price: Optional[float],
    precision_levels: Optional[Dict[str, object]],
    fallback_target: Optional[float],
    fallback_stop: Optional[float],
    timeframe_label: str,
    coin_symbol: str,
    vol_profile: Optional[Dict[str, float | str]] = None,
) -> Dict[str, object]:
    """Maak een nieuwe retest-entry vanuit een geflipte zone."""
    zone = _zone_from_any((flipped_zone_info or {}).get("zone"))
    result: Dict[str, object] = {
        "active": False,
        "side": str(side).upper(),
        "entry_level": None,
        "entry_zone": zone,
        "stop": None,
        "target": None,
        "status": "WAIT",
        "reason": "Geen geldige flipped-zone retest.",
    }
    if not flipped_zone_info.get("active") or zone is None:
        return result

    side_l = str(side).lower()
    entry_level = float(zone["center"])
    if side_l == "long":
        raw_stop = float(zone["low"])
    else:
        raw_stop = float(zone["high"])

    stop = select_intraday_stop(
        side=side_l,
        entry_price=entry_level,
        precision_levels=precision_levels,
        fallback_stop=raw_stop if fallback_stop is None else fallback_stop,
        timeframe_label=timeframe_label,
        coin_symbol=coin_symbol,
    )
    # Bij flip moet stop altijd voorbij de geflipte zone liggen.
    width = get_coin_zone_width_pct(coin_symbol, vol_profile, zone_kind="invalidation") / 100.0
    if side_l == "long":
        stop = min(float(stop or raw_stop), float(zone["low"]) * (1 - width * 0.35))
    else:
        stop = max(float(stop or raw_stop), float(zone["high"]) * (1 + width * 0.35))

    target = _next_level_after_flip(side_l, entry_level, precision_levels, fallback_target, timeframe_label)
    result.update({
        "active": True,
        "entry_level": entry_level,
        "stop": stop,
        "target": target,
        "status": str(flipped_zone_info.get("retest_state", "PLAN")),
        "reason": "Zone flip actief: oude target/resistance/support wordt retest-entry.",
    })
    return result


# =========================================================
# Phase 8 - Structural TP / SL Engine
# =========================================================
def _collect_structural_target_candidates(
    side: str,
    entry_price: Optional[float],
    primary_level: Optional[float],
    backup_level: Optional[float],
    precision_levels: Optional[Dict[str, object]] = None,
) -> List[Dict[str, object]]:
    """Collecteer target-kandidaten uit structuur. RR wordt hier bewust niet gebruikt."""
    if entry_price is None:
        return []
    entry = float(entry_price)
    side_l = str(side).lower()
    candidates: List[Dict[str, object]] = []

    def _add(level: Optional[float], source: str, weight: float) -> None:
        if level is None:
            return
        lvl = float(level)
        if side_l == "long" and lvl <= entry:
            return
        if side_l == "short" and lvl >= entry:
            return
        dist_pct = abs(lvl - entry) / max(abs(entry), 1e-9) * 100.0
        candidates.append({"level": lvl, "source": source, "distance_pct": dist_pct, "weight": weight})

    micro = (precision_levels or {}).get("micro_structure", {}) if isinstance(precision_levels, dict) else {}
    if isinstance(micro, dict):
        if side_l == "long":
            for lvl in list(micro.get("trade_resistances", [])) + list(micro.get("hard_resistances", [])):
                _add(lvl, "micro_resistance", 3.2)
        else:
            for lvl in list(micro.get("trade_supports", [])) + list(micro.get("hard_supports", [])):
                _add(lvl, "micro_support", 3.2)

    _add(primary_level, "primary_opposing_zone", 2.6)
    _add(backup_level, "backup_opposing_zone", 1.8)
    return candidates


def score_tp_realism(
    side: str,
    entry_price: Optional[float],
    target_level: Optional[float],
    timeframe_label: str,
    vol_profile: Optional[Dict[str, float | str]] = None,
    candidate_source: str = "structure",
) -> Dict[str, object]:
    """Score of TP logisch dichtbij de eerstvolgende structuur ligt, niet wensdenken."""
    if entry_price is None or target_level is None:
        return {"score": 0.0, "quality": "missing", "reason": "Geen structurele target-zone beschikbaar."}
    entry = float(entry_price)
    target = float(target_level)
    if entry <= 0 or target <= 0:
        return {"score": 0.0, "quality": "invalid", "reason": "Ongeldige target-data."}

    dist_pct = abs(target - entry) / entry * 100.0
    tf = str(timeframe_label)
    avg_range_pct = float((vol_profile or {}).get("avg_range_pct", 1.0) or 1.0)
    min_dist = {"1m": 0.025, "5m": 0.045, "15m": 0.070, "30m": 0.16, "1h": 0.25, "4h": 0.55, "1d": 0.90}.get(tf, 0.20)
    max_dist = {"1m": 0.42, "5m": 0.70, "15m": 1.10, "30m": 2.10, "1h": 3.20, "4h": 6.50, "1d": 12.0}.get(tf, 2.0)
    max_dist = max(max_dist, avg_range_pct * 1.35)

    score = 70.0
    reason = "TP komt uit eerstvolgende logische structuurzone."
    if dist_pct < min_dist:
        score -= 35.0
        reason = "TP ligt te dicht op entry; weinig ruimte na fees/ruis."
    elif dist_pct > max_dist:
        score -= 40.0
        reason = "TP ligt te ver voor deze timeframe; mogelijk wensdenken."
    elif "micro" in str(candidate_source):
        score += 12.0
    elif "primary" in str(candidate_source):
        score += 8.0

    quality = "good" if score >= 70 else ("ok" if score >= 45 else "weak")
    return {"score": round(max(0.0, min(100.0, score)), 1), "quality": quality, "distance_pct": round(dist_pct, 4), "reason": reason, "source": candidate_source}


def select_structural_target_zone(
    side: str,
    entry_price: Optional[float],
    current_price: Optional[float],
    primary_opposing_level: Optional[float],
    backup_opposing_level: Optional[float],
    precision_levels: Optional[Dict[str, object]] = None,
    timeframe_label: str = "5m",
    vol_profile: Optional[Dict[str, float | str]] = None,
    fallback_level: Optional[float] = None,
) -> Dict[str, object]:
    """Kies TP puur uit structuur. RR komt pas daarna uit metrics."""
    reference = entry_price if entry_price is not None else current_price
    if reference is None:
        return {"level": fallback_level, "valid": fallback_level is not None, "source": "fallback", "score": {}, "reason": "Geen entry/current price voor target-selectie."}

    ref = float(reference)
    candidates = _collect_structural_target_candidates(side, ref, primary_opposing_level, backup_opposing_level, precision_levels)
    tf = str(timeframe_label)
    min_dist = {"1m": 0.025, "5m": 0.045, "15m": 0.070, "30m": 0.16, "1h": 0.25, "4h": 0.55, "1d": 0.90}.get(tf, 0.20)
    max_dist = {"1m": 0.45, "5m": 0.75, "15m": 1.15, "30m": 2.20, "1h": 3.50, "4h": 7.00, "1d": 13.0}.get(tf, 2.5)

    ranked: List[Tuple[float, Dict[str, object]]] = []
    for item in candidates:
        dist_pct = float(item.get("distance_pct", 999.0))
        if dist_pct < min_dist or dist_pct > max_dist:
            continue
        rank = (100.0 - dist_pct * 12.0) + float(item.get("weight", 1.0)) * 3.0
        ranked.append((rank, item))

    if ranked:
        ranked.sort(key=lambda x: x[0], reverse=True)
        chosen = ranked[0][1]
        score = score_tp_realism(side, ref, chosen["level"], tf, vol_profile, str(chosen.get("source", "structure")))
        return {"level": float(chosen["level"]), "valid": True, "source": chosen.get("source"), "score": score, "reason": score.get("reason", "Structurele TP gekozen.")}

    if fallback_level is not None:
        score = score_tp_realism(side, ref, fallback_level, tf, vol_profile, "fallback")
        if float(score.get("score", 0.0)) >= 45.0:
            return {"level": float(fallback_level), "valid": True, "source": "fallback", "score": score, "reason": "Fallback target gebruikt omdat geen betere structuurzone beschikbaar was."}

    return {"level": None, "valid": False, "source": "none", "score": {"score": 0.0, "quality": "missing"}, "reason": "Geen realistische eerstvolgende target-zone gevonden."}


def score_sl_breathing_room(
    side: str,
    entry_price: Optional[float],
    stop_level: Optional[float],
    invalidation_level: Optional[float],
    timeframe_label: str,
    vol_profile: Optional[Dict[str, float | str]] = None,
) -> Dict[str, object]:
    """Score of SL genoeg ademruimte heeft onder/boven structurele invalidatie."""
    if entry_price is None or stop_level is None:
        return {"score": 0.0, "quality": "missing", "reason": "Geen structurele SL beschikbaar."}
    entry = float(entry_price)
    stop = float(stop_level)
    if entry <= 0 or stop <= 0:
        return {"score": 0.0, "quality": "invalid", "reason": "Ongeldige SL-data."}

    risk_pct = abs(entry - stop) / entry * 100.0
    tf = str(timeframe_label)
    avg_range_pct = float((vol_profile or {}).get("avg_range_pct", 1.0) or 1.0)
    min_risk = {"1m": 0.05, "5m": 0.09, "15m": 0.14, "30m": 0.20, "1h": 0.30, "4h": 0.50, "1d": 0.80}.get(tf, 0.20)
    max_risk = {"1m": 0.55, "5m": 0.85, "15m": 1.35, "30m": 1.90, "1h": 2.80, "4h": 5.50, "1d": 9.0}.get(tf, 2.0)
    min_risk = max(min_risk, avg_range_pct * 0.18)
    max_risk = max(max_risk, avg_range_pct * 1.20)

    score = 72.0
    reason = "SL ligt voorbij structurele invalidatie met ademruimte."
    if risk_pct < min_risk:
        score -= 35.0
        reason = "SL is waarschijnlijk te strak voor normale candle-ruis."
    elif risk_pct > max_risk:
        score -= 28.0
        reason = "SL is erg ruim voor deze timeframe; setup vraagt mogelijk te veel risico."

    if invalidation_level is not None:
        inv = float(invalidation_level)
        if str(side).lower() == "long" and stop >= inv:
            score -= 25.0
            reason = "SL ligt niet duidelijk onder structurele invalidatie."
        if str(side).lower() == "short" and stop <= inv:
            score -= 25.0
            reason = "SL ligt niet duidelijk boven structurele invalidatie."

    quality = "good" if score >= 70 else ("ok" if score >= 45 else "weak")
    return {"score": round(max(0.0, min(100.0, score)), 1), "quality": quality, "risk_pct": round(risk_pct, 4), "reason": reason}


def select_structural_invalidation(
    side: str,
    entry_price: Optional[float],
    trade_level: Optional[float],
    hard_level: Optional[float],
    precision_levels: Optional[Dict[str, object]] = None,
    vol_profile: Optional[Dict[str, float | str]] = None,
    timeframe_label: str = "5m",
    coin_symbol: str = "BTC",
    fallback_stop: Optional[float] = None,
) -> Dict[str, object]:
    """Kies SL puur uit structurele invalidatie; niet om RR mooier te maken."""
    if entry_price is None:
        return {"level": fallback_stop, "valid": fallback_stop is not None, "source": "fallback", "score": {}, "reason": "Geen entry voor SL-selectie."}
    entry = float(entry_price)
    side_l = str(side).lower()
    tf = str(timeframe_label)
    active_precision = bool(isinstance(precision_levels, dict) and precision_levels.get("active"))

    if side_l == "long":
        invalidation = None
        if active_precision:
            invalidation = precision_levels.get("hard_support") or precision_levels.get("trade_support")
        invalidation = invalidation or hard_level or trade_level
    else:
        invalidation = None
        if active_precision:
            invalidation = precision_levels.get("hard_resistance") or precision_levels.get("trade_resistance")
        invalidation = invalidation or hard_level or trade_level

    if invalidation is None:
        return {"level": fallback_stop, "valid": fallback_stop is not None, "source": "fallback", "score": {}, "reason": "Geen structurele invalidatie gevonden."}

    avg_range_pct = float((vol_profile or {}).get("avg_range_pct", 1.0) or 1.0)
    base_buffer_pct = get_coin_zone_width_pct(coin_symbol, vol_profile, zone_kind="invalidation") * 0.35
    tf_min_buffer_pct = {"1m": 0.035, "5m": 0.060, "15m": 0.095, "30m": 0.14, "1h": 0.22, "4h": 0.38, "1d": 0.60}.get(tf, 0.12)
    buffer_pct = max(tf_min_buffer_pct, base_buffer_pct, avg_range_pct * 0.08)
    inv = float(invalidation)

    if side_l == "long":
        stop = inv * (1 - buffer_pct / 100.0)
        if stop >= entry:
            stop = entry * (1 - max(buffer_pct, tf_min_buffer_pct) / 100.0)
    else:
        stop = inv * (1 + buffer_pct / 100.0)
        if stop <= entry:
            stop = entry * (1 + max(buffer_pct, tf_min_buffer_pct) / 100.0)

    score = score_sl_breathing_room(side, entry, stop, inv, tf, vol_profile)
    return {"level": float(stop), "valid": True, "source": "structural_invalidation", "invalidation_level": inv, "buffer_pct": round(buffer_pct, 4), "score": score, "reason": score.get("reason", "Structurele SL gekozen.")}



# =========================================================
# Phase 9 - Plan Ladder / Scale Orders
# =========================================================
def build_limit_order_ladder(
    entry_zone: Optional[Dict[str, object]],
    side: str,
    steps: int = 3,
    weights: Optional[List[float]] = None,
    target: Optional[float] = None,
    stop: Optional[float] = None,
    account_size: Optional[float] = None,
    max_risk_pct: Optional[float] = None,
    coin_symbol: str = "",
    entry_fee_pct: float = 0.0,
    exit_fee_pct: float = 0.0,
    short_borrow_hourly_pct: float = 0.0,
    expected_hold_hours: float = 0.0,
    short_liquidation_fee_pct: float = DEFAULT_SHORT_LIQUIDATION_FEE_PCT,
) -> List[Dict[str, object]]:
    """
    Fase 4 - scherpe entry-zone ladder.

    Deze functie maakt geen losse entry-prijs meer, maar 3 planbare limit fills:
    - top fill: eerste aanraking van support/resistance, hoogste kans op fill
    - mid fill: midden van de zone
    - deep fill: beste prijs, maar grotere kans dat prijs hem niet vult

    Als target/stop/account-data beschikbaar zijn, krijgt elke fill eigen RR/risk/reward metrics.
    """
    if not isinstance(entry_zone, dict):
        return []

    try:
        low = float(entry_zone.get("low"))
        high = float(entry_zone.get("high"))
        center = float(entry_zone.get("center", (low + high) / 2.0))
    except Exception:
        return []

    if low <= 0 or high <= 0 or high <= low or steps <= 0:
        return []

    raw_zone = {"low": low, "high": high, "center": center, "width_pct": float(entry_zone.get("width_pct", 0.0) or 0.0)}
    ladder = split_entry_zone_into_ladder(raw_zone, side=side, steps=steps, weights=weights)

    for item in ladder:
        metrics = None
        if target is not None and stop is not None and account_size is not None and max_risk_pct is not None:
            metrics = calculate_trade_metrics(
                side=str(side).lower(),
                entry=float(item["price"]),
                stop=float(stop),
                target=float(target),
                account_size=float(account_size),
                max_risk_pct=float(max_risk_pct),
                coin_symbol=coin_symbol,
                entry_fee_pct=float(entry_fee_pct),
                exit_fee_pct=float(exit_fee_pct),
                short_borrow_hourly_pct=float(short_borrow_hourly_pct) if str(side).lower() == "short" else 0.0,
                expected_hold_hours=float(expected_hold_hours) if str(side).lower() == "short" else 0.0,
                short_liquidation_fee_pct=float(short_liquidation_fee_pct),
            )
        item["metrics"] = metrics
        item["rr"] = round(float(metrics.get("rr", 0.0)), 2) if isinstance(metrics, dict) else None
        item["risk_pct_price"] = round(float(metrics.get("risk_pct_price", 0.0)), 3) if isinstance(metrics, dict) else None
        item["reward_pct_price"] = round(float(metrics.get("reward_pct_price", 0.0)), 3) if isinstance(metrics, dict) else None

    return ladder
def build_scale_out_plan(
    side: str,
    entry_price: Optional[float],
    target_zone: Optional[Dict[str, object]],
    target_level: Optional[float] = None,
) -> List[Dict[str, object]]:
    """
    Simpele exit-planning:
    - TP1: eerste aanraking target-zone
    - TP2: midden target-zone
    - Runner: laatste stuk aan overkant target-zone
    """
    if entry_price is None:
        return []

    side_l = str(side).lower()
    zone = target_zone if isinstance(target_zone, dict) else None

    if zone is not None:
        try:
            low = float(zone.get("low"))
            center = float(zone.get("center"))
            high = float(zone.get("high"))
        except Exception:
            return []
        if low <= 0 or high <= 0 or high < low:
            return []

        if side_l == "long":
            prices = [low, center, high]
        else:
            prices = [high, center, low]
    elif target_level is not None:
        target = float(target_level)
        entry = float(entry_price)
        if side_l == "long":
            if target <= entry:
                return []
            prices = [entry + (target - entry) * 0.70, target, entry + (target - entry) * 1.10]
        else:
            if target >= entry:
                return []
            prices = [entry - (entry - target) * 0.70, target, entry - (entry - target) * 1.10]
    else:
        return []

    return [
        {"label": "TP1", "price": float(prices[0]), "sell_pct": 50.0, "intent": "eerste winst nemen"},
        {"label": "TP2", "price": float(prices[1]), "sell_pct": 30.0, "intent": "extra winst nemen"},
        {"label": "Runner", "price": float(prices[2]), "sell_pct": 20.0, "intent": "klein deel laten lopen"},
    ]


def build_portfolio_compound_plan_hint(
    metrics: Optional[Dict[str, float | str]],
    scale_out_plan: Optional[List[Dict[str, object]]] = None,
) -> Dict[str, object]:
    """
    Geen auto-compounding. Alleen een rustige hint:
    vaste risk-% houden en groei laten komen door winst/opbouw, niet door chasen.
    """
    if not isinstance(metrics, dict):
        return {"enabled": False, "hint": "Nog geen geldige metrics voor compound-hint."}

    rr = float(metrics.get("rr", 0.0) or 0.0)
    risk_pct_price = float(metrics.get("risk_pct_price", 0.0) or 0.0)
    reward_pct_price = float(metrics.get("reward_pct_price", 0.0) or 0.0)
    scale_count = len(scale_out_plan or [])

    if rr < 1.0:
        tone = "voorzichtig"
        hint = "RR is nog mager: eerst kwaliteit verbeteren voordat je groter denkt."
    elif scale_count >= 2:
        tone = "rustig opschalen"
        hint = "Gebruik vaste risk-% per trade; laat groei komen door winst vast te leggen via scale-outs."
    else:
        tone = "simpel"
        hint = "Houd risk-% gelijk. Compound pas door je accountbasis te laten groeien, niet door emotioneel groter in te zetten."

    return {
        "enabled": True,
        "tone": tone,
        "hint": hint,
        "rr": round(rr, 2),
        "risk_pct_price": round(risk_pct_price, 3),
        "reward_pct_price": round(reward_pct_price, 3),
    }

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






def _safe_float(value: Optional[object], default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)




# =========================================================
# Phase 5 - Two setup families only
# =========================================================
# V3.6 / Fase 5 - Confirmation Score Engine
# Confirmatie is vanaf nu een bonuslaag, geen poortwachter.
def score_confirmation_confluence(confirmation: Optional[Dict[str, object]]) -> float:
    if not isinstance(confirmation, dict):
        return 0.0
    score = 0.0
    if bool(confirmation.get("touched_zone", False)):
        score += 18.0
    if bool(confirmation.get("bullish_reject", False)) or bool(confirmation.get("bearish_reject", False)):
        score += 26.0
    if bool(confirmation.get("close_back_in_favor", False)):
        score += 22.0
    if bool(confirmation.get("reclaim_trigger_hit", False)):
        score += 24.0
    if bool(confirmation.get("volume_support", False)):
        score += 10.0
    if bool(confirmation.get("confirmed", False)):
        score = max(score, 70.0)
    return round(min(score, 100.0), 1)


def confirmation_score_label(score: float) -> str:
    score = float(score or 0.0)
    if score >= 70:
        return "sterk"
    if score >= 40:
        return "oké"
    if score > 0:
        return "zwak"
    return "geen"

# =========================================================
# V3.8 - V4.2 Volume Engine
# =========================================================
# Volume is een confluence-laag: het versterkt of verzwakt een bestaand plan,
# maar blokkeert nooit het vooraf gemaakte speelveld/plan.
def _zone_contains_price_or_candle(zone: Optional[Dict[str, float]], price_low: float, price_high: float) -> bool:
    z = _plain_zone(zone)
    if not z:
        return False
    return bool(price_low <= float(z["high"]) and price_high >= float(z["low"]))


def classify_volume_status(volume_ratio: float, last3_ratio: Optional[float] = None) -> str:
    ratio = float(volume_ratio or 0.0)
    last3 = float(last3_ratio) if last3_ratio is not None else ratio
    if ratio >= 1.80:
        return "spike"
    if ratio >= 1.20:
        return "hoog"
    if ratio <= 0.70 or last3 <= 0.75:
        return "laag / droogt op"
    return "normaal"


def build_volume_context_engine(
    df: Optional[pd.DataFrame],
    current_price: Optional[float],
    support_zone: Optional[Dict[str, float]],
    resistance_zone: Optional[Dict[str, float]],
    target_zone: Optional[Dict[str, float]] = None,
    active_side: Optional[str] = None,
) -> Dict[str, object]:
    """Volume context, zone-volume, volume-score en simpele trade-management hints."""
    result: Dict[str, object] = {
        "active": False,
        "status": "geen data",
        "volume_ratio": 0.0,
        "latest_volume": 0.0,
        "avg_volume": 0.0,
        "last3_ratio": 0.0,
        "supports": "neutraal",
        "long_score": 0.0,
        "short_score": 0.0,
        "reason": "Nog geen volume-data beschikbaar.",
        "support_touch": False,
        "resistance_touch": False,
        "bullish_rejection_volume": False,
        "bearish_rejection_volume": False,
        "breakout_up_with_volume": False,
        "breakout_down_with_volume": False,
        "weak_breakout_risk": False,
        "drying_up": False,
        "management_hint": "Geen open trade-management signaal.",
        "ui_label": "Volume: geen data",
    }

    if df is None or len(df) < 8 or "volume" not in df.columns:
        return result

    try:
        recent = df.tail(min(len(df), 30)).copy()
        latest = recent.iloc[-1]
        latest_volume = float(latest["volume"])
        baseline = recent.iloc[:-1]["volume"].tail(min(len(recent) - 1, 20))
        avg_volume = float(baseline.mean()) if len(baseline) else 0.0
        if avg_volume <= 0:
            return result

        volume_ratio = latest_volume / avg_volume
        last3_avg = float(recent["volume"].tail(min(len(recent), 3)).mean())
        prior_avg = float(recent["volume"].iloc[:-3].tail(min(max(len(recent) - 3, 1), 12)).mean()) if len(recent) > 4 else avg_volume
        last3_ratio = last3_avg / prior_avg if prior_avg > 0 else volume_ratio

        latest_open = float(latest["open"])
        latest_high = float(latest["high"])
        latest_low = float(latest["low"])
        latest_close = float(latest["close"])
        cp = float(current_price) if current_price is not None else latest_close

        status = classify_volume_status(volume_ratio, last3_ratio)
        high_volume = volume_ratio >= 1.20
        spike_volume = volume_ratio >= 1.80
        low_volume = volume_ratio <= 0.70
        drying_up = bool(last3_ratio <= 0.75 or low_volume)

        support_touch = _zone_contains_price_or_candle(support_zone, latest_low, latest_high)
        resistance_touch = _zone_contains_price_or_candle(resistance_zone, latest_low, latest_high)
        support_z = _plain_zone(support_zone)
        resistance_z = _plain_zone(resistance_zone)
        target_z = _plain_zone(target_zone)

        bullish_body = latest_close > latest_open
        bearish_body = latest_close < latest_open
        bullish_rejection = support_touch and bullish_body
        bearish_rejection = resistance_touch and bearish_body

        breakout_up = bool(resistance_z and latest_close > float(resistance_z["high"]))
        breakout_down = bool(support_z and latest_close < float(support_z["low"]))
        breakout_up_with_volume = breakout_up and high_volume
        breakout_down_with_volume = breakout_down and high_volume
        weak_breakout_risk = bool((breakout_up or breakout_down) and volume_ratio < 0.85)

        long_score = 0.0
        short_score = 0.0
        reasons: List[str] = []

        if bullish_rejection and high_volume:
            long_score += 28.0
            reasons.append("kopers reageren met volume op support")
        elif support_touch and low_volume:
            long_score -= 8.0
            reasons.append("support-touch met laag volume; bounce is minder overtuigend")
        elif support_touch:
            long_score += 8.0
            reasons.append("prijs reageert op support")

        if bearish_rejection and high_volume:
            short_score += 28.0
            reasons.append("verkopers reageren met volume op resistance")
        elif resistance_touch and low_volume:
            short_score -= 8.0
            reasons.append("resistance-touch met laag volume; rejection is minder overtuigend")
        elif resistance_touch:
            short_score += 8.0
            reasons.append("prijs reageert op resistance")

        if breakout_up_with_volume:
            long_score += 18.0
            short_score -= 8.0
            reasons.append("breakout omhoog wordt gedragen door volume")
        if breakout_down_with_volume:
            short_score += 18.0
            long_score -= 8.0
            reasons.append("breakdown omlaag wordt gedragen door volume")
        if weak_breakout_risk:
            long_score -= 10.0 if breakout_up else 0.0
            short_score -= 10.0 if breakout_down else 0.0
            reasons.append("uitbraak zonder volume; fakeout-risico")
        if spike_volume:
            if bullish_body:
                long_score += 8.0
            if bearish_body:
                short_score += 8.0
        if drying_up and not support_touch and not resistance_touch:
            long_score -= 4.0
            short_score -= 4.0

        supports = "neutraal"
        if long_score >= short_score + 8:
            supports = "LONG"
        elif short_score >= long_score + 8:
            supports = "SHORT"
        elif drying_up:
            supports = "wachten"

        management_hint = "Volume geeft geen extra management-actie."
        if target_z and cp is not None:
            near_target = distance_to_zone_pct(cp, target_z)
            if near_target is not None and near_target <= 0.20 and drying_up:
                management_hint = "Prijs zit dicht bij target en volume droogt op: denk aan gedeeltelijke TP / niet te hebberig."
            elif near_target is not None and near_target <= 0.20 and high_volume:
                management_hint = "Prijs nadert target met sterk volume: TP blijft logisch, maar momentum is nog aanwezig."
        if active_side and str(active_side).upper() == "LONG" and bearish_body and spike_volume:
            management_hint = "LONG actief? Tegenbeweging met volume-spike: oppassen / risico verlagen."
        if active_side and str(active_side).upper() == "SHORT" and bullish_body and spike_volume:
            management_hint = "SHORT actief? Tegenbeweging met volume-spike: oppassen / risico verlagen."

        reason = "; ".join(reasons[:3]) if reasons else "Volume is neutraal; geen extra bevestiging of waarschuwing."
        result.update({
            "active": True,
            "status": status,
            "volume_ratio": round(float(volume_ratio), 2),
            "latest_volume": latest_volume,
            "avg_volume": avg_volume,
            "last3_ratio": round(float(last3_ratio), 2),
            "supports": supports,
            "long_score": round(max(-30.0, min(60.0, long_score)), 1),
            "short_score": round(max(-30.0, min(60.0, short_score)), 1),
            "reason": reason,
            "support_touch": support_touch,
            "resistance_touch": resistance_touch,
            "bullish_rejection_volume": bool(bullish_rejection and high_volume),
            "bearish_rejection_volume": bool(bearish_rejection and high_volume),
            "breakout_up_with_volume": breakout_up_with_volume,
            "breakout_down_with_volume": breakout_down_with_volume,
            "weak_breakout_risk": weak_breakout_risk,
            "drying_up": drying_up,
            "management_hint": management_hint,
            "ui_label": f"Volume: {status} ({volume_ratio:.2f}x gem.)",
        })
        return result
    except Exception as exc:
        result["reason"] = f"Volume-engine kon niet rekenen: {exc}"
        return result


def apply_volume_confluence_to_candidates(candidates: List[Dict[str, object]], volume_engine: Optional[Dict[str, object]]) -> List[Dict[str, object]]:
    """Volume-score toevoegen zonder plan te blokkeren."""
    if not candidates or not isinstance(volume_engine, dict) or not volume_engine.get("active"):
        return candidates or []
    out: List[Dict[str, object]] = []
    for candidate in candidates:
        c = dict(candidate)
        side = str(c.get("side", "")).upper()
        vol_score = float(volume_engine.get("long_score" if side == "LONG" else "short_score", 0.0) or 0.0)
        score_delta = min(vol_score * 0.35, 18.0) if vol_score > 0 else max(vol_score * 0.50, -15.0)
        c["score"] = round(float(c.get("score", 0.0) or 0.0) + score_delta, 2)
        c["volume_score"] = round(vol_score, 1)
        c["volume_label"] = str(volume_engine.get("supports", "neutraal"))
        c["volume_reason"] = str(volume_engine.get("reason", ""))
        c["volume_as_bonus"] = True
        out.append(c)
    return sorted(out, key=lambda x: (bool(x.get("allowed_by_context", False)), _status_rank(x.get("status")), float(x.get("score", 0.0) or 0.0)), reverse=True)


def render_volume_panel(selected_result: Dict[str, object], compact: bool = True) -> None:
    """Compacte UI-laag voor volume, details zitten in expander."""
    volume = selected_result.get("volume_engine") or {}
    if not isinstance(volume, dict) or not volume.get("active"):
        st.caption("Volume: geen data")
        return

    if compact:
        st.markdown("**📊 Volume**")
    else:
        st.markdown("### 📊 Volume")

    v1, v2, v3, v4 = st.columns(4)
    v1.metric("Status", str(volume.get("status", "-")))
    v2.metric("Sterkte", f"{float(volume.get('volume_ratio', 0.0) or 0.0):.2f}x gem.")
    v3.metric("Ondersteunt", str(volume.get("supports", "neutraal")))
    active_side = str(selected_result.get("best_side") or selected_result.get("primary_side") or "-").upper()
    if active_side == "LONG":
        side_score = volume.get("long_score")
    elif active_side == "SHORT":
        side_score = volume.get("short_score")
    else:
        side_score = max(float(volume.get("long_score", 0.0)), float(volume.get("short_score", 0.0)))
    v4.metric("Volume-score", f"{float(side_score or 0.0):.0f}")
    st.caption(str(volume.get("reason", "")))

    with st.expander("Volume details", expanded=False):
        st.write(f"Laatste volume: **{float(volume.get('latest_volume', 0.0) or 0.0):,.2f}**")
        st.write(f"Gemiddeld volume: **{float(volume.get('avg_volume', 0.0) or 0.0):,.2f}**")
        st.write(f"Laatste 3 candles t.o.v. vorige candles: **{float(volume.get('last3_ratio', 0.0) or 0.0):.2f}x**")
        st.write(f"Support-touch: **{bool(volume.get('support_touch'))}**")
        st.write(f"Resistance-touch: **{bool(volume.get('resistance_touch'))}**")
        st.write(f"Fakeout-risico: **{bool(volume.get('weak_breakout_risk'))}**")
        st.write(f"Management: {volume.get('management_hint', '-')}")

SETUP_FAMILIES = {
    "early_price_action": {
        "label": "Early price-action",
        "legacy_names": {"doopiecash", "early", "early_pa", "early price-action", "early_price_action"},
    },
    "retest_breakout": {
        "label": "Retest-breakout",
        "legacy_names": {"confirmation", "confirmed", "retest", "breakout", "retest_breakout"},
    },
}


def choose_setup_family(raw_variant: Optional[str], plan: Optional[Dict[str, object]] = None) -> str:
    """
    Fase 5: elke setup wordt geforceerd naar exact één van twee families:
    - early_price_action
    - retest_breakout

    Oude namen zoals 'doopiecash' en 'confirmation' blijven alleen als interne legacy input bestaan,
    maar de output is altijd canoniek.
    """
    value = str(raw_variant or "").strip().lower().replace(" ", "_").replace("-", "_")

    if value in {"doopiecash", "early", "early_pa", "early_price_action", "earlypriceaction"}:
        return "early_price_action"
    if value in {"confirmation", "confirmed", "retest", "breakout", "retest_breakout", "retestbreakout"}:
        return "retest_breakout"

    if isinstance(plan, dict):
        status = str(plan.get("status") or "").upper()
        confirmation = plan.get("confirmation")
        trigger = plan.get("trigger")
        if confirmation is not None or trigger is not None or "CONFIRMED" in status:
            return "retest_breakout"
        if "DOOPIECASH" in status or plan.get("distance_to_entry_pct") is not None:
            return "early_price_action"

    return "early_price_action"


def detect_early_price_action_setup(
    side: str,
    plan: Optional[Dict[str, object]],
    fallback_metrics: Optional[Dict[str, float | str]],
    timing_label: str,
    location_info: Optional[Dict[str, object]],
    allowed_by_context: bool,
) -> Dict[str, object]:
    """Detectie-wrapper voor de early price-action family."""
    metrics = plan.get("metrics") if isinstance(plan, dict) else None
    metrics = metrics or fallback_metrics
    location_quality = str((location_info or {}).get("quality", "UNKNOWN"))
    plan_valid = bool(plan.get("valid", False)) if isinstance(plan, dict) else metrics is not None
    setup_ready = bool(metrics is not None and allowed_by_context and location_quality != "SKIP")
    return {
        "setup_family": "early_price_action",
        "setup_label": "Early price-action",
        "side": side.upper(),
        "detected": setup_ready,
        "valid": bool(plan_valid and setup_ready),
        "needs_confirmation": False,
        "timing_label": timing_label,
        "location_quality": location_quality,
        "reason": "Entry op vooraf bepaalde zone; geen candle-confirmatie nodig." if setup_ready else "Geen geldige early price-action setup.",
    }


def detect_retest_breakout_setup(
    side: str,
    plan: Optional[Dict[str, object]],
    fallback_metrics: Optional[Dict[str, float | str]],
    timing_label: str,
    location_info: Optional[Dict[str, object]],
    allowed_by_context: bool,
) -> Dict[str, object]:
    """Detectie-wrapper voor de retest-breakout family."""
    metrics = plan.get("metrics") if isinstance(plan, dict) else None
    metrics = metrics or fallback_metrics
    confirmation = plan.get("confirmation", {}) if isinstance(plan, dict) else {}
    confirmed = bool(confirmation.get("confirmed", False)) if isinstance(confirmation, dict) else False
    plan_valid = bool(plan.get("valid", False)) if isinstance(plan, dict) else False
    location_quality = str((location_info or {}).get("quality", "UNKNOWN"))
    setup_ready = bool(metrics is not None and allowed_by_context and location_quality != "SKIP")
    confirmation_score = score_confirmation_confluence(confirmation) if "score_confirmation_confluence" in globals() else (100.0 if confirmed else 0.0)
    return {
        "setup_family": "retest_breakout",
        "setup_label": "Retest-breakout",
        "side": side.upper(),
        "detected": setup_ready,
        "valid": bool(plan_valid and setup_ready),
        "needs_confirmation": False,
        "confirmation_as_bonus": True,
        "confirmed": confirmed,
        "confirmation_score": confirmation_score,
        "timing_label": timing_label,
        "location_quality": location_quality,
        "reason": str(confirmation.get("reason", "Confirmatie is extra confluence, geen blokkade.")) if isinstance(confirmation, dict) else "Confirmatie is extra confluence, geen blokkade.",
    }


def _build_mode_candidate(
    side: str,
    mode: str,
    variant: str,
    plan: Optional[Dict[str, object]],
    fallback_metrics: Optional[Dict[str, float | str]],
    fallback_target: Optional[float],
    timing_label: str,
    location_info: Optional[Dict[str, object]],
    allowed_by_context: bool,
    context_reason: str,
    combined_bias: str,
    market_context: str,
    taker_fee_pct: float,
    status_hint: str = "PLAN",
) -> Optional[Dict[str, object]]:
    variant = choose_setup_family(variant, plan)

    setup_detection = detect_early_price_action_setup(
        side=side,
        plan=plan,
        fallback_metrics=fallback_metrics,
        timing_label=timing_label,
        location_info=location_info,
        allowed_by_context=allowed_by_context,
    ) if variant == "early_price_action" else detect_retest_breakout_setup(
        side=side,
        plan=plan,
        fallback_metrics=fallback_metrics,
        timing_label=timing_label,
        location_info=location_info,
        allowed_by_context=allowed_by_context,
    )

    metrics = None
    if isinstance(plan, dict):
        metrics = plan.get("metrics")
    if metrics is None:
        metrics = fallback_metrics

    if metrics is None:
        return None

    target_value = fallback_target
    if isinstance(plan, dict) and plan.get("target") is not None:
        target_value = float(plan.get("target"))

    score = score_trade_candidate(
        side=side,
        metrics=metrics,
        timing_label=timing_label,
        combined_bias=combined_bias,
        market_context=market_context,
        taker_fee_pct=taker_fee_pct,
    )

    if mode == "plan":
        score += 4.0
        if timing_label == "READY":
            score += 4.0
        if timing_label == "NEAR":
            score += 2.0
    else:
        score += 8.0 if variant == "early_price_action" else 6.0

    location_quality = str((location_info or {}).get("quality", "UNKNOWN"))
    if location_quality == "B_ENTRY":
        score -= 3.0
    elif location_quality == "LATE":
        score -= 8.0
    elif location_quality == "SKIP":
        score -= 100.0

    plan_status = str(plan.get("status", "WAIT")) if isinstance(plan, dict) else "WAIT"
    plan_reason = str(plan.get("reason", "")) if isinstance(plan, dict) else ""
    plan_valid = bool(plan.get("valid", False)) if isinstance(plan, dict) else False

    confirmation_obj = plan.get("confirmation", {}) if isinstance(plan, dict) else {}
    confirmed = bool(confirmation_obj.get("confirmed", False)) if isinstance(confirmation_obj, dict) else False
    confirmation_score = score_confirmation_confluence(confirmation_obj) if "score_confirmation_confluence" in globals() else (100.0 if confirmed else 0.0)
    entry_zone = None
    target_zone = None
    if metrics:
        entry_zone = build_price_zone(metrics.get("entry"), get_coin_zone_width_pct(str(metrics.get("coin_symbol", "BTC")), None, "entry"))
        target_zone = build_price_zone(metrics.get("target"), get_coin_zone_width_pct(str(metrics.get("coin_symbol", "BTC")), None, "target"))

    trade_status = compute_trade_status(
        mode=mode,
        timing_label=timing_label,
        plan_valid=plan_valid,
        setup_family=variant,
        context_allowed=allowed_by_context,
        context_engine={},
        current_price=None,
        entry_zone=entry_zone,
        target_zone=target_zone,
        side=side,
        location_info=location_info,
        confirmed=confirmed,
    )
    status = str(trade_status.get("status", "WAIT"))
    reason = plan_reason or str(trade_status.get("reason", ""))

    if status in {"BLOCKED", "HANDS_OFF"}:
        score -= 200.0
        reason = context_reason or reason
    elif status == "MISSED":
        score -= 120.0
    elif status == "READY":
        score += 10.0
    elif status == "PLAN":
        score += 4.0

    # V3.6: prijsactie-confirmatie is extra confluence, nooit een harde gate.
    if variant == "retest_breakout":
        score += min(float(confirmation_score) * 0.22, 22.0)
    elif confirmation_score > 0:
        score += min(float(confirmation_score) * 0.10, 10.0)

    conservative_net = calculate_conservative_net_profit(metrics, taker_fee_pct)

    return {
        "side": side,
        "mode": mode,
        "variant": variant,
        "setup_family": variant,
        "setup_label": "Early price-action" if variant == "early_price_action" else "Retest-breakout",
        "setup_detection": setup_detection,
        "confirmation_score": confirmation_score,
        "confirmation_label": confirmation_score_label(confirmation_score) if "confirmation_score_label" in globals() else str(confirmation_score),
        "confirmation_as_bonus": True,
        "status": status,
        "timing_label": timing_label,
        "reason": reason,
        "score": round(float(score), 2),
        "metrics": metrics,
        "target": target_value,
        "location_quality": location_quality,
        "allowed_by_context": allowed_by_context,
        "conservative_net": conservative_net,
    }


def build_plan_mode_candidates(
    long_plan: Optional[Dict[str, object]],
    short_plan: Optional[Dict[str, object]],
    long_metrics: Optional[Dict[str, float | str]],
    short_metrics: Optional[Dict[str, float | str]],
    target_long: Optional[float],
    target_short: Optional[float],
    long_timing_label: str,
    short_timing_label: str,
    long_location: Optional[Dict[str, object]],
    short_location: Optional[Dict[str, object]],
    context_allow_long: bool,
    context_allow_short: bool,
    context_long_reason: str,
    context_short_reason: str,
    combined_bias: str,
    market_context: str,
    taker_fee_pct: float,
) -> List[Dict[str, object]]:
    candidates: List[Dict[str, object]] = []

    long_candidate = _build_mode_candidate(
        side="LONG",
        mode="plan",
        variant=choose_setup_family("early_price_action", long_plan),
        plan=long_plan,
        fallback_metrics=long_metrics,
        fallback_target=target_long,
        timing_label=long_timing_label,
        location_info=long_location,
        allowed_by_context=context_allow_long,
        context_reason=context_long_reason,
        combined_bias=combined_bias,
        market_context=market_context,
        taker_fee_pct=taker_fee_pct,
    )
    short_candidate = _build_mode_candidate(
        side="SHORT",
        mode="plan",
        variant=choose_setup_family("early_price_action", short_plan),
        plan=short_plan,
        fallback_metrics=short_metrics,
        fallback_target=target_short,
        timing_label=short_timing_label,
        location_info=short_location,
        allowed_by_context=context_allow_short,
        context_reason=context_short_reason,
        combined_bias=combined_bias,
        market_context=market_context,
        taker_fee_pct=taker_fee_pct,
    )

    if long_candidate is not None:
        candidates.append(long_candidate)
    if short_candidate is not None:
        candidates.append(short_candidate)

    return sorted(candidates, key=lambda x: (x["allowed_by_context"], x["score"]), reverse=True)


def build_entry_mode_candidates(
    entry_mode: str,
    long_doopiecash_plan: Optional[Dict[str, object]],
    short_doopiecash_plan: Optional[Dict[str, object]],
    long_confirmed_plan: Optional[Dict[str, object]],
    short_confirmed_plan: Optional[Dict[str, object]],
    long_metrics: Optional[Dict[str, float | str]],
    short_metrics: Optional[Dict[str, float | str]],
    target_long: Optional[float],
    target_short: Optional[float],
    long_timing_label: str,
    short_timing_label: str,
    long_location: Optional[Dict[str, object]],
    short_location: Optional[Dict[str, object]],
    context_allow_long: bool,
    context_allow_short: bool,
    context_long_reason: str,
    context_short_reason: str,
    combined_bias: str,
    market_context: str,
    taker_fee_pct: float,
) -> List[Dict[str, object]]:
    candidates: List[Dict[str, object]] = []

    # Fase 5: exact twee families, geen verborgen derde variant.
    candidate_specs = [
        ("LONG", choose_setup_family("early_price_action", long_doopiecash_plan), long_doopiecash_plan, long_metrics, target_long, long_timing_label, long_location, context_allow_long, context_long_reason),
        ("SHORT", choose_setup_family("early_price_action", short_doopiecash_plan), short_doopiecash_plan, short_metrics, target_short, short_timing_label, short_location, context_allow_short, context_short_reason),
        ("LONG", choose_setup_family("retest_breakout", long_confirmed_plan), long_confirmed_plan, long_metrics, target_long, long_timing_label, long_location, context_allow_long, context_long_reason),
        ("SHORT", choose_setup_family("retest_breakout", short_confirmed_plan), short_confirmed_plan, short_metrics, target_short, short_timing_label, short_location, context_allow_short, context_short_reason),
    ]

    for side, variant, plan, fallback_metrics, fallback_target, timing_label, location_info, allowed_by_context, context_reason in candidate_specs:
        candidate = _build_mode_candidate(
            side=side,
            mode="entry",
            variant=variant,
            plan=plan,
            fallback_metrics=fallback_metrics,
            fallback_target=fallback_target,
            timing_label=timing_label,
            location_info=location_info,
            allowed_by_context=allowed_by_context,
            context_reason=context_reason,
            combined_bias=combined_bias,
            market_context=market_context,
            taker_fee_pct=taker_fee_pct,
        )
        if candidate is not None:
            candidates.append(candidate)

    return sorted(
        candidates,
        key=lambda x: (x["status"] == "READY", x["allowed_by_context"], x["score"], x["variant"] == "early_price_action"),
        reverse=True,
    )



# =========================================================
# Phase 12 - Planner Ranking Engine
# =========================================================
def _candidate_safe_score(candidate: Optional[Dict[str, object]]) -> float:
    if not isinstance(candidate, dict):
        return -10_000.0
    try:
        return float(candidate.get("score", 0.0) or 0.0)
    except Exception:
        return 0.0


def _candidate_distance_to_entry_pct(candidate: Optional[Dict[str, object]]) -> float:
    if not isinstance(candidate, dict):
        return 999.0
    metrics = candidate.get("metrics") or {}
    if isinstance(metrics, dict):
        for key in ["distance_to_entry_pct", "distance_pct"]:
            if key in metrics and metrics.get(key) is not None:
                try:
                    return abs(float(metrics.get(key)))
                except Exception:
                    pass
    try:
        loc = str(candidate.get("location_quality", ""))
        return {"A_ENTRY": 0.15, "B_ENTRY": 0.45, "LATE": 1.5, "SKIP": 9.0}.get(loc, 2.0)
    except Exception:
        return 999.0


def _status_rank(status: Optional[str]) -> int:
    status = normalize_trader_status(status) if "normalize_trader_status" in globals() else str(status or "WAIT")
    return {
        "READY": 5,
        "PLAN": 4,
        "WAIT": 2,
        "SCALE_OUT": 4,
        "MISSED": 0,
        "HANDS_OFF": -2,
        "BLOCKED": -3,
    }.get(str(status), 1)


def rank_plan_candidates(candidates: List[Dict[str, object]]) -> List[Dict[str, object]]:
    """
    Rank vooraf-plannen los van directe entries.
    """
    ranked: List[Dict[str, object]] = []
    for c in candidates or []:
        item = dict(c)
        status = normalize_trader_status(item.get("status")) if "normalize_trader_status" in globals() else str(item.get("status", "WAIT"))
        allowed = bool(item.get("allowed_by_context", False))
        loc = str(item.get("location_quality", "UNKNOWN"))
        loc_bonus = {"A_ENTRY": 18.0, "B_ENTRY": 10.0, "UNKNOWN": 2.0, "LATE": -14.0, "SKIP": -80.0}.get(loc, 0.0)
        status_bonus = {"READY": 16.0, "PLAN": 22.0, "WAIT": 4.0, "MISSED": -80.0, "HANDS_OFF": -120.0, "BLOCKED": -120.0}.get(status, 0.0)
        allowed_bonus = 30.0 if allowed else -120.0
        distance_penalty = min(_candidate_distance_to_entry_pct(item) * 3.0, 18.0)
        planner_score = _candidate_safe_score(item) + allowed_bonus + loc_bonus + status_bonus - distance_penalty
        item["planner_rank_score"] = round(float(planner_score), 2)
        item["rank_bucket"] = "best_plan"
        ranked.append(item)
    return sorted(ranked, key=lambda x: (x.get("planner_rank_score", -9999), x.get("score", -9999)), reverse=True)


def rank_entry_candidates(candidates: List[Dict[str, object]]) -> List[Dict[str, object]]:
    """
    Rank alleen wat nu echt handelbaar is. READY staat boven PLAN/WAIT.
    """
    ranked: List[Dict[str, object]] = []
    for c in candidates or []:
        item = dict(c)
        status = normalize_trader_status(item.get("status")) if "normalize_trader_status" in globals() else str(item.get("status", "WAIT"))
        allowed = bool(item.get("allowed_by_context", False))
        confirmed_bonus = 10.0 if bool((item.get("setup_detection") or {}).get("confirmed", False)) else 0.0
        status_bonus = {"READY": 60.0, "SCALE_OUT": 18.0, "PLAN": 4.0, "WAIT": -15.0, "MISSED": -120.0, "HANDS_OFF": -150.0, "BLOCKED": -150.0}.get(status, -10.0)
        allowed_bonus = 25.0 if allowed else -150.0
        entry_score = _candidate_safe_score(item) + status_bonus + allowed_bonus + confirmed_bonus
        item["entry_rank_score"] = round(float(entry_score), 2)
        item["rank_bucket"] = "best_entry_now"
        ranked.append(item)
    return sorted(ranked, key=lambda x: (x.get("entry_rank_score", -9999), x.get("score", -9999)), reverse=True)


def rank_upcoming_zone_candidates(candidates: List[Dict[str, object]]) -> List[Dict[str, object]]:
    """
    Rank zones die interessant zijn om klaar te zetten, maar nog geen directe entry zijn.
    """
    ranked: List[Dict[str, object]] = []
    for c in candidates or []:
        item = dict(c)
        status = normalize_trader_status(item.get("status")) if "normalize_trader_status" in globals() else str(item.get("status", "WAIT"))
        allowed = bool(item.get("allowed_by_context", False))
        if status in {"MISSED", "HANDS_OFF", "BLOCKED"}:
            zone_score = -999.0
        else:
            distance = _candidate_distance_to_entry_pct(item)
            distance_bonus = max(0.0, 24.0 - min(distance, 8.0) * 3.0)
            status_bonus = {"PLAN": 30.0, "WAIT": 18.0, "READY": 8.0, "SCALE_OUT": 0.0}.get(status, 0.0)
            allowed_bonus = 25.0 if allowed else -80.0
            zone_score = (_candidate_safe_score(item) * 0.65) + status_bonus + allowed_bonus + distance_bonus
        item["upcoming_zone_rank_score"] = round(float(zone_score), 2)
        item["rank_bucket"] = "best_upcoming_zone"
        ranked.append(item)
    return sorted(ranked, key=lambda x: (x.get("upcoming_zone_rank_score", -9999), x.get("score", -9999)), reverse=True)


def build_planner_ranking_summary(
    plan_candidates: List[Dict[str, object]],
    entry_candidates: List[Dict[str, object]],
) -> Dict[str, object]:
    ranked_plans = rank_plan_candidates(plan_candidates)
    ranked_entries = rank_entry_candidates(entry_candidates)
    ranked_upcoming = rank_upcoming_zone_candidates(plan_candidates + entry_candidates)
    return {
        "ranked_plan_candidates": ranked_plans,
        "ranked_entry_candidates": ranked_entries,
        "ranked_upcoming_zone_candidates": ranked_upcoming,
        "best_plan_candidate": ranked_plans[0] if ranked_plans else None,
        "best_entry_now_candidate": ranked_entries[0] if ranked_entries else None,
        "best_upcoming_zone_candidate": ranked_upcoming[0] if ranked_upcoming else None,
    }

def classify_trade_opportunity(
    plan_candidates: List[Dict[str, object]],
    entry_candidates: List[Dict[str, object]],
    context_engine: Dict[str, object],
    context_preferred_side: Optional[str],
) -> Dict[str, object]:
    planner_ranking = build_planner_ranking_summary(plan_candidates, entry_candidates)
    best_plan = planner_ranking.get("best_plan_candidate")
    best_entry = planner_ranking.get("best_entry_now_candidate")
    best_upcoming = planner_ranking.get("best_upcoming_zone_candidate")
    market_state = str(context_engine.get("market_state", "range"))

    best_entry_status = normalize_trader_status(best_entry.get("status")) if isinstance(best_entry, dict) else "WAIT"
    best_plan_status = normalize_trader_status(best_plan.get("status")) if isinstance(best_plan, dict) else "WAIT"

    if best_entry is not None and best_entry_status == "HANDS_OFF":
        headline = "Hands off"
        status = "HANDS_OFF"
        primary_side = None
    elif best_entry is not None and best_entry_status == "MISSED":
        headline = "Setup gemist"
        status = "MISSED"
        primary_side = None
    elif best_entry is not None and best_entry_status == "READY" and bool(best_entry.get("allowed_by_context", False)):
        headline = f"Entry nu: {best_entry['side']}"
        status = "READY"
        primary_side = best_entry["side"]
    elif best_plan is not None and bool(best_plan.get("allowed_by_context", False)) and best_plan_status in {"PLAN", "READY", "WAIT"}:
        headline = f"Plan klaarzetten: {best_plan['side']}"
        status = "PLAN"
        primary_side = best_plan["side"]
    elif best_upcoming is not None and bool(best_upcoming.get("allowed_by_context", False)):
        headline = f"Upcoming zone: {best_upcoming.get('side', '-')}"
        status = "UPCOMING_ZONE"
        primary_side = best_upcoming.get("side")
    elif market_state == "hands_off":
        headline = "Hands off"
        status = "HANDS_OFF"
        primary_side = None
    elif market_state == "compressie":
        headline = "Wachten op breakout / retest"
        status = "WAIT_BREAKOUT"
        primary_side = None
    elif market_state == "choppy":
        headline = "Choppy / overslaan"
        status = "SKIP"
        primary_side = None
    else:
        headline = "Wachten"
        status = "WAIT"
        primary_side = context_preferred_side

    return {
        "headline": headline,
        "status": status,
        "primary_side": primary_side,
        "best_plan_candidate": best_plan,
        "best_entry_candidate": best_entry,
        "best_entry_now_candidate": best_entry,
        "best_upcoming_zone_candidate": best_upcoming,
        "ranked_plan_candidates": planner_ranking.get("ranked_plan_candidates", []),
        "ranked_entry_candidates": planner_ranking.get("ranked_entry_candidates", []),
        "ranked_upcoming_zone_candidates": planner_ranking.get("ranked_upcoming_zone_candidates", []),
        "has_plan_candidate": best_plan is not None,
        "has_entry_candidate": best_entry is not None,
        "has_upcoming_zone_candidate": best_upcoming is not None,
    }


def render_plan_vs_entry_sections(selected_result: Dict[str, object]) -> None:
    trade_opportunity = selected_result.get("trade_opportunity") or {}
    best_plan = trade_opportunity.get("best_plan_candidate")
    best_entry = trade_opportunity.get("best_entry_candidate")

    def _render_mode_card(title: str, candidate: Optional[Dict[str, object]], empty_text: str):
        st.markdown(f"**{title}**")
        if candidate is None:
            st.caption(empty_text)
            return

        metrics = candidate.get("metrics") or {}
        st.write(f"Side: **{candidate.get('side', '-')}**")
        st.write(f"Status: **{candidate.get('status', '-')}**")
        pre_plan = candidate.get("pre_trade_plan") or {}
        if isinstance(pre_plan, dict) and pre_plan.get("action"):
            st.write(f"Actie: **{pre_plan.get('action')}**")
            if pre_plan.get("entry_zone"):
                st.write(f"Entry-zone: **{fmt_zone(pre_plan.get('entry_zone'))}**")
            if pre_plan.get("target_zone"):
                st.write(f"Target-zone: **{fmt_zone(pre_plan.get('target_zone'))}**")
            if pre_plan.get("invalidation_zone"):
                st.write(f"Invalidatie-zone: **{fmt_zone(pre_plan.get('invalidation_zone'))}**")
        st.write(f"Variant: **{str(candidate.get('variant', '-')).replace('_', ' ')}**")
        if candidate.get("confirmation_as_bonus"):
            st.write(f"Confirmatie-score: **{_safe_float(candidate.get('confirmation_score')):.0f}/100** ({candidate.get('confirmation_label', 'geen')})")
        if metrics:
            st.write(f"Entry: **{fmt_price_eur(_safe_float(metrics.get('entry')))}**")
            st.write(f"Stop: **{fmt_price_eur(_safe_float(metrics.get('stop')))}**")
            st.write(f"Target: **{fmt_price_eur(_safe_float(metrics.get('target')))}**")
            st.write(f"RR: **1 : {_safe_float(metrics.get('rr')):.2f}**")

        zone_map_key = "long_trade_zone_map" if candidate.get("side") == "LONG" else "short_trade_zone_map"
        zone_map = selected_result.get(zone_map_key) or {}
        ladder = []
        if isinstance(pre_plan, dict):
            ladder = pre_plan.get("limit_order_ladder") or []
        if not ladder and isinstance(zone_map, dict):
            ladder = zone_map.get("limit_order_ladder") or zone_map.get("ladder", [])
        if ladder:
            st.markdown("**🎯 Entry ladder V3.5**")
            ladder_rows = []
            for item in ladder:
                ladder_rows.append({
                    "fill": str(item.get("label", "-")),
                    "prijs": fmt_price_eur(_safe_float(item.get("price"))),
                    "weging": f"{_safe_float(item.get('weight_pct')):.0f}%",
                    "fill-kans": str(item.get("fill_probability", "-")),
                    "RR": f"1 : {_safe_float(item.get('rr')):.2f}" if item.get("rr") is not None else "-",
                    "uitleg": str(item.get("description", "")),
                })
            st.dataframe(pd.DataFrame(ladder_rows), use_container_width=True, hide_index=True)
            st.caption("Top fill = eerste aanraking zone. Mid fill = midden zone. Deep fill = beste prijs, maar grotere kans op geen fill.")
        scale_out = zone_map.get("scale_out_plan", []) if isinstance(zone_map, dict) else []
        if scale_out:
            scale_text = " • ".join([
                f"{item.get('label')}: {fmt_price_eur(_safe_float(item.get('price')))} ({_safe_float(item.get('sell_pct')):.0f}%)"
                for item in scale_out
            ])
            st.caption(f"Scale-out: {scale_text}")

        compound_hint = zone_map.get("compound_hint", {}) if isinstance(zone_map, dict) else {}
        if isinstance(compound_hint, dict) and compound_hint.get("hint"):
            st.caption(f"Compound hint: {compound_hint.get('hint')}")
        st.caption(str(candidate.get("reason") or "-"))

    st.caption("V3.6: Plan Mode blijft leidend — candle-confirmatie is extra score/confluence, geen poortwachter.")
    c1, c2 = st.columns(2)
    with c1:
        _render_mode_card("🗺️ Plan Mode V3.4", best_plan, "Nog geen bruikbaar plan-level beschikbaar.")
    with c2:
        _render_mode_card("⚡ Entry Mode", best_entry, "Nog geen entry die nu handelbaar is.")


# =========================================================
# Phase 6 - Lower Timeframe Precision Engine
# =========================================================
LOWER_TF_PRECISION_TIMEFRAMES = {"1m", "5m", "15m"}


def is_lower_tf_precision_timeframe(timeframe_label: str) -> bool:
    return str(timeframe_label) in LOWER_TF_PRECISION_TIMEFRAMES


def _recent_atr_like_value(df: Optional[pd.DataFrame], lookback: int = 20) -> Optional[float]:
    if df is None or len(df) < 3:
        return None
    recent = df.tail(min(len(df), lookback)).copy()
    ranges = (recent["high"] - recent["low"]).abs()
    if ranges.empty:
        return None
    return float(ranges.mean())


def _select_nearest_level_in_pct_window(
    levels: List[float],
    reference_price: Optional[float],
    side: str,
    min_distance_pct: float,
    max_distance_pct: float,
) -> Optional[float]:
    if reference_price is None or not levels:
        return None
    ref = float(reference_price)
    if ref <= 0:
        return None

    valid: List[Tuple[float, float]] = []
    for level in levels:
        lvl = float(level)
        if side == "below" and lvl >= ref:
            continue
        if side == "above" and lvl <= ref:
            continue
        dist_pct = abs(lvl - ref) / ref * 100.0
        if dist_pct < min_distance_pct or dist_pct > max_distance_pct:
            continue
        valid.append((dist_pct, lvl))

    if not valid:
        return None
    valid.sort(key=lambda x: x[0])
    return float(valid[0][1])


def detect_micro_structure(
    df: Optional[pd.DataFrame],
    current_price: Optional[float] = None,
    lookback: int = 50,
    swing_window: int = 2,
    merge_threshold_pct: float = 0.10,
) -> Dict[str, object]:
    """
    Lower TF price-action structuur.
    Geen indicators: alleen lokale swing highs/lows, bodies en recente range.
    """
    empty = {
        "trade_supports": [],
        "trade_resistances": [],
        "hard_supports": [],
        "hard_resistances": [],
        "recent_low": None,
        "recent_high": None,
        "micro_bias": "neutral",
        "range_width_pct": None,
    }
    if df is None or len(df) < max(12, swing_window * 2 + 6):
        return empty

    recent = df.tail(min(len(df), lookback)).copy().reset_index(drop=True)
    if recent.empty:
        return empty

    ref = float(current_price) if current_price is not None else float(recent["close"].iloc[-1])
    highs = recent["high"].tolist()
    lows = recent["low"].tolist()
    body_highs = recent[["open", "close"]].max(axis=1).tolist()
    body_lows = recent[["open", "close"]].min(axis=1).tolist()

    swing_highs: List[float] = []
    swing_lows: List[float] = []
    swing_body_highs: List[float] = []
    swing_body_lows: List[float] = []

    for i in range(swing_window, len(recent) - swing_window):
        high_slice = highs[i - swing_window:i + swing_window + 1]
        low_slice = lows[i - swing_window:i + swing_window + 1]
        if highs[i] == max(high_slice):
            swing_highs.append(float(highs[i]))
            swing_body_highs.append(float(body_highs[i]))
        if lows[i] == min(low_slice):
            swing_lows.append(float(lows[i]))
            swing_body_lows.append(float(body_lows[i]))

    recent_low = float(recent["low"].min())
    recent_high = float(recent["high"].max())
    recent_open = float(recent["open"].iloc[0])
    recent_close = float(recent["close"].iloc[-1])

    hard_supports_all = _merge_levels(swing_lows + [recent_low], merge_threshold_pct, reverse_sort=True)
    hard_resistances_all = _merge_levels(swing_highs + [recent_high], merge_threshold_pct, reverse_sort=False)
    trade_supports_all = _merge_levels(swing_body_lows + swing_lows, merge_threshold_pct * 0.85, reverse_sort=True)
    trade_resistances_all = _merge_levels(swing_body_highs + swing_highs, merge_threshold_pct * 0.85, reverse_sort=False)

    range_width_pct = ((recent_high - recent_low) / max(abs(ref), 1e-9)) * 100.0
    if recent_close > recent_open * 1.001:
        micro_bias = "bullish"
    elif recent_close < recent_open * 0.999:
        micro_bias = "bearish"
    else:
        micro_bias = "neutral"

    return {
        "trade_supports": [lvl for lvl in trade_supports_all if lvl < ref],
        "trade_resistances": [lvl for lvl in trade_resistances_all if lvl > ref],
        "hard_supports": [lvl for lvl in hard_supports_all if lvl < ref],
        "hard_resistances": [lvl for lvl in hard_resistances_all if lvl > ref],
        "recent_low": recent_low,
        "recent_high": recent_high,
        "micro_bias": micro_bias,
        "range_width_pct": round(float(range_width_pct), 4),
    }


def build_lower_tf_precision_levels(
    df: Optional[pd.DataFrame],
    current_price: Optional[float],
    coin_symbol: str,
    timeframe_label: str,
    vol_profile: Optional[Dict[str, float | str]] = None,
) -> Dict[str, object]:
    """
    Voor 1m/5m/15m: gebruik lokale price-action levels voor exacte entry/SL/TP.
    HTF-context blijft leidend, maar de trade zelf wordt intraday-realistisch.
    """
    active = is_lower_tf_precision_timeframe(timeframe_label)
    result: Dict[str, object] = {
        "active": active,
        "mode_label": "Lower TF Precision" if active else "Swing logic",
        "style": "Scalping / Daytrade" if active else "Swing / HTF",
        "trade_support": None,
        "trade_resistance": None,
        "hard_support": None,
        "hard_resistance": None,
        "micro_structure": {},
        "atr_like": None,
        "reason": "Alleen actief op 1m/5m/15m.",
    }
    if not active or current_price is None or df is None or len(df) < 15:
        return result

    lookback = {"1m": 100, "5m": 90, "15m": 70}.get(str(timeframe_label), 60)
    zone_width = get_coin_zone_width_pct(coin_symbol, vol_profile, zone_kind="entry")
    # Dunnere merge bij rustige coins: voorkomt mega-zones die niet gevuld worden.
    merge_threshold_pct = max(0.025, min(0.11, zone_width * 0.45))
    micro = detect_micro_structure(
        df=df,
        current_price=current_price,
        lookback=lookback,
        swing_window=2,
        merge_threshold_pct=merge_threshold_pct,
    )

    cp = float(current_price)
    max_dist = {"1m": 0.38, "5m": 0.62, "15m": 0.95}.get(str(timeframe_label), 0.70)
    min_dist = 0.006 if coin_symbol in {"BTC", "ETH"} else 0.010

    trade_support = _select_nearest_level_in_pct_window(micro.get("trade_supports", []), cp, "below", min_dist, max_dist)
    trade_resistance = _select_nearest_level_in_pct_window(micro.get("trade_resistances", []), cp, "above", min_dist, max_dist)
    hard_support = _select_nearest_level_in_pct_window(micro.get("hard_supports", []), cp, "below", min_dist, max_dist * 1.35)
    hard_resistance = _select_nearest_level_in_pct_window(micro.get("hard_resistances", []), cp, "above", min_dist, max_dist * 1.35)

    # Fallback: als swings ontbreken, gebruik recente range-extremen, maar alleen als die niet absurd ver liggen.
    recent_low = micro.get("recent_low")
    recent_high = micro.get("recent_high")
    if trade_support is None and recent_low is not None and recent_low < cp:
        dist = abs(cp - float(recent_low)) / cp * 100.0
        if dist <= max_dist * 1.25:
            trade_support = float(recent_low)
    if trade_resistance is None and recent_high is not None and recent_high > cp:
        dist = abs(float(recent_high) - cp) / cp * 100.0
        if dist <= max_dist * 1.25:
            trade_resistance = float(recent_high)

    if hard_support is None:
        hard_support = trade_support
    if hard_resistance is None:
        hard_resistance = trade_resistance

    atr_like = _recent_atr_like_value(df, lookback=20)
    result.update({
        "trade_support": trade_support,
        "trade_resistance": trade_resistance,
        "hard_support": hard_support,
        "hard_resistance": hard_resistance,
        "micro_structure": micro,
        "atr_like": atr_like,
        "reason": "Lagere timeframe gebruikt lokale swings/range voor realistische daytrade-zones.",
    })
    return result


def select_intraday_target(
    side: str,
    reference_price: Optional[float],
    precision_levels: Optional[Dict[str, object]],
    fallback_level: Optional[float] = None,
    timeframe_label: str = "5m",
) -> Optional[float]:
    if reference_price is None:
        return fallback_level
    if not precision_levels or not precision_levels.get("active"):
        return fallback_level

    cp = float(reference_price)
    micro = precision_levels.get("micro_structure", {}) or {}
    min_dist = {"1m": 0.025, "5m": 0.045, "15m": 0.070}.get(str(timeframe_label), 0.06)
    max_dist = {"1m": 0.42, "5m": 0.70, "15m": 1.05}.get(str(timeframe_label), 0.80)

    if side.lower() == "long":
        candidates = list(micro.get("trade_resistances", [])) + list(micro.get("hard_resistances", []))
        target = _select_nearest_level_in_pct_window(candidates, cp, "above", min_dist, max_dist)
    else:
        candidates = list(micro.get("trade_supports", [])) + list(micro.get("hard_supports", []))
        target = _select_nearest_level_in_pct_window(candidates, cp, "below", min_dist, max_dist)

    if target is not None:
        return float(target)

    # Alleen fallback gebruiken als die niet belachelijk ver weg ligt voor scalping/daytrade.
    if fallback_level is not None:
        fallback = float(fallback_level)
        dist = abs(fallback - cp) / max(abs(cp), 1e-9) * 100.0
        if dist <= max_dist * 1.75:
            return fallback
    return None


def select_intraday_stop(
    side: str,
    entry_price: Optional[float],
    precision_levels: Optional[Dict[str, object]],
    fallback_stop: Optional[float] = None,
    timeframe_label: str = "5m",
    coin_symbol: str = "BTC",
) -> Optional[float]:
    if entry_price is None:
        return fallback_stop
    if not precision_levels or not precision_levels.get("active"):
        return fallback_stop

    entry = float(entry_price)
    atr_like = precision_levels.get("atr_like")
    atr_buffer = float(atr_like) * {"1m": 0.80, "5m": 0.95, "15m": 1.15}.get(str(timeframe_label), 1.0) if atr_like else entry * 0.0012
    min_buffer_pct = {"1m": 0.06, "5m": 0.10, "15m": 0.16}.get(str(timeframe_label), 0.10)
    min_buffer = entry * (min_buffer_pct / 100.0)
    buffer_value = max(atr_buffer, min_buffer)

    if side.lower() == "long":
        base = precision_levels.get("hard_support") or precision_levels.get("trade_support")
        if base is None:
            return fallback_stop
        return min(float(base) - buffer_value * 0.25, entry - buffer_value)

    base = precision_levels.get("hard_resistance") or precision_levels.get("trade_resistance")
    if base is None:
        return fallback_stop
    return max(float(base) + buffer_value * 0.25, entry + buffer_value)


def build_scalp_trade_plan(
    side: str,
    entry_level: Optional[float],
    current_price: Optional[float],
    precision_levels: Dict[str, object],
    fallback_target: Optional[float],
    fallback_stop: Optional[float],
    timeframe_label: str,
    coin_symbol: str,
) -> Dict[str, object]:
    """Compact plan-object voor UI/debug; metrics worden later door bestaande engine berekend."""
    target = select_intraday_target(side, entry_level or current_price, precision_levels, fallback_target, timeframe_label)
    stop = select_intraday_stop(side, entry_level or current_price, precision_levels, fallback_stop, timeframe_label, coin_symbol)
    return {
        "active": bool(precision_levels.get("active")),
        "style": "Scalping / Daytrade",
        "side": side.upper(),
        "entry": entry_level,
        "stop": stop,
        "target": target,
        "reason": "Lower TF precision gebruikt micro support/resistance + ATR-achtige stopruimte.",
    }

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
    entry_mode: str = "auto",
    shared_price_map: Optional[Dict[str, float]] = None,
) -> Dict[str, object]:
    # Safety defaults so early exits on 1m/5m never hit UnboundLocalError
    long_hard_pass = False
    short_hard_pass = False
    long_hard_reason = ""
    short_hard_reason = ""
    # Context / selection defaults
    context_engine = {
        "market_state": "range",
        "sub_state": "neutral",
        "label_main": "Range",
        "label_sub": "Neutral",
        "reason": "",
        "hands_off": False,
        "impulse_active": False,
        "impulse_side": "none",
        "compression_active": False,
        "choppy": False,
        "range_bound": False,
        "directional_efficiency": 0.0,
        "trigger_efficiency": 0.0,
        "trend_efficiency": 0.0,
        "compression_score": 0.0,
    }
    context_allow_long = True
    context_allow_short = True
    context_long_reason = "LONG toegestaan"
    context_short_reason = "SHORT toegestaan"
    context_preferred_side = None

    best_side = None
    best_metrics = None
    best_targets = None
    best_reason = ""
    chosen_entry_variant = None
    primary_side = None
    alternate_side = None
    alternate_metrics = None
    alternate_targets = None
    alternate_score = None
    alternate_entry_variant = None

    market = COINS[coin]["bitvavo_market"]

    snapshot_price_map = shared_price_map or {}
    live_price = snapshot_price_map.get(market)
    if live_price is None:
        live_price = get_live_price_for_market(market)
    current_price = override_price if override_price is not None else live_price

    hierarchy_packages = get_hierarchy_packages(market, timeframe_label, reference_price=current_price)
    trigger_pkg = hierarchy_packages["trigger"]
    setup_pkg = hierarchy_packages["setup"]
    trend_pkg = hierarchy_packages["trend"]

    refinement_timeframes = get_refinement_timeframes(timeframe_label)
    authority_timeframes = get_level_authority_timeframes(timeframe_label)

    authority_packages: Dict[str, Dict[str, object]] = {}
    for authority_tf in authority_timeframes:
        reused_pkg = None
        for pkg in hierarchy_packages.values():
            if str(pkg.get("label")) == authority_tf:
                reused_pkg = pkg
                break
        if reused_pkg is None:
            authority_df, authority_levels, authority_vol = get_timeframe_package(market, authority_tf, reference_price=current_price)
            authority_packages[authority_tf] = {
                "label": authority_tf,
                "df": authority_df,
                "levels": authority_levels,
                "vol": authority_vol,
            }
        else:
            authority_packages[authority_tf] = reused_pkg

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

    authority_levels = extract_higher_timeframe_levels(
        authority_packages,
        current_price,
        allowed_timeframes=authority_timeframes,
    )
    weighted_authority_levels = weight_levels_by_timeframe(authority_levels, current_price)
    primary_trade_zones = select_primary_trade_zones(
        weighted_authority_levels,
        current_price,
        refinement_timeframes=refinement_timeframes,
        base_timeframe_label=timeframe_label,
    )

    # Hoofdlevels komen uit 1d / 4h.
    # 1h / 15m mogen de trade-zone verfijnen voor daytrading.
    entry_trade_support = primary_trade_zones.get("trade_support")
    entry_trade_resistance = primary_trade_zones.get("trade_resistance")
    entry_hard_support = primary_trade_zones.get("hard_support")
    entry_hard_resistance = primary_trade_zones.get("hard_resistance")

    # Backup target-zones mogen nog wel uit een tweede HTF-level komen.
    higher_trade_support = primary_trade_zones.get("backup_trade_support")
    higher_trade_resistance = primary_trade_zones.get("backup_trade_resistance")
    higher_hard_support = entry_hard_support
    higher_hard_resistance = entry_hard_resistance

    # Trigger TF helpt alleen nog met timing, niet met hoofd-authority van levels.
    trigger_trade_support, trigger_trade_resistance, trigger_hard_support, trigger_hard_resistance = select_levels_around_price(entry_levels, current_price)

    # Fase 6: Lower TF Precision Engine.
    # Voor 1m/5m/15m blijven HTF-levels context geven, maar exacte entry/SL/TP komen uit lokale micro-structuur.
    lower_tf_precision = build_lower_tf_precision_levels(
        df=entry_df,
        current_price=current_price,
        coin_symbol=coin,
        timeframe_label=timeframe_label,
        vol_profile=entry_vol_profile,
    )
    lower_tf_precision_active = bool(lower_tf_precision.get("active") and (lower_tf_precision.get("trade_support") or lower_tf_precision.get("trade_resistance")))
    if lower_tf_precision_active:
        entry_trade_support = lower_tf_precision.get("trade_support") or entry_trade_support
        entry_trade_resistance = lower_tf_precision.get("trade_resistance") or entry_trade_resistance
        entry_hard_support = lower_tf_precision.get("hard_support") or entry_hard_support or entry_trade_support
        entry_hard_resistance = lower_tf_precision.get("hard_resistance") or entry_hard_resistance or entry_trade_resistance
        trigger_trade_support = entry_trade_support
        trigger_trade_resistance = entry_trade_resistance
        trigger_hard_support = entry_hard_support
        trigger_hard_resistance = entry_hard_resistance
        primary_trade_zones["trade_support"] = entry_trade_support
        primary_trade_zones["trade_resistance"] = entry_trade_resistance
        primary_trade_zones["hard_support"] = entry_hard_support
        primary_trade_zones["hard_resistance"] = entry_hard_resistance
        primary_trade_zones["lower_tf_precision_active"] = True
        primary_trade_zones["precision_source"] = timeframe_label
        primary_trade_zones["trade_support_meta"] = {"level": entry_trade_support, "timeframe": timeframe_label, "kind": "micro_trade_support", "score": 99.0}
        primary_trade_zones["trade_resistance_meta"] = {"level": entry_trade_resistance, "timeframe": timeframe_label, "kind": "micro_trade_resistance", "score": 99.0}
        primary_trade_zones["hard_support_meta"] = {"level": entry_hard_support, "timeframe": timeframe_label, "kind": "micro_hard_support", "score": 99.0}
        primary_trade_zones["hard_resistance_meta"] = {"level": entry_hard_resistance, "timeframe": timeframe_label, "kind": "micro_hard_resistance", "score": 99.0}

    auto_settings = get_auto_trade_settings(coin, entry_vol_profile)
    max_risico_pct = auto_settings["max_risk_pct"]
    entry_buffer_pct = auto_settings["entry_buffer_pct"]
    stop_buffer_pct = auto_settings["stop_buffer_pct"]
    rr_target = auto_settings["rr_target"]

    entry_zone_width_pct = get_coin_zone_width_pct(coin, entry_vol_profile, zone_kind="entry")
    target_zone_width_pct = get_coin_zone_width_pct(coin, entry_vol_profile, zone_kind="target")
    invalidation_zone_width_pct = get_coin_zone_width_pct(coin, entry_vol_profile, zone_kind="invalidation")
    if is_daytrade_timeframe(timeframe_label):
        # Intraday moet strak genoeg zijn om binnen uren/dagen bruikbaar te zijn.
        entry_zone_width_pct = round(max(0.025, entry_zone_width_pct * 0.65), 3)
        target_zone_width_pct = round(max(0.018, target_zone_width_pct * 0.60), 3)
        invalidation_zone_width_pct = round(max(0.022, invalidation_zone_width_pct * 0.60), 3)

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

    long_prelimit_zone = entry_trade_support
    short_prelimit_zone = entry_trade_resistance

    long_metrics = None
    short_metrics = None
    long_valid = False
    short_valid = False
    # Legacy limit-plan flow removed from active decisioning.
    long_limit_plan = None
    short_limit_plan = None
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
    long_entry_zone = short_entry_zone = None
    long_target_zone = short_target_zone = None
    long_invalidation_zone = short_invalidation_zone = None
    long_trade_zone_map = None
    short_trade_zone_map = None
    long_scalp_plan = {"active": False}
    short_scalp_plan = {"active": False}
    # Phase 12.5: zone flip / reclaim defaults
    long_zone_flip = {"active": False, "retest_state": "WAIT", "reason": ""}
    short_zone_flip = {"active": False, "retest_state": "WAIT", "reason": ""}
    long_flip_retest_plan = {"active": False}
    short_flip_retest_plan = {"active": False}
    zone_flip_active = False
    zone_flip_note = ""
    long_chase_risk = {"status": "OK", "chase_risk": False, "reason": ""}
    short_chase_risk = {"status": "OK", "chase_risk": False, "reason": ""}
    # lower_tf_precision is already built above. Do not reset it here.
    raw_setup_timing = compute_setup_timing(
        current_price,
        trigger_trade_support,
        trigger_trade_resistance,
        entry_vol_profile,
        structure_bias=combined_bias,
        coin_symbol=coin,
    )

    # Fase 4: context-engine is leidend. Eerst marktcontext, daarna pas timing/permissies.
    context_engine = build_market_context_engine(
        trigger_df=entry_df,
        setup_df=setup_df,
        trend_df=higher_df,
        trigger_structure=trigger_structure,
        setup_structure=setup_structure,
        trend_structure=trend_structure,
        trigger_vol_profile=entry_vol_profile,
        setup_vol_profile=setup_vol_profile,
        trend_vol_profile=higher_vol_profile,
        current_price=current_price,
        support=entry_trade_support,
        resistance=entry_trade_resistance,
    )
    setup_timing = apply_context_to_timing(raw_setup_timing, context_engine)
    context_permissions = derive_trade_permissions(
        context_engine=context_engine,
        current_price=current_price,
        support=entry_trade_support,
        resistance=entry_trade_resistance,
        long_timing_label=str(setup_timing.get("long_timing", "")),
        short_timing_label=str(setup_timing.get("short_timing", "")),
    )
    context_allow_long = bool(context_permissions.get("allow_long", True))
    context_allow_short = bool(context_permissions.get("allow_short", True))
    context_long_reason = str(context_permissions.get("long_reason", "LONG toegestaan"))
    context_short_reason = str(context_permissions.get("short_reason", "SHORT toegestaan"))
    context_preferred_side = context_permissions.get("preferred_side")

    # V3.3 Speelveld Engine: eerst speelveld bepalen, daarna pas timing/signalen.
    speelveld = build_speelveld_engine(
        current_price=current_price,
        support_level=entry_trade_support,
        resistance_level=entry_trade_resistance,
        coin_symbol=coin,
        timeframe_label=timeframe_label,
        vol_profile=entry_vol_profile,
        context_engine=context_engine,
        trend_label=trend_label,
        support_meta=primary_trade_zones.get("trade_support_meta"),
        resistance_meta=primary_trade_zones.get("trade_resistance_meta"),
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

        if lower_tf_precision_active:
            stop_long = select_intraday_stop(
                side="long",
                entry_price=entry_long,
                precision_levels=lower_tf_precision,
                fallback_stop=stop_long,
                timeframe_label=timeframe_label,
                coin_symbol=coin,
            ) or stop_long
            stop_short = select_intraday_stop(
                side="short",
                entry_price=entry_short,
                precision_levels=lower_tf_precision,
                fallback_stop=stop_short,
                timeframe_label=timeframe_label,
                coin_symbol=coin,
            ) or stop_short

        if target_mode == "Resistance/Support":
            # DoopieCash = eerstvolgende level op de entry timeframe.
            # Alleen als dat ontbreekt of te dicht is, fallback naar higher timeframe.
            target_selector = select_target_level_doopiecash

            target_long = target_selector(
                side="long",
                reference_price=entry_long,
                local_trade_level=entry_trade_resistance,
                higher_trade_level=higher_trade_resistance,
                min_distance_pct=MIN_DISTANCE_TO_TARGET_PCT,
            )
            target_short = target_selector(
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

        if lower_tf_precision_active:
            intraday_target_long = select_intraday_target(
                side="long",
                reference_price=entry_long,
                precision_levels=lower_tf_precision,
                fallback_level=target_long,
                timeframe_label=timeframe_label,
            )
            intraday_target_short = select_intraday_target(
                side="short",
                reference_price=entry_short,
                precision_levels=lower_tf_precision,
                fallback_level=target_short,
                timeframe_label=timeframe_label,
            )
            target_long = intraday_target_long if intraday_target_long is not None else target_long
            target_short = intraday_target_short if intraday_target_short is not None else target_short
            rr_target = min(float(rr_target), 1.8)

        # Fase 8: TP/SL komen nu eerst uit structuur. RR wordt pas daarna berekend.
        structural_target_long = select_structural_target_zone(
            side="long", entry_price=entry_long, current_price=current_price,
            primary_opposing_level=entry_trade_resistance, backup_opposing_level=higher_trade_resistance,
            precision_levels=lower_tf_precision, timeframe_label=timeframe_label, vol_profile=entry_vol_profile, fallback_level=target_long,
        )
        structural_target_short = select_structural_target_zone(
            side="short", entry_price=entry_short, current_price=current_price,
            primary_opposing_level=entry_trade_support, backup_opposing_level=higher_trade_support,
            precision_levels=lower_tf_precision, timeframe_label=timeframe_label, vol_profile=entry_vol_profile, fallback_level=target_short,
        )
        target_long = structural_target_long.get("level") if structural_target_long.get("level") is not None else target_long
        target_short = structural_target_short.get("level") if structural_target_short.get("level") is not None else target_short

        structural_stop_long = select_structural_invalidation(
            side="long", entry_price=entry_long, trade_level=entry_trade_support, hard_level=entry_hard_support,
            precision_levels=lower_tf_precision, vol_profile=entry_vol_profile, timeframe_label=timeframe_label, coin_symbol=coin, fallback_stop=stop_long,
        )
        structural_stop_short = select_structural_invalidation(
            side="short", entry_price=entry_short, trade_level=entry_trade_resistance, hard_level=entry_hard_resistance,
            precision_levels=lower_tf_precision, vol_profile=entry_vol_profile, timeframe_label=timeframe_label, coin_symbol=coin, fallback_stop=stop_short,
        )
        stop_long = structural_stop_long.get("level") if structural_stop_long.get("level") is not None else stop_long
        stop_short = structural_stop_short.get("level") if structural_stop_short.get("level") is not None else stop_short

        long_entry_zone = build_price_zone(entry_trade_support, entry_zone_width_pct)
        short_entry_zone = build_price_zone(entry_trade_resistance, entry_zone_width_pct)
        long_target_zone = build_price_zone(target_long, target_zone_width_pct) if target_long is not None else None
        short_target_zone = build_price_zone(target_short, target_zone_width_pct) if target_short is not None else None

        # Invalidation-zone moet de echte stoplogica volgen.
        # Eerst zat de zone rond de ruwe hard support/resistance, terwijl de echte stop
        # met stop_buffer_pct daar nog onder/boven lag. Daardoor leek de chart fout:
        # stoploss stond buiten of midden in de entryzone. De zone hoort dus rond de
        # daadwerkelijke buffered stop te liggen, niet rond het ongebufferde hard level.
        long_invalidation_zone = build_price_zone(stop_long, invalidation_zone_width_pct)
        short_invalidation_zone = build_price_zone(stop_short, invalidation_zone_width_pct)

        # Phase 12.5: Zone Flip / Reclaim Engine.
        # Als oude target/resistance geaccepteerd is, wordt die geen target meer maar mogelijke retest-entry.
        long_zone_flip = detect_flipped_zone(
            df=entry_df,
            zone=long_target_zone,
            direction="long",
            confirm_closes=2,
            retest_lookback=6,
        )
        short_zone_flip = detect_flipped_zone(
            df=entry_df,
            zone=short_target_zone,
            direction="short",
            confirm_closes=2,
            retest_lookback=6,
        )

        if bool(long_zone_flip.get("active")):
            long_flip_retest_plan = build_retest_entry_from_flipped_zone(
                side="long",
                flipped_zone_info=long_zone_flip,
                current_price=current_price,
                precision_levels=lower_tf_precision,
                fallback_target=higher_trade_resistance,
                fallback_stop=stop_long,
                timeframe_label=timeframe_label,
                coin_symbol=coin,
                vol_profile=entry_vol_profile,
            )
            if bool(long_flip_retest_plan.get("active")):
                entry_trade_support = float(long_flip_retest_plan.get("entry_level"))
                entry_hard_support = float((long_zone_flip.get("zone") or {}).get("low", entry_trade_support))
                entry_long = entry_trade_support
                stop_long = float(long_flip_retest_plan.get("stop")) if long_flip_retest_plan.get("stop") is not None else stop_long
                target_long = long_flip_retest_plan.get("target")
                # Oude target-zone is nu entry-zone; toon hem niet meer als TP.
                long_entry_zone = build_price_zone(entry_trade_support, entry_zone_width_pct)
                long_target_zone = build_price_zone(target_long, target_zone_width_pct) if target_long is not None else None
                long_invalidation_zone = build_price_zone(stop_long, invalidation_zone_width_pct)
                structural_target_long = {"level": target_long, "valid": target_long is not None, "source": "zone_flip_next_target", "score": {"score": 75.0 if target_long is not None else 0.0}, "reason": "Na zone flip is oude target entry geworden; TP is volgende logische zone."}
                structural_stop_long = {"level": stop_long, "valid": stop_long is not None, "source": "zone_flip_invalidation", "score": {"score": 72.0}, "reason": "SL voorbij geflipte support-zone."}
                setup_timing["long_timing"] = str(long_flip_retest_plan.get("status") or "PLAN")
                primary_trade_zones["trade_support"] = entry_trade_support
                primary_trade_zones["hard_support"] = entry_hard_support
                primary_trade_zones["trade_support_meta"] = {"level": entry_trade_support, "timeframe": timeframe_label, "kind": "flipped_support", "score": 120.0}
                primary_trade_zones["hard_support_meta"] = {"level": entry_hard_support, "timeframe": timeframe_label, "kind": "flipped_support_low", "score": 118.0}

        if bool(short_zone_flip.get("active")):
            short_flip_retest_plan = build_retest_entry_from_flipped_zone(
                side="short",
                flipped_zone_info=short_zone_flip,
                current_price=current_price,
                precision_levels=lower_tf_precision,
                fallback_target=higher_trade_support,
                fallback_stop=stop_short,
                timeframe_label=timeframe_label,
                coin_symbol=coin,
                vol_profile=entry_vol_profile,
            )
            if bool(short_flip_retest_plan.get("active")):
                entry_trade_resistance = float(short_flip_retest_plan.get("entry_level"))
                entry_hard_resistance = float((short_zone_flip.get("zone") or {}).get("high", entry_trade_resistance))
                entry_short = entry_trade_resistance
                stop_short = float(short_flip_retest_plan.get("stop")) if short_flip_retest_plan.get("stop") is not None else stop_short
                target_short = short_flip_retest_plan.get("target")
                # Oude target-zone is nu entry-zone; toon hem niet meer als TP.
                short_entry_zone = build_price_zone(entry_trade_resistance, entry_zone_width_pct)
                short_target_zone = build_price_zone(target_short, target_zone_width_pct) if target_short is not None else None
                short_invalidation_zone = build_price_zone(stop_short, invalidation_zone_width_pct)
                structural_target_short = {"level": target_short, "valid": target_short is not None, "source": "zone_flip_next_target", "score": {"score": 75.0 if target_short is not None else 0.0}, "reason": "Na zone flip is oude target entry geworden; TP is volgende logische zone."}
                structural_stop_short = {"level": stop_short, "valid": stop_short is not None, "source": "zone_flip_invalidation", "score": {"score": 72.0}, "reason": "SL voorbij geflipte resistance-zone."}
                setup_timing["short_timing"] = str(short_flip_retest_plan.get("status") or "PLAN")
                primary_trade_zones["trade_resistance"] = entry_trade_resistance
                primary_trade_zones["hard_resistance"] = entry_hard_resistance
                primary_trade_zones["trade_resistance_meta"] = {"level": entry_trade_resistance, "timeframe": timeframe_label, "kind": "flipped_resistance", "score": 120.0}
                primary_trade_zones["hard_resistance_meta"] = {"level": entry_hard_resistance, "timeframe": timeframe_label, "kind": "flipped_resistance_high", "score": 118.0}

        zone_flip_active = bool((long_zone_flip or {}).get("active") or (short_zone_flip or {}).get("active"))
        if bool((long_zone_flip or {}).get("active")):
            zone_flip_note = "Zone flip actief: oude target/resistance is nu mogelijke support voor retest-entry."
        elif bool((short_zone_flip or {}).get("active")):
            zone_flip_note = "Zone flip actief: oude support/target is nu mogelijke resistance voor retest-entry."

        long_trade_zone_map = build_trade_zone_map(
            side="long",
            entry_level=entry_trade_support,
            target_level=target_long,
            stop_level=stop_long,
            coin_symbol=coin,
            vol_profile=entry_vol_profile,
            entry_source_timeframe=str((primary_trade_zones.get("trade_support_meta") or {}).get("timeframe", setup_timeframe_label)) if isinstance(primary_trade_zones.get("trade_support_meta"), dict) else setup_timeframe_label,
            target_source_timeframe=str((primary_trade_zones.get("trade_resistance_meta") or {}).get("timeframe", setup_timeframe_label)) if isinstance(primary_trade_zones.get("trade_resistance_meta"), dict) else setup_timeframe_label,
            invalidation_source_timeframe=str((primary_trade_zones.get("hard_support_meta") or {}).get("timeframe", trend_timeframe_label)) if isinstance(primary_trade_zones.get("hard_support_meta"), dict) else trend_timeframe_label,
        )
        short_trade_zone_map = build_trade_zone_map(
            side="short",
            entry_level=entry_trade_resistance,
            target_level=target_short,
            stop_level=stop_short,
            coin_symbol=coin,
            vol_profile=entry_vol_profile,
            entry_source_timeframe=str((primary_trade_zones.get("trade_resistance_meta") or {}).get("timeframe", setup_timeframe_label)) if isinstance(primary_trade_zones.get("trade_resistance_meta"), dict) else setup_timeframe_label,
            target_source_timeframe=str((primary_trade_zones.get("trade_support_meta") or {}).get("timeframe", setup_timeframe_label)) if isinstance(primary_trade_zones.get("trade_support_meta"), dict) else setup_timeframe_label,
            invalidation_source_timeframe=str((primary_trade_zones.get("hard_resistance_meta") or {}).get("timeframe", trend_timeframe_label)) if isinstance(primary_trade_zones.get("hard_resistance_meta"), dict) else trend_timeframe_label,
        )
        long_scalp_plan = build_scalp_trade_plan("long", entry_long, current_price, lower_tf_precision, target_long, stop_long, timeframe_label, coin)
        short_scalp_plan = build_scalp_trade_plan("short", entry_short, current_price, lower_tf_precision, target_short, stop_short, timeframe_label, coin)

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

        if isinstance(long_metrics, dict):
            long_metrics["structural_target"] = structural_target_long
            long_metrics["structural_invalidation"] = structural_stop_long
            long_metrics["tp_realism_score"] = float((structural_target_long.get("score") or {}).get("score", 0.0))
            long_metrics["sl_breathing_score"] = float((structural_stop_long.get("score") or {}).get("score", 0.0))
        if isinstance(short_metrics, dict):
            short_metrics["structural_target"] = structural_target_short
            short_metrics["structural_invalidation"] = structural_stop_short
            short_metrics["tp_realism_score"] = float((structural_target_short.get("score") or {}).get("score", 0.0))
            short_metrics["sl_breathing_score"] = float((structural_stop_short.get("score") or {}).get("score", 0.0))

        # Fase 9: koppel compound-hint aan echte metrics nadat TP/SL/fees bekend zijn.
        if isinstance(long_trade_zone_map, dict):
            long_trade_zone_map["compound_hint"] = build_portfolio_compound_plan_hint(
                long_metrics,
                long_trade_zone_map.get("scale_out_plan", []),
            )
        if isinstance(short_trade_zone_map, dict):
            short_trade_zone_map["compound_hint"] = build_portfolio_compound_plan_hint(
                short_metrics,
                short_trade_zone_map.get("scale_out_plan", []),
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

        if target_long is None or not bool(structural_target_long.get("valid", False)):
            long_hard_pass = False
            long_hard_reason = str(structural_target_long.get("reason") or "Geen logisch opposing target boven prijs")
        if target_short is None or not bool(structural_target_short.get("valid", False)):
            short_hard_pass = False
            short_hard_reason = str(structural_target_short.get("reason") or "Geen logisch opposing target onder prijs")

        long_valid = long_valid and long_hard_pass
        short_valid = short_valid and short_hard_pass
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

        # Fase 7: Anti-chase gating. Geen entries midden in een explosieve move.
        long_chase_risk = detect_impulse_chase_risk(
            df=entry_df,
            current_price=current_price,
            entry_level=entry_trade_support,
            target_level=target_long,
            side="long",
            timeframe_label=timeframe_label,
            vol_profile=entry_vol_profile,
        )
        short_chase_risk = detect_impulse_chase_risk(
            df=entry_df,
            current_price=current_price,
            entry_level=entry_trade_resistance,
            target_level=target_short,
            side="short",
            timeframe_label=timeframe_label,
            vol_profile=entry_vol_profile,
        )

        long_doopiecash_plan = apply_anti_chase_to_plan(long_doopiecash_plan, long_chase_risk) or long_doopiecash_plan
        long_confirmed_plan = apply_anti_chase_to_plan(long_confirmed_plan, long_chase_risk) or long_confirmed_plan
        short_doopiecash_plan = apply_anti_chase_to_plan(short_doopiecash_plan, short_chase_risk) or short_doopiecash_plan
        short_confirmed_plan = apply_anti_chase_to_plan(short_confirmed_plan, short_chase_risk) or short_confirmed_plan

        if str(long_chase_risk.get("status")) in {"MISSED", "HANDS_OFF"}:
            long_valid = False
            long_hard_pass = False
            long_hard_reason = str(long_chase_risk.get("reason"))
            setup_timing["long_timing"] = str(long_chase_risk.get("status"))
        if str(short_chase_risk.get("status")) in {"MISSED", "HANDS_OFF"}:
            short_valid = False
            short_hard_pass = False
            short_hard_reason = str(short_chase_risk.get("reason"))
            setup_timing["short_timing"] = str(short_chase_risk.get("status"))

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

    if not context_allow_long:
        long_valid = False
        long_hard_pass = False
        long_hard_reason = context_long_reason
        if long_doopiecash_plan.get("status") != "WAIT" or long_doopiecash_plan.get("valid"):
            long_doopiecash_plan["status"] = "WAIT"
            long_doopiecash_plan["valid"] = False
            long_doopiecash_plan["reason"] = context_long_reason
        if long_confirmed_plan.get("status") != "WAIT" or long_confirmed_plan.get("valid"):
            long_confirmed_plan["status"] = "WAIT"
            long_confirmed_plan["valid"] = False
            long_confirmed_plan["reason"] = context_long_reason

    if not context_allow_short:
        short_valid = False
        short_hard_pass = False
        short_hard_reason = context_short_reason
        if short_doopiecash_plan.get("status") != "WAIT" or short_doopiecash_plan.get("valid"):
            short_doopiecash_plan["status"] = "WAIT"
            short_doopiecash_plan["valid"] = False
            short_doopiecash_plan["reason"] = context_short_reason
        if short_confirmed_plan.get("status") != "WAIT" or short_confirmed_plan.get("valid"):
            short_confirmed_plan["status"] = "WAIT"
            short_confirmed_plan["valid"] = False
            short_confirmed_plan["reason"] = context_short_reason

    plan_mode_candidates = build_plan_mode_candidates(
        long_plan=long_doopiecash_plan,
        short_plan=short_doopiecash_plan,
        long_metrics=long_metrics,
        short_metrics=short_metrics,
        target_long=target_long,
        target_short=target_short,
        long_timing_label=long_timing_label,
        short_timing_label=short_timing_label,
        long_location=long_location,
        short_location=short_location,
        context_allow_long=context_allow_long,
        context_allow_short=context_allow_short,
        context_long_reason=context_long_reason,
        context_short_reason=context_short_reason,
        combined_bias=combined_bias,
        market_context=market_context,
        taker_fee_pct=taker_fee_pct,
    )

    # V3.4: Plan Mode standaard.
    # Ook als prijs nog niet bij entry is, komt er een vooraf-plan vanuit het speelveld.
    plan_mode_candidates = add_standard_plan_mode_candidates(
        existing_candidates=plan_mode_candidates,
        speelveld=speelveld,
        coin_symbol=coin,
        timeframe_label=timeframe_label,
        current_price=current_price,
        vol_profile=entry_vol_profile,
        account_size=account_size,
        max_risk_pct=max_risico_pct,
        entry_fee_pct=entry_fee_pct,
        exit_fee_pct=exit_fee_pct,
        taker_fee_pct=taker_fee_pct,
        context_allow_long=context_allow_long,
        context_allow_short=context_allow_short,
        context_long_reason=context_long_reason,
        context_short_reason=context_short_reason,
        short_borrow_hourly_pct=short_borrow_hourly_pct,
        expected_hold_hours=expected_hold_hours,
        short_liquidation_fee_pct=short_liquidation_fee_pct,
    )

    # Phase 12.6: Pre-Plan Engine.
    # Lage timeframe: vooraf zone-plan tonen, niet pas na candle-reactie.
    plan_mode_candidates = show_plan_before_reaction(
        candidates=plan_mode_candidates,
        current_price=current_price,
        coin_symbol=coin,
        preferred_side=context_preferred_side,
        context_engine=context_engine,
    )

    entry_mode_candidates = build_entry_mode_candidates(
        entry_mode=entry_mode,
        long_doopiecash_plan=long_doopiecash_plan,
        short_doopiecash_plan=short_doopiecash_plan,
        long_confirmed_plan=long_confirmed_plan,
        short_confirmed_plan=short_confirmed_plan,
        long_metrics=long_metrics,
        short_metrics=short_metrics,
        target_long=target_long,
        target_short=target_short,
        long_timing_label=long_timing_label,
        short_timing_label=short_timing_label,
        long_location=long_location,
        short_location=short_location,
        context_allow_long=context_allow_long,
        context_allow_short=context_allow_short,
        context_long_reason=context_long_reason,
        context_short_reason=context_short_reason,
        combined_bias=combined_bias,
        market_context=market_context,
        taker_fee_pct=taker_fee_pct,
    )

    # Countertrend niet als hoofdverhaal tonen in trendcontext.
    entry_mode_candidates = [
        suppress_countertrend_plan(c, context_preferred_side, context_engine)
        for c in entry_mode_candidates
    ]

    # V3.8-V4.1: volume als confluence- en managementlaag.
    # Niet als poortwachter: plannen blijven zichtbaar, volume versterkt/verzwakt alleen de score.
    volume_engine = build_volume_context_engine(
        df=entry_df,
        current_price=current_price,
        support_zone=long_entry_zone or (speelveld or {}).get("support_zone"),
        resistance_zone=short_entry_zone or (speelveld or {}).get("resistance_zone"),
        target_zone=long_target_zone if context_preferred_side == "LONG" else short_target_zone if context_preferred_side == "SHORT" else None,
        active_side=context_preferred_side,
    )
    plan_mode_candidates = apply_volume_confluence_to_candidates(plan_mode_candidates, volume_engine)
    entry_mode_candidates = apply_volume_confluence_to_candidates(entry_mode_candidates, volume_engine)

    trade_opportunity = classify_trade_opportunity(
        plan_candidates=plan_mode_candidates,
        entry_candidates=entry_mode_candidates,
        context_engine=context_engine,
        context_preferred_side=context_preferred_side,
    )

    best_entry_candidate = trade_opportunity.get("best_entry_candidate")
    best_plan_candidate = trade_opportunity.get("best_plan_candidate")

    if best_entry_candidate is not None and str(best_entry_candidate.get("status")) == "READY":
        best_side = best_entry_candidate.get("side")
        best_metrics = best_entry_candidate.get("metrics")
        best_targets = best_entry_candidate.get("target")
        chosen_entry_variant = best_entry_candidate.get("variant")
        best_reason = str(best_entry_candidate.get("reason") or f"Beste {best_side} entry-mode setup.")
        primary_side = best_side
    else:
        primary_side = trade_opportunity.get("primary_side")
        if best_plan_candidate is not None:
            best_reason = str(best_plan_candidate.get("reason") or "Plan-mode heeft een bruikbaar level klaarstaan.")
        elif primary_side is not None:
            best_reason = f"Nog geen entry nu; bot wacht op {primary_side}."

    if len(entry_mode_candidates) > 1:
        alt_candidate = entry_mode_candidates[1]
        alternate_side = alt_candidate.get("side")
        alternate_metrics = alt_candidate.get("metrics")
        alternate_targets = alt_candidate.get("target")
        alternate_score = alt_candidate.get("score")
        alternate_entry_variant = alt_candidate.get("variant")
    else:
        alternate_side = None
        alternate_metrics = None
        alternate_targets = None
        alternate_score = None
        alternate_entry_variant = None

    conservative_best_net = calculate_conservative_net_profit(best_metrics, taker_fee_pct)
    score = calculate_setup_score(best_metrics, best_side, combined_bias, taker_fee_pct)

    context_state = str(context_engine.get("market_state", ""))
    if best_metrics is not None:
        longish = best_side == "LONG"
        timing_label = setup_timing["long_timing"] if longish else setup_timing["short_timing"]
        status = f"Kansrijk • {timing_label}"
    elif best_plan_candidate is not None and normalize_trader_status(best_plan_candidate.get("status")) == "READY":
        action = (best_plan_candidate.get("pre_trade_plan") or {}).get("action") or "Entry nu"
        status = f"{action} • {best_plan_candidate.get('side', '-')}"
    elif best_plan_candidate is not None and normalize_trader_status(best_plan_candidate.get("status")) == "PLAN":
        status = f"Plan actief • {best_plan_candidate.get('side', '-')}"
    elif context_state == "hands_off":
        status = "Hands off"
    elif context_state == "compressie":
        status = "Wacht op breakout/retest"
    elif context_state == "choppy":
        status = "Choppy / overslaan"
    elif context_state == "range" and str(context_engine.get("sub_state", "")) == "mid_range":
        status = "Range midden / wachten"
    elif primary_side == "LONG":
        status = f"Wachten op LONG • {setup_timing.get('long_timing', '-')}"
    elif primary_side == "SHORT":
        status = f"Wachten op SHORT • {setup_timing.get('short_timing', '-')}"
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
        "volume_engine": volume_engine,
        "volume_engine_active": bool((volume_engine or {}).get("active", False)),
        "volume_status": (volume_engine or {}).get("status"),
        "volume_supports": (volume_engine or {}).get("supports"),
        "entry_trade_support": entry_trade_support,
        "entry_trade_resistance": entry_trade_resistance,
        "trigger_trade_support": trigger_trade_support,
        "trigger_trade_resistance": trigger_trade_resistance,
        "entry_zone_width_pct": entry_zone_width_pct,
        "target_zone_width_pct": target_zone_width_pct,
        "invalidation_zone_width_pct": invalidation_zone_width_pct,
        "long_entry_zone": long_entry_zone,
        "short_entry_zone": short_entry_zone,
        "long_target_zone": long_target_zone,
        "short_target_zone": short_target_zone,
        "long_invalidation_zone": long_invalidation_zone,
        "short_invalidation_zone": short_invalidation_zone,
        "long_trade_zone_map": long_trade_zone_map,
        "short_trade_zone_map": short_trade_zone_map,
        "long_entry_ladder": (long_trade_zone_map or {}).get("limit_order_ladder", (long_trade_zone_map or {}).get("ladder", [])),
        "short_entry_ladder": (short_trade_zone_map or {}).get("limit_order_ladder", (short_trade_zone_map or {}).get("ladder", [])),
        "long_scale_out_plan": (long_trade_zone_map or {}).get("scale_out_plan", []),
        "short_scale_out_plan": (short_trade_zone_map or {}).get("scale_out_plan", []),
        "long_compound_hint": (long_trade_zone_map or {}).get("compound_hint", {}),
        "short_compound_hint": (short_trade_zone_map or {}).get("compound_hint", {}),
        "entry_hard_support": entry_hard_support,
        "entry_hard_resistance": entry_hard_resistance,
        "higher_trade_support": higher_trade_support,
        "higher_trade_resistance": higher_trade_resistance,
        "higher_hard_support": higher_hard_support,
                "higher_hard_resistance": higher_hard_resistance,
        "authority_levels": authority_levels,
        "authority_timeframes": authority_timeframes,
        "level_authority_note": "Daytrade: 15m/1h leidend, 4h macro-context, 1d niet gebruikt voor 15m-zones." if is_daytrade_timeframe(timeframe_label) else "Swing: hogere timeframes leidend.",
        "weighted_authority_levels": weighted_authority_levels,
        "primary_trade_zones": primary_trade_zones,
        "lower_tf_precision_active": lower_tf_precision_active,
        "lower_tf_precision": lower_tf_precision,
        "lower_tf_precision_label": "Lower TF Precision actief" if lower_tf_precision_active else "HTF/Swing levels",
        "trade_style_label": "Scalping / Daytrade mode" if lower_tf_precision_active else "Swing / HTF mode",
        "long_scalp_plan": long_scalp_plan,
        "short_scalp_plan": short_scalp_plan,
        "zone_flip_active": zone_flip_active,
        "zone_flip_note": zone_flip_note,
        "long_zone_flip": long_zone_flip,
        "short_zone_flip": short_zone_flip,
        "long_flip_retest_plan": long_flip_retest_plan,
        "short_flip_retest_plan": short_flip_retest_plan,

        "entry_bias": entry_bias,
        "higher_bias": higher_bias,
        "combined_bias": combined_bias,
        "market_context": market_context,
        "trend_label": trend_label,
        "context_engine": context_engine,
        "speelveld": speelveld,
        "speelveld_engine_active": True,
        "context_market_state": context_engine.get("market_state"),
        "context_sub_state": context_engine.get("sub_state"),
        "context_label_main": context_engine.get("label_main"),
        "context_label_sub": context_engine.get("label_sub"),
        "context_reason": context_engine.get("reason"),
        "context_hands_off": context_engine.get("hands_off"),
        "context_impulse_active": context_engine.get("impulse_active"),
        "context_impulse_side": context_engine.get("impulse_side"),
        "context_compression_active": context_engine.get("compression_active"),
        "context_choppy": context_engine.get("choppy"),
        "context_range_bound": context_engine.get("range_bound"),
        "context_directional_efficiency": context_engine.get("directional_efficiency"),
        "context_trigger_efficiency": context_engine.get("trigger_efficiency"),
        "context_trend_efficiency": context_engine.get("trend_efficiency"),
        "context_compression_score": context_engine.get("compression_score"),
        "context_priority": context_engine.get("context_priority"),
        "context_risk_state": context_engine.get("context_risk_state"),
        "context_action_bias": context_engine.get("context_action_bias"),
        "context_priority_reason": context_engine.get("context_priority_reason"),
        "context_allow_long": context_allow_long,
        "context_allow_short": context_allow_short,
        "context_long_reason": context_long_reason,
        "context_short_reason": context_short_reason,
        "context_preferred_side": context_preferred_side,
        "long_metrics": long_metrics,
        "short_metrics": short_metrics,
        "long_valid": long_valid,
        "short_valid": short_valid,
        "target_long": target_long,
        "target_short": target_short,
        "structural_target_long": structural_target_long if "structural_target_long" in locals() else None,
        "structural_target_short": structural_target_short if "structural_target_short" in locals() else None,
        "structural_stop_long": structural_stop_long if "structural_stop_long" in locals() else None,
        "structural_stop_short": structural_stop_short if "structural_stop_short" in locals() else None,
        "long_location": long_location,
        "short_location": short_location,
        "long_doopiecash_plan": long_doopiecash_plan,
        "short_doopiecash_plan": short_doopiecash_plan,
        "long_confirmed_plan": long_confirmed_plan,
        "short_confirmed_plan": short_confirmed_plan,
        "plan_mode_candidates": plan_mode_candidates,
        "entry_mode_candidates": entry_mode_candidates,
        "trade_opportunity": trade_opportunity,
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
        "chosen_entry_variant": choose_setup_family(chosen_entry_variant) if chosen_entry_variant else None,
        "active_setup_families": ["early_price_action", "retest_breakout"],
        "anti_chase_engine_active": True,
        "long_chase_risk": long_chase_risk,
        "short_chase_risk": short_chase_risk,
        "trader_status_engine_active": True,
        "trader_statuses": ["PLAN", "READY", "WAIT", "HANDS_OFF", "MISSED", "SCALE_OUT"],
        "setup_family_rule": "Elke setup valt in exact één van deze families: early_price_action of retest_breakout.",
        "primary_side": primary_side,
        "alternate_side": alternate_side,
        "alternate_metrics": alternate_metrics,
        "alternate_targets": alternate_targets,
        "alternate_score": alternate_score,
        "alternate_entry_variant": alternate_entry_variant,
        "raw_setup_timing": raw_setup_timing,
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
    st.session_state.entry_fee_type = "maker"  # vaste default
if "exit_fee_type" not in st.session_state:
    st.session_state.exit_fee_type = "taker"  # vaste default
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

if "live_price_last_change_ts" not in st.session_state:
    st.session_state.live_price_last_change_ts = time.time()
if "live_price_prev_map" not in st.session_state:
    st.session_state.live_price_prev_map = {}
if "last_good_price_map" not in st.session_state:
    st.session_state.last_good_price_map = {}
if "last_price_source" not in st.session_state:
    st.session_state.last_price_source = "unknown"
if "last_price_fetch_ts" not in st.session_state:
    st.session_state.last_price_fetch_ts = 0.0
if "scanner_results" not in st.session_state:
    st.session_state.scanner_results = []
if "scanner_signature" not in st.session_state:
    st.session_state.scanner_signature = None
if "scanner_last_updated" not in st.session_state:
    st.session_state.scanner_last_updated = None
if "scanner_last_scan_epoch" not in st.session_state:
    st.session_state.scanner_last_scan_epoch = 0.0
if "shared_market_snapshot" not in st.session_state:
    st.session_state.shared_market_snapshot = {"created_at": 0.0, "price_map": {}, "price_source": "unknown", "fetch_ts": 0.0}
if "bf_view_mode" not in st.session_state:
    st.session_state["bf_view_mode"] = "desktop"

# =========================================================
# CSS
# =========================================================
st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.0rem;
        padding-bottom: 1.8rem;
        max-width: 1480px;
    }
    div[data-testid="stMetric"] {
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 18px;
        padding: 12px 14px;
    }
    .bf-card {
        background: linear-gradient(180deg, rgba(255,255,255,0.045), rgba(255,255,255,0.02));
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 20px;
        padding: 18px;
        min-height: 210px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.18);
    }
    .bf-card-title {
        font-size: 0.82rem;
        color: #9CA3AF;
        margin-bottom: 8px;
        letter-spacing: 0.2px;
        text-transform: uppercase;
    }
    .bf-card-coin {
        font-size: 1.7rem;
        font-weight: 800;
        margin-bottom: 8px;
    }
    .bf-card-side {
        display: inline-block;
        padding: 4px 11px;
        border-radius: 999px;
        font-size: 0.78rem;
        font-weight: 800;
        margin-bottom: 14px;
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
        font-size: 0.96rem;
        line-height: 1.45;
        margin-top: 7px;
    }
    .bf-scan-card {
        background: linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.018));
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 18px;
        padding: 16px;
        min-height: 210px;
        box-shadow: 0 10px 24px rgba(0,0,0,0.16);
    }
    .bf-scan-coin {
        font-size: 1.55rem;
        font-weight: 800;
        margin-bottom: 10px;
    }
    .bf-scan-status {
        display: inline-block;
        padding: 5px 10px;
        border-radius: 999px;
        font-size: 0.82rem;
        font-weight: 800;
        margin-bottom: 12px;
    }
    .bf-scan-status.long_ready, .bf-scan-status.short_ready {
        background: rgba(34,197,94,0.18);
        color: #86EFAC;
    }
    .bf-scan-status.long_near, .bf-scan-status.short_near {
        background: rgba(234,179,8,0.18);
        color: #FDE68A;
    }
    .bf-scan-status.long_wait, .bf-scan-status.short_wait, .bf-scan-status.wait, .bf-scan-status.no_trade {
        background: rgba(148,163,184,0.18);
        color: #CBD5E1;
    }
    .bf-scan-status.blocked, .bf-scan-status.hands_off, .bf-scan-status.missed {
        background: rgba(239,68,68,0.18);
        color: #FCA5A5;
    }
    .bf-scan-setup {
        font-size: 0.9rem;
        color: #9CA3AF;
        margin-bottom: 12px;
    }
    .bf-scan-line {
        font-size: 0.95rem;
        color: #F3F4F6;
        margin-top: 6px;
        line-height: 1.35;
    }
    .bf-scan-reason {
        font-size: 0.85rem;
        color: #9CA3AF;
        margin-top: 12px;
        line-height: 1.35;
    }

    .bf-mobile-card { background: linear-gradient(180deg, rgba(255,255,255,0.055), rgba(255,255,255,0.025)); border: 1px solid rgba(255,255,255,0.10); border-radius: 18px; padding: 16px; margin: 10px 0; box-shadow: 0 8px 24px rgba(0,0,0,0.16); }
    .bf-mobile-title { font-size: 1.05rem; font-weight: 900; margin-bottom: 8px; color: #F8FAFC; }
    .bf-mobile-pill { display: inline-block; padding: 6px 12px; border-radius: 999px; font-weight: 900; font-size: 0.86rem; margin: 2px 4px 8px 0; background: rgba(148,163,184,0.18); color: #E5E7EB; }
    .bf-mobile-pill.long { background: rgba(34,197,94,0.18); color:#86EFAC; }
    .bf-mobile-pill.short { background: rgba(239,68,68,0.18); color:#FCA5A5; }
    .bf-mobile-pill.wait { background: rgba(234,179,8,0.18); color:#FDE68A; }
    .bf-mobile-line { font-size: 0.96rem; line-height: 1.45; margin: 6px 0; color: #E5E7EB; }
    .bf-mobile-muted { color: #AAB4C3; font-size: 0.88rem; line-height: 1.4; }
    @media (max-width: 760px) { .block-container { padding-left: 0.75rem; padding-right: 0.75rem; padding-top: 0.6rem; max-width: 100%; } div[data-testid="stMetric"] { padding: 10px 12px; border-radius: 14px; } .bf-scan-card, .bf-card { min-height: auto; padding: 14px; } .stButton > button { min-height: 44px; border-radius: 14px; font-weight: 800; } }
    </style>
    """,
    unsafe_allow_html=True,
)

# =========================================================
# Scanner universe
# =========================================================
# Bewust beperkt tot de 5 focusmunten voor validatie.

# =========================================================
# Weergave switch
# =========================================================
view_options = ["🖥️ Desktop weergave", "📱 Mobiele weergave"]
current_view_index = 1 if st.session_state.get("bf_view_mode") == "mobile" else 0
selected_view_label = st.radio("Weergave", view_options, index=current_view_index, horizontal=True, label_visibility="collapsed", key="bf_view_mode_radio")
st.session_state["bf_view_mode"] = "mobile" if selected_view_label.startswith("📱") else "desktop"

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
            st.caption("Detectie: Multi-timeframe marktstructuur")
            st.info("Winstdoel staat vast op: eerstvolgend logisch level")
            st.caption("Plan Mode en Entry Mode gebruiken nu automatisch zowel early price-action als retest-breakout waar logisch.")
        with s2:
            st.session_state.maker_fee_pct = st.number_input(
                "Maker fee (%)",
                min_value=0.0,
                value=float(st.session_state.maker_fee_pct),
                step=0.01,
                format="%.2f",
            )
            st.session_state.taker_fee_pct = st.number_input(
                "Taker fee (%)",
                min_value=0.0,
                value=float(st.session_state.taker_fee_pct),
                step=0.01,
                format="%.2f",
            )
        with s3:
            st.caption("Orders staan vast voor eenvoud")
            st.write("Entry order: **maker**")
            st.write("Exit order: **taker**")
            st.caption("Geavanceerde instellingen hieronder")

        with st.expander("Geavanceerd: sizing / short-kosten / test", expanded=False):
            g1, g2 = st.columns(2)
            with g1:
                st.session_state.account_size = st.number_input(
                    "Account grootte (€)",
                    min_value=1.0,
                    value=float(st.session_state.account_size),
                    step=50.0,
                )
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
            with g2:
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
                st.caption("Pas hier per coin de geschatte leenkosten aan. Alleen voor SHORTs.")

account_size = float(st.session_state.account_size)
target_mode = "Resistance/Support"
min_profit_buffer_eur = 0.0
entry_mode = "auto"
maker_fee_pct = float(st.session_state.maker_fee_pct)
taker_fee_pct = float(st.session_state.taker_fee_pct)
entry_fee_type = "maker"
exit_fee_type = "taker"
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


@bf_fragment(run_every=f"{REFRESH_UI_SEC}s")
def render_live_price_bar():
    snapshot = build_shared_market_snapshot(force_refresh=True)
    live_price_map = snapshot.get("price_map", {}) or {}

    prev_map = st.session_state.get("live_price_prev_map", {}) or {}
    changed = any(prev_map.get(market) != price for market, price in live_price_map.items() if market in prev_map)
    if changed or (live_price_map and not prev_map):
        st.session_state.live_price_last_change_ts = time.time()
    if live_price_map:
        st.session_state.live_price_prev_map = live_price_map.copy()

    st.markdown("### Live prijzen")
    cols = st.columns(len(COINS))
    scanner_price_map = {f"{coin}-EUR": result.get("current_price") for coin, result in [(r.get("coin"), r) for r in st.session_state.get("scanner_results", [])] if coin}

    for idx, (coin_symbol, meta) in enumerate(COINS.items()):
        market = meta["bitvavo_market"]
        live_price = live_price_map.get(market)
        scan_price = scanner_price_map.get(market)
        with cols[idx]:
            if live_price is None:
                fallback_price = (st.session_state.get("last_good_price_map", {}) or {}).get(market)
                if fallback_price is None:
                    st.markdown(f"**{coin_symbol}**\n\nGeen live prijs")
                    continue
                live_price = fallback_price
            delta_text = None
            if scan_price is not None and float(scan_price) != 0:
                delta_pct = ((float(live_price) - float(scan_price)) / float(scan_price)) * 100
                delta_text = f"{delta_pct:+.2f}% vs scan"
            st.metric(coin_symbol, fmt_price_eur(float(live_price)), delta_text)

    source = st.session_state.get("last_price_source", "unknown")
    seconds_since_change = int(max(0, time.time() - float(st.session_state.get("live_price_last_change_ts", time.time()))))
    last_fetch_ts = float(st.session_state.get("last_price_fetch_ts", 0.0) or 0.0)
    seconds_since_fetch = int(max(0, time.time() - last_fetch_ts)) if last_fetch_ts else 0

    if not live_price_map and not (st.session_state.get("last_good_price_map", {}) or {}):
        st.warning("⚠️ Live prijzen tijdelijk niet beschikbaar.")
    elif source == "fallback":
        st.caption(f"Live refresh ± {REFRESH_UI_SEC}s • fallback actief • scanner blijft rustig staan • laatste prijswijziging {seconds_since_change}s geleden")
    elif source == "cached":
        st.caption(f"Live refresh ± {REFRESH_UI_SEC}s • cached prijzen gebruikt • laatste fetch {seconds_since_fetch}s geleden • laatste prijswijziging {seconds_since_change}s geleden")
    else:
        st.caption(f"Live refresh ± {REFRESH_UI_SEC}s • scanner blijft rustig staan • laatste prijswijziging {seconds_since_change}s geleden")

render_live_price_bar()

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

def run_scanner_scan(show_spinner: bool = False) -> None:
    def _scan():
        shared_snapshot = get_shared_market_snapshot(force_refresh=False)
        shared_price_map = shared_snapshot.get("price_map", {}) or {}
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
                shared_price_map=shared_price_map,
            )
            for coin_symbol in COINS.keys()
        ]
        st.session_state.scanner_signature = current_scan_signature
        st.session_state.scanner_last_updated = pd.Timestamp.now().strftime("%H:%M:%S")
        st.session_state.scanner_last_scan_epoch = time.time()

    if show_spinner:
        with st.spinner("BullForge scant de beste kansen..."):
            _scan()
    else:
        _scan()

def _result_planner_scores(result: Dict[str, object]) -> Dict[str, float]:
    trade_opportunity = result.get("trade_opportunity") or {}
    plan = trade_opportunity.get("best_plan_candidate") or {}
    entry = trade_opportunity.get("best_entry_now_candidate") or trade_opportunity.get("best_entry_candidate") or {}
    upcoming = trade_opportunity.get("best_upcoming_zone_candidate") or {}
    return {
        "plan_rank_score": float(plan.get("planner_rank_score", -999.0) or -999.0) if isinstance(plan, dict) else -999.0,
        "entry_rank_score": float(entry.get("entry_rank_score", -999.0) or -999.0) if isinstance(entry, dict) else -999.0,
        "upcoming_zone_rank_score": float(upcoming.get("upcoming_zone_rank_score", -999.0) or -999.0) if isinstance(upcoming, dict) else -999.0,
    }


def compute_ranked_results(scanner_results: List[Dict[str, object]]):
    """
    Phase 12:
    Scanner ranking denkt nu in drie losse buckets:
    - beste entry nu
    - beste plan
    - beste upcoming zone

    Voor backwards compatibility geeft deze functie nog steeds dezelfde 3 lijsten terug,
    maar ieder result krijgt extra rank-scores mee.
    """
    grade_rank = {"GOOD": 3, "OK": 2, "WEAK": 1, "NO DATA": 0}
    enriched: List[Dict[str, object]] = []
    for result in scanner_results or []:
        item = dict(result)
        item.update(_result_planner_scores(item))
        item["planner_overall_rank_score"] = max(
            item.get("entry_rank_score", -999.0) + 12.0,
            item.get("plan_rank_score", -999.0) + 4.0,
            item.get("upcoming_zone_rank_score", -999.0),
            float(item.get("score", 0.0) or 0.0),
        )
        enriched.append(item)

    ranked_results = sorted(
        enriched,
        key=lambda x: (
            x.get("entry_rank_score", -999.0),
            x.get("plan_rank_score", -999.0),
            x.get("upcoming_zone_rank_score", -999.0),
            grade_rank.get(x.get("setup_grade", "NO DATA"), 0),
            x.get("planner_overall_rank_score", -999.0),
            x.get("conservative_best_net") or -999,
        ),
        reverse=True,
    )
    valid_results = [
        result for result in ranked_results
        if result.get("best_metrics") is not None
        or (result.get("trade_opportunity") or {}).get("best_plan_candidate") is not None
        or (result.get("trade_opportunity") or {}).get("best_upcoming_zone_candidate") is not None
    ]
    visible_results = [result for result in ranked_results if result.get("setup_grade") in {"GOOD", "OK", "WEAK"}]
    return ranked_results, valid_results, visible_results


should_scan = (
    scan_now
    or not st.session_state.scanner_results
    or st.session_state.scanner_signature != current_scan_signature
)

if should_scan:
    run_scanner_scan(show_spinner=bool(scan_now or st.session_state.scanner_signature != current_scan_signature))

scanner_results = st.session_state.scanner_results
ranked_results, valid_results, visible_results = compute_ranked_results(scanner_results)

if valid_results and st.session_state.selected_coin not in COINS:
    st.session_state.selected_coin = valid_results[0]["coin"]
elif valid_results and st.session_state.selected_coin not in [r["coin"] for r in ranked_results]:
    st.session_state.selected_coin = valid_results[0]["coin"]
elif st.session_state.selected_coin not in COINS:
    st.session_state.selected_coin = list(COINS.keys())[0]

@bf_fragment(run_every=f"{AUTO_SCAN_TICK_SEC}s")
def render_auto_scanner_dashboard():
    auto_scan_interval = get_auto_scan_interval_sec(timeframe_label)
    now_ts = time.time()
    needs_auto_scan = (
        st.session_state.scanner_signature != current_scan_signature
        or not st.session_state.scanner_results
        or (now_ts - float(st.session_state.get("scanner_last_scan_epoch", 0.0))) >= auto_scan_interval
    )
    if needs_auto_scan:
        run_scanner_scan(show_spinner=False)

    local_scanner_results = st.session_state.scanner_results
    local_ranked_results, _, local_visible_results = compute_ranked_results(local_scanner_results)

    summary1, summary2, summary3, summary4 = st.columns(4)
    summary1.metric("Actieve coins", len(COINS))
    summary2.metric("Zichtbare setups", len(local_visible_results))
    summary3.metric("Scanner TF", timeframe_label)
    summary4.metric("Auto-scan", f"{auto_scan_interval}s")
    st.caption("Rustige scannerweergave: coin • context • setup-type • entry zone • status.")

    st.markdown("### 🏠 Scanner overzicht")
    st.caption("Eerst kijken naar status. Daarna pas naar entry, stop en target.")
    top_cards = local_ranked_results[:5]
    card_cols = st.columns(5)

    for idx, result in enumerate(top_cards):
        with card_cols[idx]:
            context_label = str(result.get("context_label_main") or result.get("trend_label") or "-")
            setup_type = str(result.get("chosen_entry_variant") or "")
            if setup_type == "early_price_action":
                setup_type = "Early PA"
            elif setup_type == "retest_breakout":
                setup_type = "Retest breakout"
            else:
                setup_type = "Auto"

            side_for_display = result.get("best_side") or result.get("primary_side") or result.get("context_preferred_side")
            timing = result.get("setup_timing", {}) or {}
            if side_for_display == "LONG":
                entry_zone_text = fmt_zone(result.get("long_entry_zone"))
                stop_text = fmt_price_eur(float((result.get("best_metrics") or result.get("long_metrics") or {}).get("stop"))) if (result.get("best_metrics") or result.get("long_metrics")) else "-"
                target_zone_text = fmt_zone(result.get("long_target_zone"))
                status_base = str(timing.get("long_timing") or "WAIT")
            elif side_for_display == "SHORT":
                entry_zone_text = fmt_zone(result.get("short_entry_zone"))
                stop_text = fmt_price_eur(float((result.get("best_metrics") or result.get("short_metrics") or {}).get("stop"))) if (result.get("best_metrics") or result.get("short_metrics")) else "-"
                target_zone_text = fmt_zone(result.get("short_target_zone"))
                status_base = str(timing.get("short_timing") or "WAIT")
            else:
                entry_zone_text = "-"
                stop_text = "-"
                target_zone_text = "-"
                status_base = "WAIT"

            if side_for_display in {"LONG", "SHORT"}:
                status_label = f"{side_for_display} {status_base}"
                status_class = f"{side_for_display.lower()}_{status_base.lower()}"
            else:
                status_label = "NO TRADE"
                status_class = "no_trade"

            short_reason = str(result.get("context_reason") or result.get("best_reason") or "")
            if side_for_display == "LONG" and str(timing.get("long_timing") or "") == "READY":
                short_reason = f"{context_label} + support retest"
            elif side_for_display == "SHORT" and str(timing.get("short_timing") or "") == "READY":
                short_reason = f"{context_label} + resistance retest"
            elif side_for_display == "LONG" and str(timing.get("long_timing") or "") == "NEAR":
                short_reason = "Bijna in long-zone"
            elif side_for_display == "SHORT" and str(timing.get("short_timing") or "") == "NEAR":
                short_reason = "Bijna in short-zone"
            short_reason = short_reason[:52] + "…" if len(short_reason) > 52 else short_reason

            st.markdown(
                f"""
                <div class="bf-scan-card">
                    <div class="bf-scan-coin">{result['coin']}</div>
                    <div class="bf-scan-status {status_class}">{status_label}</div>
                    <div class="bf-scan-setup">{setup_type}</div>
                    <div class="bf-scan-line"><strong>Entry</strong> {entry_zone_text}</div>
                    <div class="bf-scan-line"><strong>SL</strong> {stop_text}</div>
                    <div class="bf-scan-line"><strong>TP</strong> {target_zone_text}</div>
                    <div class="bf-scan-reason">{short_reason or '-'}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if st.button(f"Open {result['coin']}", key=f"auto_open_top_card_{result['coin']}", use_container_width=True):
                st.session_state.selected_coin = result["coin"]
                st.rerun()

    with st.expander("📋 Bekijk alle scanner-resultaten", expanded=False):
        overview_rows = []
        for result in local_ranked_results:
            side_for_display = result.get("best_side") or result.get("primary_side") or result.get("context_preferred_side")
            if side_for_display == "LONG":
                entry_zone_text = fmt_zone(result.get("long_entry_zone"))
            elif side_for_display == "SHORT":
                entry_zone_text = fmt_zone(result.get("short_entry_zone"))
            else:
                entry_zone_text = "-"
            overview_rows.append({
                "Coin": result["coin"],
                "Context": result.get("context_label_main") or result.get("trend_label"),
                "Setup": result.get("chosen_entry_variant") or "auto",
                "Status": result.get("status"),
                "Entry zone": entry_zone_text,
                "Richting": side_for_display or "-",
            })
        st.dataframe(pd.DataFrame(overview_rows), use_container_width=True, hide_index=True)

if st.session_state.get("bf_view_mode") == "desktop":
    render_auto_scanner_dashboard()
else:
    st.caption("📱 Mobiele weergave actief • breed scanner-overzicht is verborgen; compact overzicht staat onderaan.")

scanner_results = st.session_state.scanner_results
ranked_results, valid_results, visible_results = compute_ranked_results(scanner_results)
# =========================================================
# Selected coin detail
# =========================================================
if valid_results and st.session_state.selected_coin not in COINS:
    st.session_state.selected_coin = valid_results[0]["coin"]

selected_coin = st.session_state.selected_coin if st.session_state.selected_coin in COINS else list(COINS.keys())[0]
selected_market = COINS[selected_coin]["bitvavo_market"]
selected_snapshot = get_shared_market_snapshot(force_refresh=False)
selected_price_map = selected_snapshot.get("price_map", {}) or {}
live_selected_price = selected_price_map.get(selected_market) or get_live_price_for_market(selected_market)
if live_selected_price is not None and not st.session_state.manual_override:
    st.session_state.manual_price = float(live_selected_price)


# =========================================================
# Mobiele weergave helpers
# =========================================================
def _bf_mobile_active_side(selected_result: Dict[str, object]) -> Optional[str]:
    return selected_result.get("best_side") or selected_result.get("primary_side") or selected_result.get("context_preferred_side")


def _bf_mobile_active_zones(selected_result: Dict[str, object]):
    side = _bf_mobile_active_side(selected_result)
    if side == "LONG":
        return selected_result.get("long_entry_zone"), selected_result.get("long_target_zone"), selected_result.get("long_invalidation_zone")
    if side == "SHORT":
        return selected_result.get("short_entry_zone"), selected_result.get("short_target_zone"), selected_result.get("short_invalidation_zone")
    speelveld = selected_result.get("speelveld") or {}
    return speelveld.get("support_zone"), speelveld.get("resistance_zone"), None


def _bf_mobile_get_selected_result(scanner_results_local: List[Dict[str, object]], selected_coin_local: str) -> Dict[str, object]:
    cached_selected_result = next((r for r in scanner_results_local if r.get("coin") == selected_coin_local), None)
    if st.session_state.manual_override:
        return analyze_coin_setup(
            coin=selected_coin_local,
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
            manual_price=float(st.session_state.manual_price),
        )
    if cached_selected_result:
        market = COINS[selected_coin_local]["bitvavo_market"]
        live_price = get_live_price_for_market(market)
        if live_price is not None:
            return inject_live_price_into_selected_result(cached_selected_result, live_price)
        return cached_selected_result
    return analyze_coin_setup(
        coin=selected_coin_local,
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


def _bf_mobile_status_icon(status: str) -> str:
    status = str(status or "").upper()
    if status in {"READY", "CONFIRMED_READY", "PLAN_READY"}:
        return "🟢"
    if status in {"PLAN", "NEAR", "WATCH", "WAIT", "UPCOMING"}:
        return "🟡"
    if status in {"HANDS_OFF", "BLOCKED", "MISSED"}:
        return "🔴"
    return "🔵"


def _bf_mobile_reason(selected_result: Dict[str, object], trade_story: Optional[Dict[str, object]] = None) -> str:
    if isinstance(trade_story, dict):
        text = str(trade_story.get("summary") or trade_story.get("detail") or "")
        if text:
            return text
    return str(selected_result.get("context_reason") or selected_result.get("best_reason") or "Geen extra toelichting beschikbaar.")


def render_mobile_trade_plan(selected_result: Dict[str, object]) -> None:
    trade_story = build_trade_tab_story(selected_result)
    display_side = trade_story.get("display_best_side") or _bf_mobile_active_side(selected_result) or "WACHTEN"
    display_metrics = trade_story.get("display_best_metrics") or selected_result.get("best_metrics")
    entry_zone, target_zone, invalidation_zone = _bf_mobile_active_zones(selected_result)
    status = str(selected_result.get("status") or (trade_story.get("headline") or "WAIT"))
    pill_class = "long" if display_side == "LONG" else ("short" if display_side == "SHORT" else "wait")
    price = selected_result.get("live_price") or selected_result.get("current_price")
    rr_text = f"1 : {float(display_metrics['rr']):.2f}" if isinstance(display_metrics, dict) and display_metrics.get("rr") is not None else "-"
    score_text = f"{float(selected_result.get('score') or 0.0):.0f}/100"

    if isinstance(display_metrics, dict):
        entry_text = fmt_price_eur(float(display_metrics["entry"]))
        stop_text = fmt_price_eur(float(display_metrics["stop"]))
        target_text = fmt_price_eur(float(display_metrics["target"]))
    else:
        entry_text = fmt_zone(entry_zone)
        stop_text = fmt_zone(invalidation_zone)
        target_text = fmt_zone(target_zone)

    st.markdown(
        f"""
        <div class="bf-mobile-card">
            <div class="bf-mobile-title">🎯 Trade Plan kaart</div>
            <span class="bf-mobile-pill {pill_class}">{display_side}</span>
            <span class="bf-mobile-pill">{_bf_mobile_status_icon(status)} {status}</span>
            <div class="bf-mobile-line"><strong>Coin / TF:</strong> {selected_result.get('coin', '-')} • {timeframe_label}</div>
            <div class="bf-mobile-line"><strong>Live prijs:</strong> {fmt_price_eur(float(price)) if price is not None else '-'}</div>
            <div class="bf-mobile-line"><strong>Entry:</strong> {entry_text}</div>
            <div class="bf-mobile-line"><strong>Stoploss:</strong> {stop_text}</div>
            <div class="bf-mobile-line"><strong>Take Profit:</strong> {target_text}</div>
            <div class="bf-mobile-line"><strong>RR / Score:</strong> {rr_text} • {score_text}</div>
            <div class="bf-mobile-muted"><strong>Reden:</strong> {_bf_mobile_reason(selected_result, trade_story)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if isinstance(display_metrics, dict) and display_side in {"LONG", "SHORT"}:
        if st.button("📝 Log deze hoofdtrade", key=f"mobile_log_best_{selected_result.get('coin')}", use_container_width=True):
            entry = build_journal_entry(selected_result, display_side, "best", display_metrics)
            append_trade_journal(entry)
            st.success("Hoofdtrade gelogd in journal.")
    else:
        st.button("Geen logbare trade", disabled=True, use_container_width=True, key=f"mobile_no_trade_{selected_result.get('coin')}")


def render_mobile_market_card(selected_result: Dict[str, object]) -> None:
    speelveld = selected_result.get("speelveld") or {}
    price = selected_result.get("live_price") or selected_result.get("current_price")
    trend = selected_result.get("trend_label") or selected_result.get("context_label_main") or "-"
    context = selected_result.get("context_label_main") or speelveld.get("context") or "-"
    support_zone = speelveld.get("support_zone") or selected_result.get("long_entry_zone")
    resistance_zone = speelveld.get("resistance_zone") or selected_result.get("short_entry_zone")
    st.markdown(
        f"""
        <div class="bf-mobile-card">
            <div class="bf-mobile-title">📈 Markt kaart</div>
            <div class="bf-mobile-line"><strong>Live prijs:</strong> {fmt_price_eur(float(price)) if price is not None else '-'}</div>
            <div class="bf-mobile-line"><strong>Context:</strong> {context}</div>
            <div class="bf-mobile-line"><strong>Trend:</strong> {trend}</div>
            <div class="bf-mobile-line"><strong>Support zone:</strong> {fmt_zone(support_zone)}</div>
            <div class="bf-mobile-line"><strong>Resistance zone:</strong> {fmt_zone(resistance_zone)}</div>
            <div class="bf-mobile-muted">{str(speelveld.get('reason') or selected_result.get('context_reason') or '')}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    render_range_position_bar(selected_result, side=_bf_mobile_active_side(selected_result))


def render_mobile_chart(selected_result: Dict[str, object]) -> None:
    st.markdown("### 📉 Compacte chart")
    entry_zone, target_zone, invalidation_zone = _bf_mobile_active_zones(selected_result)
    if selected_result.get("entry_df") is None:
        st.warning("Chartdata tijdelijk niet beschikbaar.")
        return
    chart_df = prepare_chart_focus_df(selected_result["entry_df"], timeframe_label)
    precision_micro = (selected_result.get("lower_tf_precision") or {}).get("micro_structure", {}) if selected_result.get("lower_tf_precision_active") else {}
    render_price_chart(
        chart_df,
        trade_supports=precision_micro.get("trade_supports", selected_result["entry_levels"].get("trade_supports", [])),
        trade_resistances=precision_micro.get("trade_resistances", selected_result["entry_levels"].get("trade_resistances", [])),
        hard_supports=precision_micro.get("hard_supports", selected_result["entry_levels"].get("hard_supports", [])),
        hard_resistances=precision_micro.get("hard_resistances", selected_result["entry_levels"].get("hard_resistances", [])),
        higher_trade_support=selected_result["higher_trade_support"],
        higher_trade_resistance=selected_result["higher_trade_resistance"],
        active_support=selected_result["entry_trade_support"],
        active_resistance=selected_result["entry_trade_resistance"],
        support_zone=selected_result.get("long_entry_zone") or (selected_result.get("speelveld") or {}).get("support_zone"),
        resistance_zone=selected_result.get("short_entry_zone") or (selected_result.get("speelveld") or {}).get("resistance_zone"),
        target_zone=target_zone,
        invalidation_zone=invalidation_zone,
        height=360,
    )


def render_mobile_scanner_overview(ranked_results_local: List[Dict[str, object]]) -> None:
    st.markdown("### 🔎 Compact scanner-overzicht")
    for result in ranked_results_local[:len(COINS)]:
        side = result.get("best_side") or result.get("primary_side") or result.get("context_preferred_side") or "-"
        timing = result.get("setup_timing", {}) or {}
        status = timing.get("long_timing") if side == "LONG" else (timing.get("short_timing") if side == "SHORT" else result.get("status"))
        entry_zone = result.get("long_entry_zone") if side == "LONG" else (result.get("short_entry_zone") if side == "SHORT" else None)
        target_zone = result.get("long_target_zone") if side == "LONG" else (result.get("short_target_zone") if side == "SHORT" else None)
        with st.expander(f"{result.get('coin')} • {side} • {status or '-'}", expanded=result.get("coin") == st.session_state.selected_coin):
            st.write(f"**Context:** {result.get('context_label_main') or result.get('trend_label') or '-'}")
            st.write(f"**Entry-zone:** {fmt_zone(entry_zone)}")
            st.write(f"**Target-zone:** {fmt_zone(target_zone)}")
            st.caption(str(result.get("context_reason") or result.get("best_reason") or ""))
            if st.button(f"Open {result.get('coin')}", key=f"mobile_open_{result.get('coin')}", use_container_width=True):
                st.session_state.selected_coin = result["coin"]
                st.rerun()


def render_mobile_journal_learning() -> None:
    with st.expander("📝 Journal", expanded=False):
        journal_df = load_trade_journal()
        if journal_df.empty:
            st.info("Nog geen journal trades.")
        else:
            open_count = int((journal_df["outcome"].fillna("OPEN") == "OPEN").sum()) if "outcome" in journal_df.columns else 0
            st.metric("Open journal trades", open_count)
            st.dataframe(journal_df.tail(10), use_container_width=True, hide_index=True)
    with st.expander("🧠 Learning", expanded=False):
        learning = build_learning_engine(load_trade_journal(), load_daily_results())
        if learning["closed_df"].empty:
            st.info("Nog te weinig gesloten trades om te leren.")
        else:
            st.markdown("**Top inzichten**")
            for item in learning.get("top_working", [])[:3]:
                st.success(item)
            st.markdown("**Verbeterpunten**")
            for item in learning.get("top_improve", [])[:3]:
                st.warning(item)
    with st.expander("📒 Dagresultaten", expanded=False):
        daily_df = load_daily_results()
        if daily_df.empty:
            st.info("Nog geen dagresultaten ingevuld.")
        else:
            st.dataframe(daily_df.tail(10), use_container_width=True, hide_index=True)


def render_mobile_app(scanner_results_local: List[Dict[str, object]], ranked_results_local: List[Dict[str, object]]) -> None:
    st.subheader("📱 BullForge mobiele weergave")
    st.caption("Compact dashboard: trade plan → markt → chart → scanner/journal/learning.")

    selected_coin_local = st.selectbox(
        "Coin",
        list(COINS.keys()),
        index=list(COINS.keys()).index(st.session_state.selected_coin) if st.session_state.selected_coin in COINS else 0,
        key="mobile_selected_coin",
    )
    if selected_coin_local != st.session_state.selected_coin:
        st.session_state.selected_coin = selected_coin_local
        st.rerun()

    with st.expander("⚙️ Mobiele instellingen", expanded=False):
        st.session_state.manual_override = st.checkbox("Handmatige prijs", value=st.session_state.manual_override, key="mobile_manual_override")
        st.session_state.manual_price = st.number_input(
            "Prijs (€)",
            min_value=0.0,
            value=float(st.session_state.manual_price),
            step=0.0001,
            format="%.8f",
            disabled=not st.session_state.manual_override,
            key="mobile_manual_price",
        )
        st.caption(f"Scanner timeframe: {timeframe_label} • Account: {fmt_eur(float(st.session_state.account_size))}")

    selected_result = _bf_mobile_get_selected_result(scanner_results_local, st.session_state.selected_coin)
    render_mobile_trade_plan(selected_result)
    render_mobile_market_card(selected_result)
    render_volume_panel(selected_result, compact=True)
    render_mobile_chart(selected_result)

    with st.expander("🧭 Speelveld details", expanded=False):
        render_speelveld_panel(selected_result, compact=True)
    with st.expander("📦 Entry ladder / plan details", expanded=False):
        render_plan_vs_entry_sections(selected_result)

    render_mobile_scanner_overview(ranked_results_local)
    render_mobile_journal_learning()


if st.session_state.get("bf_view_mode") == "mobile":
    render_mobile_app(scanner_results, ranked_results)
    st.stop()

# =========================================================
# Tabs
# =========================================================
tab_market, tab_trade, tab_journal, tab_daily, tab_learning = st.tabs(["📈 Markt", "🎯 Trade", "📝 Journal", "📒 Dagresultaten", "🧠 Leren"])

# =========================================================
# Rustige live refresh voor Markt + Trade
# =========================================================
# =========================================================
# Rustige live refresh voor Markt + Trade
# =========================================================
# Fragment UIT: voorkomt duplicatie en StreamlitFragmentWidgetsNotAllowedOutsideError.
def render_live_market_and_trade_tabs():
    shared_snapshot = get_shared_market_snapshot(force_refresh=False)
    shared_price_map = shared_snapshot.get("price_map", {}) or {}

    def _get_selected_result() -> Dict[str, object]:
        cached_selected_result = next((r for r in scanner_results if r["coin"] == st.session_state.selected_coin), None)
        if st.session_state.manual_override:
            return analyze_coin_setup(
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
                shared_price_map=shared_price_map,
            )
        if cached_selected_result:
            market = COINS.get(st.session_state.selected_coin, {}).get("bitvavo_market")
            live_price = shared_price_map.get(market) if market else None
            if live_price is None and market:
                live_price = get_live_price_for_market(market)
            return inject_live_price_into_selected_result(cached_selected_result, live_price)

        return analyze_coin_setup(
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
            shared_price_map=shared_price_map,
        )

    def _setup_type_label(selected_result: Dict[str, object]) -> str:
        if selected_result.get("lower_tf_precision_active"):
            return "Scalp/Daytrade"
        variant = choose_setup_family(selected_result.get("chosen_entry_variant")) if selected_result.get("chosen_entry_variant") else "auto"
        if variant == "early_price_action":
            return "Early PA"
        if variant == "retest_breakout":
            return "Retest breakout"
        return "Auto"

    def _active_side(selected_result: Dict[str, object]) -> Optional[str]:
        return selected_result.get("best_side") or selected_result.get("primary_side") or selected_result.get("context_preferred_side")

    def _active_zones(selected_result: Dict[str, object]):
        side = _active_side(selected_result)
        if side == "LONG":
            return selected_result.get("long_entry_zone"), selected_result.get("long_target_zone"), selected_result.get("long_invalidation_zone")
        if side == "SHORT":
            return selected_result.get("short_entry_zone"), selected_result.get("short_target_zone"), selected_result.get("short_invalidation_zone")
        return None, None, None

    def _status_icon(status: str) -> str:
        status = str(status or "").upper()
        if status in {"READY", "CONFIRMED_READY", "PLAN_READY"}:
            return "🟢"
        if status in {"PLAN", "NEAR", "WATCH", "WAIT"}:
            return "🟡"
        if status in {"HANDS_OFF", "BLOCKED", "MISSED"}:
            return "🔴"
        return "🔵"

    def _one_line_reason(selected_result: Dict[str, object], trade_story: Optional[Dict[str, object]] = None) -> str:
        text = ""
        if isinstance(trade_story, dict):
            text = str(trade_story.get("summary") or trade_story.get("detail") or "")
        if not text:
            text = str(selected_result.get("context_reason") or selected_result.get("best_reason") or "")
        return text.strip() or "Geen extra toelichting beschikbaar."

    def _render_zone_row(entry_zone, invalidation_zone, target_zone) -> None:
        z1, z2, z3 = st.columns(3)
        z1.markdown(f"**🎯 Entry-zone**  \n{fmt_zone(entry_zone)}")
        z2.markdown(f"**🛑 Invalidatie**  \n{fmt_zone(invalidation_zone)}")
        z3.markdown(f"**✅ Target-zone**  \n{fmt_zone(target_zone)}")

    def _render_top_summary(selected_result: Dict[str, object], trade_story: Optional[Dict[str, object]] = None) -> None:
        side = _active_side(selected_result) or "-"
        status = str(selected_result.get("status") or "-")
        price = selected_result.get("live_price") or selected_result.get("current_price")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Prijs", fmt_price_eur(float(price)) if price is not None else "-")
        c2.metric("Context", str(selected_result.get("context_label_main") or "-"))
        c3.metric("Actie", f"{_status_icon(status)} {status}")
        c4.metric("Richting", str(side))
        st.caption(_one_line_reason(selected_result, trade_story))

    def _render_plan_summary(selected_result: Dict[str, object], display_best_side=None, display_best_metrics=None) -> None:
        active_entry_zone, active_target_zone, active_invalidation_zone = _active_zones(selected_result)
        side = display_best_side or _active_side(selected_result) or "-"
        status = str(selected_result.get("status") or "-")
        st.markdown(f"### {_status_icon(status)} Plan: {side} — {status}")
        _render_zone_row(active_entry_zone, active_invalidation_zone, active_target_zone)
        if display_best_metrics is not None:
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Instap", fmt_price_eur(float(display_best_metrics["entry"])))
            m2.metric("Stop", fmt_price_eur(float(display_best_metrics["stop"])))
            m3.metric("Target", fmt_price_eur(float(display_best_metrics["target"])))
            m4.metric("RR", f"1 : {float(display_best_metrics['rr']):.2f}")

    with tab_market:
        selected_result = _get_selected_result()
        active_entry_zone, active_target_zone, active_invalidation_zone = _active_zones(selected_result)

        st.subheader(f"📈 Markt — {selected_result['coin']} ({timeframe_label})")
        _render_top_summary(selected_result)
        render_range_position_bar(selected_result, side=_active_side(selected_result))
        render_volume_panel(selected_result, compact=True)

        control_col, chart_col = st.columns([1.0, 4.0])
        with control_col:
            detail_coin = st.selectbox("Coin", list(COINS.keys()), index=list(COINS.keys()).index(selected_coin))
            if detail_coin != st.session_state.selected_coin:
                st.session_state.selected_coin = detail_coin
                st.rerun()

            st.session_state.manual_override = st.checkbox("Handmatige prijs", value=st.session_state.manual_override)
            price_input = st.number_input(
                "Prijs (€)", min_value=0.0, value=float(st.session_state.manual_price), step=0.0001,
                format="%.8f", disabled=not st.session_state.manual_override,
            )
            if st.session_state.manual_override:
                st.session_state.manual_price = float(price_input)
                selected_result = _get_selected_result()
                active_entry_zone, active_target_zone, active_invalidation_zone = _active_zones(selected_result)

            st.markdown("**Snelle info**")
            st.caption(f"Setup: {_setup_type_label(selected_result)}")
            st.caption(f"Subcontext: {selected_result.get('context_label_sub') or '-'}")
            if selected_result.get("lower_tf_precision_active"):
                st.success("Lower TF Precision")
            if selected_result.get("zone_flip_active"):
                st.success("Zone flip")

            with st.expander("Details", expanded=False):
                timing = selected_result.get("setup_timing", {})
                st.write(f"LONG timing: **{timing.get('long_timing', '-')}**")
                st.write(f"SHORT timing: **{timing.get('short_timing', '-')}**")
                st.write(f"Trend/Setup/Trigger: **{selected_result.get('trend_timeframe_label', '-')} / {selected_result.get('setup_timeframe_label', '-')} / {selected_result.get('trigger_timeframe_label', '-')}**")
                st.write(f"Volatiliteit: **{str(selected_result['entry_vol_profile']['vol_label']).capitalize()}**")

        with chart_col:
            _render_zone_row(active_entry_zone or (selected_result.get("speelveld") or {}).get("support_zone"), active_invalidation_zone, active_target_zone or (selected_result.get("speelveld") or {}).get("resistance_zone"))
            if selected_result["entry_df"] is not None:
                chart_df = prepare_chart_focus_df(selected_result["entry_df"], timeframe_label)
                precision_micro = (selected_result.get("lower_tf_precision") or {}).get("micro_structure", {}) if selected_result.get("lower_tf_precision_active") else {}
                render_price_chart(
                    chart_df,
                    trade_supports=precision_micro.get("trade_supports", selected_result["entry_levels"].get("trade_supports", [])),
                    trade_resistances=precision_micro.get("trade_resistances", selected_result["entry_levels"].get("trade_resistances", [])),
                    hard_supports=precision_micro.get("hard_supports", selected_result["entry_levels"].get("hard_supports", [])),
                    hard_resistances=precision_micro.get("hard_resistances", selected_result["entry_levels"].get("hard_resistances", [])),
                    higher_trade_support=selected_result["higher_trade_support"],
                    higher_trade_resistance=selected_result["higher_trade_resistance"],
                    active_support=selected_result["entry_trade_support"],
                    active_resistance=selected_result["entry_trade_resistance"],
                    support_zone=selected_result.get("long_entry_zone") or (selected_result.get("speelveld") or {}).get("support_zone"),
                    resistance_zone=selected_result.get("short_entry_zone") or (selected_result.get("speelveld") or {}).get("resistance_zone"),
                    target_zone=active_target_zone,
                    invalidation_zone=active_invalidation_zone,
                    height=560,
                )
            else:
                st.warning("Chartdata tijdelijk niet beschikbaar.")

            with st.expander("Speelveld details", expanded=False):
                render_speelveld_panel(selected_result, compact=True)

    with tab_trade:
        selected_result = _get_selected_result()
        trade_story = build_trade_tab_story(selected_result)
        display_best_side = trade_story.get("display_best_side")
        display_best_metrics = trade_story.get("display_best_metrics")

        st.subheader(f"🎯 Trade — {selected_result['coin']}")

        if trade_story["status_kind"] == "success":
            st.success(f"🟢 **{trade_story['headline']}**")
        elif trade_story["status_kind"] == "warning":
            st.warning(f"🟡 **{trade_story['headline']}**")
        elif trade_story["status_kind"] == "error":
            st.error(f"🔴 **{trade_story['headline']}**")
        else:
            st.info(f"🔵 **{trade_story['headline']}**")

        st.caption(_one_line_reason(selected_result, trade_story))
        _render_top_summary(selected_result, trade_story)
        _render_plan_summary(selected_result, display_best_side, display_best_metrics)
        render_volume_panel(selected_result, compact=True)

        action_col, note_col = st.columns([1.2, 2.8])
        with action_col:
            if display_best_metrics is not None and display_best_side in {"LONG", "SHORT"}:
                if st.button("Log hoofdtrade", key=f"log_best_{selected_result['coin']}", use_container_width=True):
                    entry = build_journal_entry(selected_result, display_best_side, "best", display_best_metrics)
                    append_trade_journal(entry)
                    st.success("Hoofdtrade gelogd in journal.")
            else:
                st.button("Geen logbare trade", disabled=True, use_container_width=True)
        with note_col:
            if selected_result.get("lower_tf_precision_active"):
                st.info("Lower TF Precision actief • detail-entry op lagere timeframe.")
            if selected_result.get("zone_flip_active"):
                st.success(f"Zone flip actief • {selected_result.get('zone_flip_note') or 'oude target is nu retest-zone'}")
            if trade_story.get("plan_text"):
                st.caption(trade_story["plan_text"])

        with st.expander("Entry ladder / Plan details", expanded=False):
            render_plan_vs_entry_sections(selected_result)

        with st.expander("Metrics & context", expanded=False):
            if display_best_metrics is not None:
                x1, x2, x3, x4 = st.columns(4)
                x1.metric("Netto winst", fmt_eur(float(display_best_metrics["net_profit_eur"])))
                x2.metric("Kosten", fmt_eur(float(display_best_metrics["total_fees_eur"])))
                x3.metric("Score", f"{float(selected_result.get('score', 0) or 0):.0f}/100")
                x4.metric("Conservatief", fmt_eur(float(selected_result.get("conservative_best_net") or 0.0)))
                entry_notional_eur = float(display_best_metrics["entry"]) * float(display_best_metrics["position_size"])
                account_usage_pct = (entry_notional_eur / account_size * 100) if account_size > 0 else 0.0
                st.write(f"Inleg nodig: **{fmt_eur(entry_notional_eur)}**")
                st.write(f"Positiegrootte: **{float(display_best_metrics['position_size']):.6f} {selected_result['coin']}**")
                st.write(f"Gebruikt van account: **{account_usage_pct:.1f}%**")
            timing = selected_result.get("setup_timing", {})
            st.write(f"LONG context: **{selected_result.get('context_long_reason', '-')}**")
            st.write(f"SHORT context: **{selected_result.get('context_short_reason', '-')}**")
            st.write(f"LONG timing: **{timing.get('long_timing', '-')}**")
            st.write(f"SHORT timing: **{timing.get('short_timing', '-')}**")
            st.write(f"Trend/Setup/Trigger: **{selected_result.get('trend_timeframe_label', '-')} / {selected_result.get('setup_timeframe_label', '-')} / {selected_result.get('trigger_timeframe_label', '-')}**")
            volume = selected_result.get("volume_engine") or {}
            if isinstance(volume, dict) and volume.get("active"):
                st.write(f"Volume management: **{volume.get('management_hint', '-')}**")
render_live_market_and_trade_tabs()

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
        manual_plan_type = m3.selectbox("Plan type", ["manual_early", "manual_confirmed", "manual_other"], key="manual_journal_plan_type")

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

    # Ruim oude/lege CSV-rijen op voor we tonen.
    if not journal_df.empty and "journal_id" in journal_df.columns:
        journal_df = journal_df[journal_df["journal_id"].notna()].copy()
        journal_df = journal_df[journal_df["journal_id"].astype(str).str.strip().ne("")].copy()
        journal_df = journal_df[journal_df["journal_id"].astype(str).str.lower().ne("none")].copy()

    if journal_df.empty:
        st.info("Nog geen journal entries. Log eerst een trade vanuit de Trade-tab of voeg handmatig een trade toe.")
    else:
        journal_df["logged_at"] = journal_df["logged_at"].astype(str)
        journal_df["outcome"] = journal_df["outcome"].fillna("OPEN").astype(str)

        open_count = int((journal_df["outcome"] == "OPEN").sum())
        tp_count = int((journal_df["outcome"] == "TP").sum())
        sl_count = int((journal_df["outcome"] == "SL").sum())
        no_fill_count = int((journal_df["outcome"] == "NO_FILL").sum())
        closed_count = int((journal_df["outcome"] != "OPEN").sum())

        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Totaal", len(journal_df))
        c2.metric("Open", open_count)
        c3.metric("Gesloten", closed_count)
        c4.metric("TP", tp_count)
        c5.metric("SL", sl_count)
        c6.metric("Geen fill", no_fill_count)

        def _clean_journal_view(df: pd.DataFrame) -> pd.DataFrame:
            view = df.copy()
            numeric_cols = ["entry", "stop", "target", "rr", "net_profit_eur", "score", "current_price", "tp_miss_pct"]
            for col in numeric_cols:
                if col in view.columns:
                    view[col] = pd.to_numeric(view[col], errors="coerce")
            for col in ["logged_at", "coin", "scanner_tf", "side", "plan_type", "entry_variant", "location_quality", "outcome", "fill_status"]:
                if col in view.columns:
                    view[col] = view[col].fillna("-").astype(str)
            return view.sort_values("logged_at", ascending=False)

        sorted_journal = _clean_journal_view(journal_df)
        open_view = sorted_journal[sorted_journal["outcome"] == "OPEN"].copy()
        closed_view = sorted_journal[sorted_journal["outcome"] != "OPEN"].copy()

        main_cols = [
            "logged_at", "coin", "scanner_tf", "side", "outcome",
            "plan_type", "entry_variant", "location_quality",
            "entry", "stop", "target", "rr", "net_profit_eur", "notes",
        ]
        plan_cols = [
            "logged_at", "coin", "scanner_tf", "side", "plan_mode_active", "plan_preplaced",
            "zone_touch_before_signal", "fill_status", "late_signal_flag", "tp_miss_pct", "sl_too_tight_flag",
        ]
        raw_cols = [col for col in sorted_journal.columns]

        def _existing(cols):
            return [col for col in cols if col in sorted_journal.columns]

        column_config = {
            "logged_at": st.column_config.TextColumn("Tijd", width="medium"),
            "coin": st.column_config.TextColumn("Coin", width="small"),
            "scanner_tf": st.column_config.TextColumn("TF", width="small"),
            "side": st.column_config.TextColumn("Side", width="small"),
            "outcome": st.column_config.TextColumn("Status", width="small"),
            "plan_type": st.column_config.TextColumn("Plan", width="small"),
            "entry_variant": st.column_config.TextColumn("Setup", width="medium"),
            "location_quality": st.column_config.TextColumn("Locatie", width="small"),
            "entry": st.column_config.NumberColumn("Entry", format="%.6f"),
            "stop": st.column_config.NumberColumn("SL", format="%.6f"),
            "target": st.column_config.NumberColumn("TP", format="%.6f"),
            "rr": st.column_config.NumberColumn("RR", format="%.2f"),
            "net_profit_eur": st.column_config.NumberColumn("Net €", format="€ %.2f"),
            "current_price": st.column_config.NumberColumn("Live prijs", format="%.6f"),
            "tp_miss_pct": st.column_config.NumberColumn("TP miss %", format="%.2f%%"),
            "notes": st.column_config.TextColumn("Notities", width="large"),
        }

        journal_view_tab, open_tab, closed_tab, learning_tab, raw_tab = st.tabs([
            "📋 Overzicht", "🟡 Open trades", "✅ Gesloten", "🧠 Leerdata", "🧾 Raw"
        ])

        with journal_view_tab:
            st.caption("Rustige weergave met alleen de belangrijkste kolommen.")
            st.dataframe(
                sorted_journal[_existing(main_cols)],
                use_container_width=True,
                hide_index=True,
                column_config=column_config,
            )

        with open_tab:
            if open_view.empty:
                st.caption("Geen open trades.")
            else:
                st.dataframe(
                    open_view[_existing(main_cols)],
                    use_container_width=True,
                    hide_index=True,
                    column_config=column_config,
                )

        with closed_tab:
            if closed_view.empty:
                st.caption("Nog geen gesloten trades.")
            else:
                st.dataframe(
                    closed_view[_existing(main_cols)],
                    use_container_width=True,
                    hide_index=True,
                    column_config=column_config,
                )

        with learning_tab:
            st.caption("Velden waarmee de bot leert van te late signalen, gemiste fills en TP/SL-problemen.")
            st.dataframe(
                sorted_journal[_existing(plan_cols)],
                use_container_width=True,
                hide_index=True,
                column_config=column_config,
            )

        with raw_tab:
            st.caption("Volledige CSV-data voor controle/debug. Normaal hoef je hier niet naar te kijken.")
            st.dataframe(sorted_journal[_existing(raw_cols)], use_container_width=True, hide_index=True)

        st.markdown("### Journal beheren")
        manage_col1, manage_col2 = st.columns(2)

        with manage_col1:
            st.markdown("#### Verwijder trade")
            journal_df_display = sorted_journal.copy()
            journal_df_display["delete_label"] = journal_df_display.apply(
                lambda r: f"{r['journal_id']} • {r['coin']} • {r['side']} • {r['plan_type']} • {r['outcome']}",
                axis=1
            )
            delete_label = st.selectbox("Kies trade om te verwijderen", journal_df_display["delete_label"].tolist(), key="journal_delete_select")
            confirm_delete = st.checkbox("Bevestig verwijderen", key="journal_delete_confirm")
            st.caption("Gebruik dit om fout ingevoerde trades uit je leerdata te halen.")
            if st.button("Verwijder gekozen trade", key="journal_delete_btn", disabled=not confirm_delete):
                delete_id = delete_label.split(" • ")[0]
                updated_df = load_trade_journal()
                updated_df = updated_df[updated_df["journal_id"] != delete_id].copy()
                save_trade_journal(updated_df)
                st.success("Trade verwijderd uit journal en learning-data.")
                st.rerun()

        with manage_col2:
            st.markdown("#### Update uitkomst")
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

                outcome_labels = {
                    "TP": "TP geraakt",
                    "SL": "SL geraakt",
                    "BE": "Break-even",
                    "MANUAL_EXIT": "Handmatig gesloten",
                    "NO_FILL": "Entry niet geraakt / order niet gevuld",
                }
                outcome_options = JOURNAL_OUTCOMES[1:]
                selected_outcome_label = st.selectbox(
                    "Uitkomst",
                    [outcome_labels.get(o, o) for o in outcome_options],
                    key="journal_outcome_select",
                )
                outcome = next((key for key, label in outcome_labels.items() if label == selected_outcome_label), selected_outcome_label)
                if outcome == "NO_FILL":
                    st.caption("Handig voor learning: de setup was gepland, maar prijs raakte je entry-zone/order niet.")
                notes = st.text_input("Notitie", value=str(row.get("notes") or ""), key="journal_notes_input")

                if st.button("Sla uitkomst op", key="journal_save_btn"):
                    for text_col in ["outcome", "resolved_at", "notes"]:
                        journal_df[text_col] = journal_df[text_col].astype("object")

                    resolved_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    journal_df.loc[journal_df["journal_id"] == selected_id, "outcome"] = str(outcome)
                    journal_df.loc[journal_df["journal_id"] == selected_id, "resolved_at"] = str(resolved_now)
                    journal_df.loc[journal_df["journal_id"] == selected_id, "notes"] = str(notes)
                    if outcome == "NO_FILL":
                        if "fill_status" not in journal_df.columns:
                            journal_df["fill_status"] = "UNKNOWN"
                        journal_df["fill_status"] = journal_df["fill_status"].astype("object")
                        journal_df.loc[journal_df["journal_id"] == selected_id, "fill_status"] = "MISSED"
                    save_trade_journal(journal_df)
                    st.success("Journal bijgewerkt.")
                    st.rerun()

# =========================================================
# Daily results tab
# =========================================================
with tab_daily:
    st.subheader("📒 Dagresultaten / 1% per dag tracker")
    st.caption("Handmatig invullen per coin. Wordt opgeslagen in CSV zodat het blijft staan na refresh of nieuwe code-versies.")

    daily_df = load_daily_results()

    with st.expander("➕ Dagresultaat toevoegen", expanded=True):
        d1, d2, d3, d4 = st.columns(4)
        default_date = datetime.now().date()
        daily_date = d1.date_input("Datum", value=default_date, key="daily_result_date")
        daily_coin = d2.selectbox("Coin", list(COINS.keys()), key="daily_result_coin")
        daily_type = d3.selectbox("Type", DAILY_RESULT_TYPES, key="daily_result_type")
        daily_trades_count = d4.number_input("Aantal trades", min_value=0, value=0, step=1, key="daily_result_trades_count")

        d5, d6 = st.columns(2)
        default_eur = 0.0
        default_pct = 0.0
        if daily_type == "LOSS":
            default_eur = -5.0
            default_pct = -0.5
        daily_pnl_eur = d5.number_input("Resultaat (€)", value=float(default_eur), step=1.0, format="%.2f", key="daily_result_pnl_eur")
        daily_pnl_pct = d6.number_input("Resultaat (%)", value=float(default_pct), step=0.1, format="%.2f", key="daily_result_pnl_pct")
        daily_notes = st.text_input("Notitie", value="", key="daily_result_notes")

        if st.button("Dagresultaat opslaan", key="daily_result_save_btn"):
            normalized_type = str(daily_type).upper()
            pnl_eur_value = float(daily_pnl_eur)
            pnl_pct_value = float(daily_pnl_pct)

            if normalized_type == "NO_TRADE":
                pnl_eur_value = 0.0
                pnl_pct_value = 0.0
            elif normalized_type == "WIN":
                pnl_eur_value = abs(pnl_eur_value)
                pnl_pct_value = abs(pnl_pct_value)
            elif normalized_type == "LOSS":
                pnl_eur_value = -abs(pnl_eur_value)
                pnl_pct_value = -abs(pnl_pct_value)

            entry = build_daily_result_entry(
                date_value=daily_date.isoformat(),
                coin=daily_coin,
                result_type=normalized_type,
                pnl_eur=pnl_eur_value,
                pnl_pct=pnl_pct_value,
                trades_count=int(daily_trades_count),
                notes=daily_notes,
            )
            append_daily_result(entry)
            st.success("Dagresultaat opgeslagen.")
            st.rerun()

    if daily_df.empty:
        st.info("Nog geen dagresultaten ingevuld.")
    else:
        daily_df["pnl_eur"] = pd.to_numeric(daily_df["pnl_eur"], errors="coerce").fillna(0.0)
        daily_df["pnl_pct"] = pd.to_numeric(daily_df["pnl_pct"], errors="coerce").fillna(0.0)
        daily_df["trades_count"] = pd.to_numeric(daily_df["trades_count"], errors="coerce").fillna(0).astype(int)
        daily_df["date"] = pd.to_datetime(daily_df["date"], errors="coerce")
        daily_df = daily_df.sort_values(["date", "logged_at"], ascending=[False, False]).reset_index(drop=True)

        total_days = int(daily_df["date"].dt.date.nunique())
        win_days = int((daily_df["result_type"] == "WIN").sum())
        loss_days = int((daily_df["result_type"] == "LOSS").sum())
        no_trade_days = int((daily_df["result_type"] == "NO_TRADE").sum())
        total_pnl_eur = float(daily_df["pnl_eur"].sum())
        avg_day_pct = float(daily_df.groupby(daily_df["date"].dt.date)["pnl_pct"].sum().mean()) if not daily_df.empty else 0.0

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Dagen gelogd", total_days)
        k2.metric("Winstdagen", win_days)
        k3.metric("Verliesdagen", loss_days)
        k4.metric("No-trade", no_trade_days)
        k5.metric("Gem. dag %", f"{avg_day_pct:.2f}%")

        k6, k7, k8 = st.columns(3)
        k6.metric("Totaal resultaat", fmt_eur(total_pnl_eur))
        k7.metric("Totaal trades", int(daily_df["trades_count"].sum()))
        k8.metric("Gem. per entry", fmt_eur(total_pnl_eur / max(len(daily_df), 1)))

        st.markdown("### Per coin")
        coin_summary = (
            daily_df.groupby("coin", dropna=False)
            .agg(
                dagen=("result_id", "count"),
                totaal_eur=("pnl_eur", "sum"),
                gemiddeld_pct=("pnl_pct", "mean"),
                trades=("trades_count", "sum"),
            )
            .reset_index()
            .sort_values("totaal_eur", ascending=False)
        )
        st.dataframe(coin_summary, use_container_width=True, hide_index=True)

        st.markdown("### Per dag")
        per_day = (
            daily_df.assign(date_only=daily_df["date"].dt.date)
            .groupby("date_only", dropna=False)
            .agg(
                totaal_eur=("pnl_eur", "sum"),
                totaal_pct=("pnl_pct", "sum"),
                trades=("trades_count", "sum"),
                entries=("result_id", "count"),
            )
            .reset_index()
            .sort_values("date_only", ascending=False)
        )
        st.dataframe(per_day, use_container_width=True, hide_index=True)

        st.markdown("### Alle ingevulde dagresultaten")
        display_daily_df = daily_df.copy()
        display_daily_df["date"] = display_daily_df["date"].dt.strftime("%Y-%m-%d")
        st.dataframe(display_daily_df, use_container_width=True, hide_index=True)

        st.markdown("### Verwijderen / aanpassen")
        remove_options = [
            f"{row.result_id} • {row.date.strftime('%Y-%m-%d') if pd.notna(row.date) else '-'} • {row.coin} • {row.result_type} • {fmt_eur(row.pnl_eur)}"
            for row in daily_df.itertuples()
        ]
        selected_remove = st.selectbox("Kies entry", remove_options, key="daily_remove_select")
        if st.button("Verwijder gekozen dagresultaat", key="daily_remove_btn"):
            result_id = selected_remove.split(" • ")[0]
            updated_df = load_daily_results()
            updated_df = updated_df[updated_df["result_id"] != result_id].copy()
            save_daily_results(updated_df)
            st.success("Dagresultaat verwijderd.")
            st.rerun()

# =========================================================
# Learning tab
# =========================================================
with tab_learning:
    st.subheader("🧠 Journal Learning Engine")
    st.caption("Niet alleen opslaan, maar lezen wat werkt en wat beter kan.")

    learning_journal_df = load_trade_journal()
    learning_daily_df = load_daily_results()
    learning = build_learning_engine(learning_journal_df, learning_daily_df)
    closed_df = learning["closed_df"]

    if closed_df.empty:
        st.info("Nog te weinig gesloten journal trades om echt van te leren. Log eerst trades en werk uitkomsten bij in de Journal-tab.")
    else:
        total_closed = len(closed_df)
        total_tp = int(closed_df["is_win"].sum())
        total_sl = int(closed_df["is_loss"].sum())
        total_be = int(closed_df["is_be"].sum())
        total_manual = int(closed_df["is_manual"].sum())

        l1, l2, l3, l4, l5 = st.columns(5)
        l1.metric("Gesloten trades", total_closed)
        l2.metric("TP", total_tp)
        l3.metric("SL", total_sl)
        l4.metric("BE", total_be)
        l5.metric("Manual exit", total_manual)

        left, right = st.columns(2)
        with left:
            st.markdown("### ✅ Wat werkt goed")
            for item in learning.get("top_working", []):
                st.success(item)
            if not learning.get("top_working"):
                st.caption("Nog niet genoeg data voor sterke positieve inzichten.")

        with right:
            st.markdown("### ⚠️ Wat kan beter")
            for item in learning.get("top_improve", []):
                st.warning(item)
            if not learning.get("top_improve"):
                st.caption("Nog niet genoeg data voor duidelijke verbeterpunten.")

        st.markdown("### Snelle leerblokken")
        b1, b2 = st.columns(2)
        with b1:
            st.markdown("**Top 3 inzichten**")
            for item in learning.get("good_insights", [])[:3]:
                st.write(f"- {item}")
        with b2:
            st.markdown("**Top 3 verbeterpunten**")
            for item in learning.get("bad_insights", [])[:3]:
                st.write(f"- {item}")

        st.markdown("### Performance-overzichten")
        perf_tabs = st.tabs(["Coin", "Timeframe", "Setup", "Long/Short", "Context", "Location"])
        perf_map = [
            ("coin_perf", "coin"),
            ("timeframe_perf", "scanner_tf"),
            ("setup_perf", "setup_family"),
            ("side_perf", "side"),
            ("context_perf", "context"),
            ("location_perf", "location_quality"),
        ]
        for perf_tab, (key, sort_col) in zip(perf_tabs, perf_map):
            with perf_tab:
                perf_df = learning.get(key, pd.DataFrame())
                if perf_df is None or perf_df.empty:
                    st.caption("Nog niet genoeg data.")
                else:
                    st.dataframe(perf_df, use_container_width=True, hide_index=True)

        with st.expander("Waarom deze feedback?", expanded=False):
            st.write("De learning engine kijkt naar je gesloten trades en zoekt patronen in winrate, TP/SL-verdeling en setup-types.")
            st.write("Hij probeert nu antwoord te geven op vragen zoals:")
            st.write("- welke coin werkt best")
            st.write("- welke timeframe werkt best")
            st.write("- early price-action vs retest-breakout")
            st.write("- longs vs shorts")
            st.write("- welke context en location quality beter werken")
            st.write("- of TP/SL en entries logisch voelen op basis van jouw uitkomsten")
            st.caption("Nog geen auto-optimization: eerst begrijpen wat werkt, daarna pas tweaken.")
