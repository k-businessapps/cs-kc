from __future__ import annotations

import base64
import hmac
import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import streamlit as st

APP_TITLE = "KrispCall Deals Recovery Analyzer"
KTM = ZoneInfo("Asia/Kathmandu")
EMAIL_REGEX = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)

KC_PURPLE = "#B04EF0"
KC_DEEP = "#7C3AED"
KC_PINK = "#E060F0"
KC_SOFT = "#F8F2FF"
KC_TEXT = "#1F1430"

EXPIRED_PIPELINE = "Expired Subscriptions"
CANCELLED_PIPELINE = "Cancelled Subscriptions"
EXCLUDED_SUMMARY_OWNER_NORMALIZED = "pipedrive krispcall"


@dataclass
class PipelineResult:
    pipeline_name: str
    kind: str
    enriched_df: pd.DataFrame
    summary_df: pd.DataFrame
    total_deals: int
    connected_deals: int
    won_first_payment_count: int
    won_status_count: int
    revenue_risk_sum: float
    revenue_recovered_first_payment_sum: float
    revenue_recovered_status_sum: float
    deduped_from: int
    deduped_to: int


# -----------------------------
# Secrets and auth
# -----------------------------
def get_secret(path: List[str], default=None):
    cur = st.secrets
    for key in path:
        if key not in cur:
            return default
        cur = cur[key]
    return cur


def check_required_secrets() -> List[str]:
    missing = []
    required = [
        (["auth", "username"], "auth.username"),
        (["auth", "password"], "auth.password"),
        (["mixpanel", "project_id"], "mixpanel.project_id"),
        (["mixpanel", "base_url"], "mixpanel.base_url"),
        (["mixpanel", "auth_header"], "mixpanel.auth_header"),
    ]
    for path, label in required:
        if get_secret(path) in (None, ""):
            missing.append(label)
    return missing


def inject_css() -> None:
    st.markdown(
        f"""
        <style>
          .stApp {{ background: linear-gradient(180deg, #ffffff 0%, #fcf9ff 100%); color: {KC_TEXT}; }}
          .block-container {{ padding-top: 0.9rem; padding-bottom: 1.2rem; max-width: 1280px; }}
          .kc-hero {{
            border-radius: 22px;
            padding: 20px 22px;
            background: linear-gradient(90deg, {KC_DEEP} 0%, {KC_PURPLE} 45%, {KC_PINK} 100%);
            color: white;
            box-shadow: 0 14px 34px rgba(103, 35, 196, 0.18);
          }}
          .kc-hero h1 {{ margin: 0; font-size: 2rem; line-height: 1.1; }}
          .kc-hero p {{ margin: 8px 0 0 0; font-size: 0.96rem; opacity: 0.94; }}
          .kc-panel {{
            background: white;
            border-radius: 18px;
            border: 1px solid rgba(176, 78, 240, 0.14);
            padding: 14px 16px;
            box-shadow: 0 10px 28px rgba(31, 20, 48, 0.05);
          }}
          .kc-chip {{
            display: inline-block;
            padding: 6px 10px;
            border-radius: 999px;
            background: {KC_SOFT};
            color: {KC_DEEP};
            border: 1px solid rgba(176, 78, 240, 0.18);
            font-size: 0.86rem;
            font-weight: 600;
            margin-right: 8px;
            margin-bottom: 8px;
          }}
          .kc-note {{ font-size: 0.9rem; color: rgba(31, 20, 48, 0.78); }}
          .kc-rule {{ height: 0; border: 0; border-top: 3px solid rgba(176, 78, 240, 0.42); margin: 18px 0 14px 0; }}
          div.stButton > button, div.stDownloadButton > button {{
            border-radius: 14px !important;
            border: 0 !important;
            background: linear-gradient(90deg, {KC_DEEP} 0%, {KC_PURPLE} 55%, {KC_PINK} 100%) !important;
            color: white !important;
            font-weight: 700 !important;
            padding: 0.62rem 1rem !important;
            box-shadow: 0 10px 24px rgba(103, 35, 196, 0.18) !important;
          }}
          section[data-testid="stFileUploaderDropzone"] {{
            border-radius: 16px;
            border: 2px dashed rgba(176, 78, 240, 0.35);
            background: {KC_SOFT};
          }}
          div[data-testid="metric-container"] {{
            background: white;
            border: 1px solid rgba(176, 78, 240, 0.14);
            border-radius: 18px;
            padding: 10px 14px;
            box-shadow: 0 10px 28px rgba(31, 20, 48, 0.04);
          }}
          div[data-testid="stDataFrame"] {{
            border-radius: 16px;
            overflow: hidden;
            border: 1px solid rgba(176, 78, 240, 0.12);
          }}
          .stTabs [data-baseweb="tab-list"] {{ gap: 8px; }}
          .stTabs [data-baseweb="tab"] {{
            border-radius: 12px;
            padding: 8px 12px;
            background: white;
            border: 1px solid rgba(176, 78, 240, 0.12);
          }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def logo_html(width_px: int = 245) -> str:
    local_paths = [
        Path(__file__).parent / "assets" / "KrispCallLogo.png",
        Path(__file__).parent / "KrispCallLogo.png",
    ]
    for logo_path in local_paths:
        if logo_path.exists():
            b64 = base64.b64encode(logo_path.read_bytes()).decode("utf-8")
            return f'<img src="data:image/png;base64,{b64}" style="width:{width_px}px;height:auto;"/>'
    return ""


def require_login() -> None:
    st.session_state.setdefault("authenticated", False)
    if st.session_state["authenticated"]:
        return

    inject_css()
    missing = check_required_secrets()
    if missing:
        st.error("Missing Streamlit secrets: " + ", ".join(missing))
        st.stop()

    left, right = st.columns([1, 1.6], vertical_alignment="center")
    with left:
        st.markdown(logo_html(280), unsafe_allow_html=True)
    with right:
        st.markdown(
            '<div class="kc-hero"><h1>KrispCall Secure Access</h1><p>Login required before viewing the deal recovery dashboard.</p></div>',
            unsafe_allow_html=True,
        )

    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Login")

    if submit:
        expected_user = str(get_secret(["auth", "username"]))
        expected_pass = str(get_secret(["auth", "password"]))
        ok = hmac.compare_digest(username, expected_user) and hmac.compare_digest(password, expected_pass)
        if ok:
            st.session_state["authenticated"] = True
            st.rerun()
        st.error("Invalid username or password.")

    st.stop()


# -----------------------------
# Utility helpers
# -----------------------------
def pick_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    lower = {str(c).strip().lower(): c for c in df.columns}
    for candidate in candidates:
        found = lower.get(candidate.strip().lower())
        if found is not None:
            return found
    return None


def extract_first_email(value) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    matches = EMAIL_REGEX.findall(str(value))
    if not matches:
        return None
    return matches[0].strip().lower()


def parse_datetime_to_nepal_naive(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    if getattr(parsed.dt, "tz", None) is not None:
        return parsed.dt.tz_convert(KTM).dt.tz_localize(None)
    return parsed


def epoch_series_to_nepal_naive(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.dropna().empty:
        parsed = pd.to_datetime(series, errors="coerce", utc=True)
    else:
        if float(numeric.dropna().median()) > 1e11:
            numeric = numeric / 1000.0
        parsed = pd.to_datetime(numeric, unit="s", errors="coerce", utc=True)
    return parsed.dt.tz_convert(KTM).dt.tz_localize(None)


def money_or_zero(value) -> float:
    num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return 0.0 if pd.isna(num) else float(num)


def safe_copy(df: pd.DataFrame) -> pd.DataFrame:
    return df.copy() if df is not None else pd.DataFrame()


def canonical_pipeline(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    if "cancel" in text:
        return CANCELLED_PIPELINE
    if "expire" in text:
        return EXPIRED_PIPELINE
    return None


def is_connected_value(value) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    text = str(value).strip().lower()
    if not text:
        return False
    return "connected" in text and "not connected" not in text


def format_money(value: float) -> str:
    return f"{value:,.2f}"


def format_percent(value: float) -> str:
    return f"{value:.2f}%"


def safe_pct(numerator: float, denominator: float) -> float:
    denominator = float(denominator or 0)
    if denominator == 0:
        return 0.0
    return float(numerator or 0) / denominator * 100.0


def normalize_owner(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip().lower()


def exclude_summary_owner(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "Deal_Owner" not in df.columns:
        return df.copy()
    return df[df["Deal_Owner"].apply(normalize_owner) != EXCLUDED_SUMMARY_OWNER_NORMALIZED].copy()


def excluded_summary_count(df: pd.DataFrame) -> int:
    if df.empty or "Deal_Owner" not in df.columns:
        return 0
    return int((df["Deal_Owner"].apply(normalize_owner) == EXCLUDED_SUMMARY_OWNER_NORMALIZED).sum())


def total_summary_denominator(total_deduped_deals: int) -> int:
    return int(total_deduped_deals or 0)


def read_uploaded_table(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(uploaded_file)
    raise ValueError("Unsupported file type. Please upload CSV or Excel.")


# -----------------------------
# Mixpanel export
# -----------------------------
def mixpanel_headers() -> Dict[str, str]:
    auth_header = get_secret(["mixpanel", "auth_header"])
    if not auth_header:
        raise RuntimeError("mixpanel.auth_header is missing in Streamlit secrets.")
    return {
        "accept": "text/plain",
        "authorization": str(auth_header).strip(),
    }


@st.cache_data(show_spinner=False, ttl=900)
def fetch_mixpanel_export(
    project_id: str,
    base_url: str,
    event_name: str,
    from_date: date,
    to_date: date,
) -> pd.DataFrame:
    url = f"{str(base_url).rstrip('/')}/api/2.0/export"
    params = {
        "project_id": str(project_id),
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "event": json.dumps([event_name]),
    }
    response = requests.get(url, params=params, headers=mixpanel_headers(), timeout=240)
    if response.status_code != 200:
        body = (response.text or "")[:600]
        raise RuntimeError(f"Mixpanel export failed for {event_name}. Status {response.status_code}. Response: {body}")

    rows: List[dict] = []
    for line in response.text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    if not rows:
        return pd.DataFrame()

    raw = pd.DataFrame(rows)
    if "properties" in raw.columns:
        props = pd.json_normalize(raw["properties"])
        raw = pd.concat([raw.drop(columns=["properties"]), props], axis=1)

    if "time" in raw.columns:
        raw["event_time_npt"] = epoch_series_to_nepal_naive(raw["time"])

    return raw


def dedupe_mixpanel_export(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    if df.empty:
        return df.copy(), 0

    work = df.copy()
    required = [c for c in ["event", "distinct_id", "time", "$insert_id"] if c in work.columns]
    if len(required) < 3:
        return work, 0

    sort_cols = [c for c in ["event_time_npt", "mp_processing_time_ms"] if c in work.columns]
    if sort_cols:
        work = work.sort_values(sort_cols, kind="mergesort")

    before = len(work)
    subset = [c for c in ["event", "distinct_id", "time", "$insert_id"] if c in work.columns]
    work = work.drop_duplicates(subset=subset, keep="last")
    return work, before - len(work)


def prep_payment_df(df: pd.DataFrame) -> pd.DataFrame:
    work = safe_copy(df)
    if work.empty:
        return work
    email_col = pick_col(work, ["$email", "email", "Email", "user.email", "User Email"])
    amount_col = pick_col(work, ["Amount", "amount", "amount_value"])
    desc_col = pick_col(work, ["Amount Description", "description", "Description", "$description"])

    work["email"] = work[email_col].apply(extract_first_email) if email_col else None
    work["Amount"] = pd.to_numeric(work[amount_col], errors="coerce") if amount_col else pd.NA
    work["Amount Description"] = work[desc_col].astype(str) if desc_col else ""
    work = work[work["email"].notna()].copy()
    if "event_time_npt" in work.columns:
        work = work.sort_values(["email", "event_time_npt"], kind="mergesort")
    return work


# -----------------------------
# Deal processing
# -----------------------------
def standardize_deals(deals_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    work = deals_df.copy()

    created_col = pick_col(work, ["Deal - Deal created", "Deal - Deal created on", "Deal created on", "Created on", "Deal Created On"])
    closed_col = pick_col(work, ["Deal - Deal closed on", "Deal closed on", "Closed on", "Deal Closed On"])
    email_work_col = pick_col(work, ["Person - Email - Work", "Person Email Work", "Work Email", "Email Work"])
    email_other_col = pick_col(work, ["Person - Email - Other", "Person Email Other", "Other Email", "Email Other"])
    title_col = pick_col(work, ["Deal - Title", "Deal Title", "Title"])
    owner_col = pick_col(work, ["Deal - Owner", "Deal Owner", "Owner"])
    pipeline_col = pick_col(work, ["Deal - Pipeline", "Deal Pipeline", "Pipeline"])
    status_col = pick_col(work, ["Deal - Status", "Deal Status", "Status"])
    raw_deal_value_col = pick_col(work, ["Deal - Value", "Deal - Deal value", "Deal Value", "Deal - Amount", "Value"])
    label_col = pick_col(work, ["Deal - Label", "Deal Label", "Label"])
    reach_col = pick_col(work, ["Deal - Reach Status", "Deal Reach Status", "Reach Status"])

    required = {
        "Deal - Deal created": created_col,
        "Deal - Owner": owner_col,
        "Deal - Pipeline": pipeline_col,
        "Deal - Status": status_col,
        "Deal - Value": raw_deal_value_col,
    }
    missing_required = [label for label, col in required.items() if col is None]
    if missing_required:
        raise ValueError("Missing required columns: " + ", ".join(missing_required))

    work["Deal_Created_NPT"] = parse_datetime_to_nepal_naive(work[created_col])
    work["Deal_Closed_NPT"] = parse_datetime_to_nepal_naive(work[closed_col]) if closed_col else pd.NaT
    work["Person_Email_Work_Extracted"] = work[email_work_col].apply(extract_first_email) if email_work_col else None
    work["Person_Email_Other_Extracted"] = work[email_other_col].apply(extract_first_email) if email_other_col else None
    work["Deal_Title_Email_Extracted"] = work[title_col].apply(extract_first_email) if title_col else None
    work["Unified_Email"] = work["Person_Email_Work_Extracted"].combine_first(work["Person_Email_Other_Extracted"]).combine_first(work["Deal_Title_Email_Extracted"])
    work["Pipeline_Group"] = work[pipeline_col].apply(canonical_pipeline)
    work["Deal_Owner"] = work[owner_col].astype(str).str.strip()
    work["Deal_Status_Normalized"] = work[status_col].astype(str).str.strip()
    work["Raw_Deal_Value"] = pd.to_numeric(work[raw_deal_value_col], errors="coerce").fillna(0.0)
    work["Deal_Label"] = work[label_col].astype(str) if label_col else ""
    work["Deal_Reach_Status"] = work[reach_col].astype(str) if reach_col else ""
    work["Connected"] = work.apply(
        lambda row: bool(is_connected_value(row.get("Deal_Reach_Status")) or is_connected_value(row.get("Deal_Label"))),
        axis=1,
    )

    mapping = {
        "created_col": created_col,
        "closed_col": closed_col or "",
        "email_work_col": email_work_col or "",
        "email_other_col": email_other_col or "",
        "title_col": title_col or "",
        "owner_col": owner_col,
        "pipeline_col": pipeline_col,
        "status_col": status_col,
        "raw_deal_value_col": raw_deal_value_col,
        "label_col": label_col or "",
        "reach_col": reach_col or "",
    }
    return work, mapping


def dedupe_pipeline(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, int]:
    if df.empty:
        return df.copy(), 0, 0
    before = len(df)
    deduped = (
        df[df["Unified_Email"].notna()].copy()
        .sort_values(["Pipeline_Group", "Unified_Email", "Deal_Created_NPT"], kind="mergesort")
        .drop_duplicates(subset=["Pipeline_Group", "Unified_Email"], keep="first")
        .copy()
    )
    return deduped, before, len(deduped)


def build_payment_map(payments_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    if payments_df.empty:
        return {}
    return {
        email: grp.sort_values("event_time_npt", kind="mergesort").reset_index(drop=True)
        for email, grp in payments_df.groupby("email", sort=False)
    }


def enrich_pipeline(df: pd.DataFrame, pay_map: Dict[str, pd.DataFrame], kind: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    records = []
    for _, row in df.iterrows():
        email = row.get("Unified_Email")
        created = row.get("Deal_Created_NPT")
        raw_deal_value = money_or_zero(row.get("Raw_Deal_Value"))

        first_payment_amount = math.nan
        first_payment_time = pd.NaT
        first_payment_desc = None
        has_first_payment = False

        if pd.notna(created) and email in pay_map:
            p_df = pay_map[email]
            p_match = p_df[p_df["event_time_npt"] > created]
            if not p_match.empty:
                first = p_match.iloc[0]
                first_payment_amount = money_or_zero(first.get("Amount"))
                first_payment_time = first.get("event_time_npt")
                first_payment_desc = first.get("Amount Description")
                has_first_payment = True

        status_won = str(row.get("Deal_Status_Normalized", "")).strip().lower() == "won"

        out = row.to_dict()
        out["Revenue_Risk"] = raw_deal_value
        out["First_Payment_Found_After_Created"] = bool(has_first_payment)
        out["First_Payment_Amount"] = None if pd.isna(first_payment_amount) else float(first_payment_amount)
        out["First_Payment_Time_NPT"] = first_payment_time
        out["First_Payment_Description"] = first_payment_desc

        if kind == "expired":
            out["Won_First_Payment"] = bool(has_first_payment)
            out["Won_Deal_Status"] = bool(status_won)
            out["Revenue_Recovered_First_Payment"] = float(first_payment_amount) if has_first_payment else 0.0
            out["Revenue_Recovered_Deal_Status"] = float(first_payment_amount) if status_won and has_first_payment else 0.0
        else:
            out["Won_First_Payment"] = False
            out["Won_Deal_Status"] = bool(status_won)
            out["Revenue_Recovered_First_Payment"] = 0.0
            out["Revenue_Recovered_Deal_Status"] = raw_deal_value if status_won else 0.0

        records.append(out)

    enriched = pd.DataFrame(records)
    if not enriched.empty:
        for bool_col in ["Connected", "First_Payment_Found_After_Created", "Won_First_Payment", "Won_Deal_Status"]:
            if bool_col in enriched.columns:
                enriched[bool_col] = enriched[bool_col].fillna(False).astype(bool)
        for money_col in [
            "Raw_Deal_Value",
            "Revenue_Risk",
            "First_Payment_Amount",
            "Revenue_Recovered_First_Payment",
            "Revenue_Recovered_Deal_Status",
        ]:
            if money_col in enriched.columns:
                enriched[money_col] = pd.to_numeric(enriched[money_col], errors="coerce")
    return enriched


def build_cancelled_summary(enriched_df: pd.DataFrame, total_deduped_deals: int) -> pd.DataFrame:
    cols = [
        "Owner",
        "Attempted",
        "Attempted %",
        "Connected",
        "Connect %",
        "Won",
        "Won %",
        "Revenue Risk",
        "Revenue Recovered",
        "Recovery %",
    ]
    if enriched_df.empty:
        return pd.DataFrame(columns=cols)

    denominator = total_summary_denominator(total_deduped_deals)
    work = exclude_summary_owner(enriched_df)
    if work.empty:
        return pd.DataFrame(columns=cols)

    grouped = (
        work.groupby("Deal_Owner", dropna=False)
        .agg(
            Attempted=("Unified_Email", "size"),
            Connected=("Connected", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
            Won=("Won_Deal_Status", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
            Revenue_Risk=("Revenue_Risk", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).sum())),
            Revenue_Recovered=("Revenue_Recovered_Deal_Status", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).sum())),
        )
        .reset_index()
        .rename(columns={"Deal_Owner": "Owner", "Revenue_Risk": "Revenue Risk", "Revenue_Recovered": "Revenue Recovered"})
    )
    grouped["Attempted %"] = grouped.apply(lambda r: safe_pct(r["Attempted"], denominator), axis=1)
    grouped["Connect %"] = grouped.apply(lambda r: safe_pct(r["Connected"], r["Attempted"]), axis=1)
    grouped["Won %"] = grouped.apply(lambda r: safe_pct(r["Won"], r["Attempted"]), axis=1)
    grouped["Recovery %"] = grouped.apply(lambda r: safe_pct(r["Revenue Recovered"], r["Revenue Risk"]), axis=1)
    grouped = grouped[cols].sort_values(["Revenue Recovered", "Won"], ascending=[False, False], kind="mergesort")
    return grouped


def build_expired_summary(enriched_df: pd.DataFrame, total_deduped_deals: int) -> pd.DataFrame:
    cols = [
        "Owner",
        "Attempted",
        "Attempted %",
        "Connected",
        "Connect %",
        "Won - First Payment",
        "Won % - First Payment",
        "Won - Deal Status",
        "Won % - Deal Status",
        "Revenue Risk",
        "Revenue Recovered - First Payment",
        "Recovery % - First Payment",
        "Revenue Recovered - Deal Status",
        "Recovery % - Deal Status",
    ]
    if enriched_df.empty:
        return pd.DataFrame(columns=cols)

    denominator = total_summary_denominator(total_deduped_deals)
    work = exclude_summary_owner(enriched_df)
    if work.empty:
        return pd.DataFrame(columns=cols)

    grouped = (
        work.groupby("Deal_Owner", dropna=False)
        .agg(
            Attempted=("Unified_Email", "size"),
            Connected=("Connected", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
            Won_First_Payment=("Won_First_Payment", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
            Won_Deal_Status=("Won_Deal_Status", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
            Revenue_Risk=("Revenue_Risk", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).sum())),
            Revenue_Recovered_First_Payment=("Revenue_Recovered_First_Payment", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).sum())),
            Revenue_Recovered_Deal_Status=("Revenue_Recovered_Deal_Status", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).sum())),
        )
        .reset_index()
        .rename(
            columns={
                "Deal_Owner": "Owner",
                "Won_First_Payment": "Won - First Payment",
                "Won_Deal_Status": "Won - Deal Status",
                "Revenue_Risk": "Revenue Risk",
                "Revenue_Recovered_First_Payment": "Revenue Recovered - First Payment",
                "Revenue_Recovered_Deal_Status": "Revenue Recovered - Deal Status",
            }
        )
    )
    grouped["Attempted %"] = grouped.apply(lambda r: safe_pct(r["Attempted"], denominator), axis=1)
    grouped["Connect %"] = grouped.apply(lambda r: safe_pct(r["Connected"], r["Attempted"]), axis=1)
    grouped["Won % - First Payment"] = grouped.apply(lambda r: safe_pct(r["Won - First Payment"], r["Attempted"]), axis=1)
    grouped["Won % - Deal Status"] = grouped.apply(lambda r: safe_pct(r["Won - Deal Status"], r["Attempted"]), axis=1)
    grouped["Recovery % - First Payment"] = grouped.apply(lambda r: safe_pct(r["Revenue Recovered - First Payment"], r["Revenue Risk"]), axis=1)
    grouped["Recovery % - Deal Status"] = grouped.apply(lambda r: safe_pct(r["Revenue Recovered - Deal Status"], r["Revenue Risk"]), axis=1)
    grouped = grouped[cols].sort_values(["Revenue Recovered - First Payment", "Won - First Payment"], ascending=[False, False], kind="mergesort")
    return grouped


def pipeline_result(name: str, kind: str, source_df: pd.DataFrame, pay_map: Dict[str, pd.DataFrame]) -> PipelineResult:
    deduped, before, after = dedupe_pipeline(source_df)
    enriched = enrich_pipeline(deduped, pay_map, kind)
    summary = build_expired_summary(enriched, len(enriched)) if kind == "expired" else build_cancelled_summary(enriched, len(enriched))

    total_deals = len(enriched)
    connected_deals = int(enriched["Connected"].fillna(False).astype(bool).sum()) if not enriched.empty else 0
    won_first_payment_count = int(enriched["Won_First_Payment"].fillna(False).astype(bool).sum()) if not enriched.empty else 0
    won_status_count = int(enriched["Won_Deal_Status"].fillna(False).astype(bool).sum()) if not enriched.empty else 0
    revenue_risk_sum = float(pd.to_numeric(enriched.get("Revenue_Risk", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not enriched.empty else 0.0
    revenue_recovered_first_payment_sum = float(pd.to_numeric(enriched.get("Revenue_Recovered_First_Payment", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not enriched.empty else 0.0
    revenue_recovered_status_sum = float(pd.to_numeric(enriched.get("Revenue_Recovered_Deal_Status", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not enriched.empty else 0.0

    return PipelineResult(
        pipeline_name=name,
        kind=kind,
        enriched_df=enriched,
        summary_df=summary,
        total_deals=total_deals,
        connected_deals=connected_deals,
        won_first_payment_count=won_first_payment_count,
        won_status_count=won_status_count,
        revenue_risk_sum=revenue_risk_sum,
        revenue_recovered_first_payment_sum=revenue_recovered_first_payment_sum,
        revenue_recovered_status_sum=revenue_recovered_status_sum,
        deduped_from=before,
        deduped_to=after,
    )


def run_analysis(deals_df: pd.DataFrame, payments_df: pd.DataFrame) -> Tuple[PipelineResult, PipelineResult, List[str]]:
    logs: List[str] = []

    deals_std, mapping = standardize_deals(deals_df)
    logs.append(f"Deals rows loaded: {len(deals_std):,}")
    unified_missing = int(deals_std["Unified_Email"].isna().sum())
    if unified_missing:
        logs.append(f"Deals with no usable email from Work, Other, or Deal Title: {unified_missing:,}")

    payments_prepped = prep_payment_df(payments_df)
    logs.append(f"Payment events usable after cleanup: {len(payments_prepped):,}")
    pay_map = build_payment_map(payments_prepped)

    cancelled_source = deals_std[deals_std["Pipeline_Group"] == CANCELLED_PIPELINE].copy()
    expired_source = deals_std[deals_std["Pipeline_Group"] == EXPIRED_PIPELINE].copy()
    logs.append(f"Cancelled Subscriptions rows before dedupe: {len(cancelled_source):,}")
    logs.append(f"Expired Subscriptions rows before dedupe: {len(expired_source):,}")

    cancelled = pipeline_result(CANCELLED_PIPELINE, "cancelled", cancelled_source, pay_map)
    expired = pipeline_result(EXPIRED_PIPELINE, "expired", expired_source, pay_map)

    logs.append(f"Cancelled dedupe kept {cancelled.deduped_to:,} of {cancelled.deduped_from:,} rows")
    logs.append(f"Expired dedupe kept {expired.deduped_to:,} of {expired.deduped_from:,} rows")
    logs.append("Deduplication is pipeline-specific: the same email can remain once in Cancelled and once in Expired, but not twice inside the same pipeline.")
    logs.append("Owner summaries exclude Pipedrive Krispcall. Attempted % still uses the full deduped pipeline count, including Pipedrive Krispcall, as the denominator.")
    logs.append(f"Excluded from Cancelled owner summary: {excluded_summary_count(cancelled.enriched_df):,} Pipedrive Krispcall rows")
    logs.append(f"Excluded from Expired owner summary: {excluded_summary_count(expired.enriched_df):,} Pipedrive Krispcall rows")
    logs.append("Connected is TRUE when Deal - Reach Status or Deal - Label contains Connected, unless that same value contains Not Connected.")
    logs.append("Expired Subscriptions has two won definitions: First Payment after deal creation, and Deal Status equals Won.")
    logs.append("Expired revenue recovered uses the first Mixpanel payment amount after deal creation for each won definition.")
    logs.append("Cancelled Subscriptions won logic uses Deal Status equals Won. Revenue recovered uses Deal - Value from the uploaded CSV.")
    logs.append("Refund Granted is no longer fetched or used.")
    logs.append(f"Detected columns. Created: {mapping['created_col']}, Closed: {mapping['closed_col'] or 'not found'}, Owner: {mapping['owner_col']}, Pipeline: {mapping['pipeline_col']}")
    logs.append(f"Email columns used. Work: {mapping['email_work_col'] or 'not found'}, Other: {mapping['email_other_col'] or 'not found'}, Title fallback: {mapping['title_col'] or 'not found'}")
    logs.append(f"Connected columns used. Reach Status: {mapping['reach_col'] or 'not found'}, Label: {mapping['label_col'] or 'not found'}")
    logs.append(f"Revenue risk column used: {mapping['raw_deal_value_col']}")

    return cancelled, expired, logs


# -----------------------------
# Excel export with XlsxWriter
# -----------------------------
def display_columns(enriched_df: pd.DataFrame) -> List[str]:
    preferred = [
        "Unified_Email",
        "Deal_Created_NPT",
        "Deal_Closed_NPT",
        "Deal_Owner",
        "Pipeline_Group",
        "Deal_Status_Normalized",
        "Deal_Reach_Status",
        "Deal_Label",
        "Connected",
        "Raw_Deal_Value",
        "Revenue_Risk",
        "First_Payment_Found_After_Created",
        "First_Payment_Amount",
        "First_Payment_Time_NPT",
        "First_Payment_Description",
        "Won_First_Payment",
        "Won_Deal_Status",
        "Revenue_Recovered_First_Payment",
        "Revenue_Recovered_Deal_Status",
    ]
    keep = [c for c in preferred if c in enriched_df.columns]
    passthrough = [c for c in enriched_df.columns if c not in keep]
    return keep + passthrough


def summary_with_total(df: pd.DataFrame, kind: str, total_deduped_deals: int) -> pd.DataFrame:
    if df.empty:
        return df

    denominator = total_summary_denominator(total_deduped_deals)
    total = {col: "" for col in df.columns}
    total["Owner"] = "TOTAL"
    total["Attempted"] = float(pd.to_numeric(df["Attempted"], errors="coerce").fillna(0).sum())
    total["Attempted %"] = safe_pct(total["Attempted"], denominator)
    total["Connected"] = float(pd.to_numeric(df["Connected"], errors="coerce").fillna(0).sum())
    total["Connect %"] = safe_pct(total["Connected"], total["Attempted"])
    total["Revenue Risk"] = float(pd.to_numeric(df["Revenue Risk"], errors="coerce").fillna(0).sum())

    if kind == "expired":
        total["Won - First Payment"] = float(pd.to_numeric(df["Won - First Payment"], errors="coerce").fillna(0).sum())
        total["Won % - First Payment"] = safe_pct(total["Won - First Payment"], total["Attempted"])
        total["Won - Deal Status"] = float(pd.to_numeric(df["Won - Deal Status"], errors="coerce").fillna(0).sum())
        total["Won % - Deal Status"] = safe_pct(total["Won - Deal Status"], total["Attempted"])
        total["Revenue Recovered - First Payment"] = float(pd.to_numeric(df["Revenue Recovered - First Payment"], errors="coerce").fillna(0).sum())
        total["Recovery % - First Payment"] = safe_pct(total["Revenue Recovered - First Payment"], total["Revenue Risk"])
        total["Revenue Recovered - Deal Status"] = float(pd.to_numeric(df["Revenue Recovered - Deal Status"], errors="coerce").fillna(0).sum())
        total["Recovery % - Deal Status"] = safe_pct(total["Revenue Recovered - Deal Status"], total["Revenue Risk"])
    else:
        total["Won"] = float(pd.to_numeric(df["Won"], errors="coerce").fillna(0).sum())
        total["Won %"] = safe_pct(total["Won"], total["Attempted"])
        total["Revenue Recovered"] = float(pd.to_numeric(df["Revenue Recovered"], errors="coerce").fillna(0).sum())
        total["Recovery %"] = safe_pct(total["Revenue Recovered"], total["Revenue Risk"])

    return pd.concat([df, pd.DataFrame([total])], ignore_index=True)


def write_sheet(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = pd.to_datetime(out[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    out.to_excel(writer, sheet_name=sheet_name, index=False)

    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    header_fmt = workbook.add_format({
        "bold": True,
        "font_color": "#4C1D95",
        "bg_color": "#EEE2FF",
        "border": 1,
        "align": "center",
        "valign": "vcenter",
    })
    total_fmt = workbook.add_format({
        "bold": True,
        "bg_color": "#F8F2FF",
        "border": 1,
    })
    money_fmt = workbook.add_format({"num_format": "#,##0.00"})
    percent_fmt = workbook.add_format({"num_format": "0.00"})

    for idx, col_name in enumerate(out.columns):
        worksheet.write(0, idx, col_name, header_fmt)
        series_as_str = out[col_name].astype(str).fillna("")
        max_len = max([len(str(col_name))] + [len(v) for v in series_as_str.head(500).tolist()])
        width = min(max(12, max_len + 2), 42)
        fmt = None
        if col_name in {
            "Deal_Value",
            "Deal - Value",
            "Raw_Deal_Value",
            "Revenue_Risk",
            "Revenue Risk",
            "First_Payment_Amount",
            "Revenue_Recovered_First_Payment",
            "Revenue_Recovered_Deal_Status",
            "Revenue Recovered",
            "Revenue Recovered - First Payment",
            "Revenue Recovered - Deal Status",
        }:
            fmt = money_fmt
            width = min(max(width, 14), 22)
        if "%" in str(col_name):
            fmt = percent_fmt
            width = min(max(width, 14), 24)
        worksheet.set_column(idx, idx, width, fmt)

    worksheet.freeze_panes(1, 0)
    if not out.empty and str(out.iloc[-1, 0]).strip().upper() == "TOTAL":
        worksheet.set_row(len(out), None, total_fmt)


def build_workbook(cancelled: PipelineResult, expired: PipelineResult, logs: List[str]) -> bytes:
    out = BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        cancelled_enriched = cancelled.enriched_df[display_columns(cancelled.enriched_df)] if not cancelled.enriched_df.empty else cancelled.enriched_df
        expired_enriched = expired.enriched_df[display_columns(expired.enriched_df)] if not expired.enriched_df.empty else expired.enriched_df
        write_sheet(writer, "Cancelled_Enriched", cancelled_enriched)
        write_sheet(writer, "Expired_Enriched", expired_enriched)
        write_sheet(writer, "Cancelled_Summary", summary_with_total(cancelled.summary_df, "cancelled", cancelled.total_deals))
        write_sheet(writer, "Expired_Summary", summary_with_total(expired.summary_df, "expired", expired.total_deals))
        write_sheet(writer, "Logs", pd.DataFrame({"log": logs}))
    return out.getvalue()


# -----------------------------
# UI
# -----------------------------
def render_metric_row(result: PipelineResult) -> None:
    if result.kind == "expired":
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Expired attempted", f"{result.total_deals:,}")
        c2.metric("Expired connected", f"{result.connected_deals:,}", format_percent(safe_pct(result.connected_deals, result.total_deals)))
        c3.metric("Won by first payment", f"{result.won_first_payment_count:,}", format_percent(safe_pct(result.won_first_payment_count, result.total_deals)))
        c4.metric("Won by deal status", f"{result.won_status_count:,}", format_percent(safe_pct(result.won_status_count, result.total_deals)))

        c5, c6, c7 = st.columns(3)
        c5.metric("Expired revenue risk", format_money(result.revenue_risk_sum))
        c6.metric("Recovered by first payment", format_money(result.revenue_recovered_first_payment_sum), format_percent(safe_pct(result.revenue_recovered_first_payment_sum, result.revenue_risk_sum)))
        c7.metric("Recovered by deal status", format_money(result.revenue_recovered_status_sum), format_percent(safe_pct(result.revenue_recovered_status_sum, result.revenue_risk_sum)))
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Cancelled attempted", f"{result.total_deals:,}")
        c2.metric("Cancelled connected", f"{result.connected_deals:,}", format_percent(safe_pct(result.connected_deals, result.total_deals)))
        c3.metric("Cancelled won", f"{result.won_status_count:,}", format_percent(safe_pct(result.won_status_count, result.total_deals)))
        c4.metric("Cancelled recovered", format_money(result.revenue_recovered_status_sum), format_percent(safe_pct(result.revenue_recovered_status_sum, result.revenue_risk_sum)))


def render_pipeline_tab(result: PipelineResult) -> None:
    st.markdown(
        f'<div class="kc-panel"><div class="kc-chip">Deduped {result.deduped_from:,} to {result.deduped_to:,}</div>'
        f'<div class="kc-chip">Attempted: {result.total_deals:,}</div>'
        f'<div class="kc-chip">Connected: {result.connected_deals:,}</div>'
        f'<div class="kc-chip">Revenue risk: {format_money(result.revenue_risk_sum)}</div></div>',
        unsafe_allow_html=True,
    )
    render_metric_row(result)
    st.markdown("#### Summary by Deal Owner")
    st.dataframe(summary_with_total(result.summary_df, result.kind, result.total_deals), use_container_width=True, hide_index=True)
    st.markdown("#### Enriched rows")
    st.dataframe(result.enriched_df[display_columns(result.enriched_df)] if not result.enriched_df.empty else result.enriched_df, use_container_width=True, hide_index=True)


def render_results(payload: Dict[str, object]) -> None:
    cancelled: PipelineResult = payload["cancelled"]
    expired: PipelineResult = payload["expired"]
    logs: List[str] = payload["logs"]
    payments_count: int = int(payload["payments_count"])
    workbook_bytes: bytes = payload["workbook_bytes"]
    filename: str = str(payload["filename"])

    overview_tab, cancelled_tab, expired_tab, export_tab, logs_tab = st.tabs(
        ["Overview", "Cancelled", "Expired Subscriptions", "Export", "Logs"]
    )

    with overview_tab:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Payment events", f"{payments_count:,}")
        c2.metric("Cancelled recovered", format_money(cancelled.revenue_recovered_status_sum), format_percent(safe_pct(cancelled.revenue_recovered_status_sum, cancelled.revenue_risk_sum)))
        c3.metric("Expired recovered, first payment", format_money(expired.revenue_recovered_first_payment_sum), format_percent(safe_pct(expired.revenue_recovered_first_payment_sum, expired.revenue_risk_sum)))
        c4.metric("Expired recovered, deal status", format_money(expired.revenue_recovered_status_sum), format_percent(safe_pct(expired.revenue_recovered_status_sum, expired.revenue_risk_sum)))
        st.markdown("### Cancelled")
        render_metric_row(cancelled)
        st.markdown("### Expired Subscriptions")
        render_metric_row(expired)
        st.markdown(
            '<div class="kc-panel"><span class="kc-chip">Refund Granted removed</span><span class="kc-chip">Connected logic added</span><span class="kc-chip">Pipeline-level dedupe</span><span class="kc-chip">Pipedrive Krispcall excluded from summaries</span><span class="kc-chip">Export persists after download</span></div>',
            unsafe_allow_html=True,
        )

    with cancelled_tab:
        render_pipeline_tab(cancelled)

    with expired_tab:
        render_pipeline_tab(expired)

    with export_tab:
        st.markdown("#### Download output workbook")
        st.download_button(
            label="Download Excel workbook",
            data=workbook_bytes,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        st.markdown("#### Included sheets")
        st.markdown(
            '<div class="kc-panel"><span class="kc-chip">Cancelled_Enriched</span><span class="kc-chip">Expired_Enriched</span><span class="kc-chip">Cancelled_Summary</span><span class="kc-chip">Expired_Summary</span><span class="kc-chip">Logs</span></div>',
            unsafe_allow_html=True,
        )

    with logs_tab:
        st.dataframe(pd.DataFrame({"log": logs}), use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, page_icon="📊", layout="wide")
    require_login()
    inject_css()

    left, right = st.columns([1, 2.6], vertical_alignment="center")
    with left:
        st.markdown(logo_html(260), unsafe_allow_html=True)
    with right:
        st.markdown(
            '<div class="kc-hero"><h1>KrispCall Deals Recovery Analyzer</h1>'
            '<p>Upload the deals file. The app pulls <strong>New Payment Made</strong> from Mixpanel and builds owner-wise recovery summaries for Cancelled and Expired Subscriptions.</p></div>',
            unsafe_allow_html=True,
        )

    with st.sidebar:
        st.markdown("### Mixpanel payment range")
        today = datetime.now(KTM).date()
        default_from = today.replace(day=1)
        from_date = st.date_input("From date", value=default_from)
        to_date = st.date_input("To date", value=today)
        if from_date > to_date:
            st.error("From date cannot be later than To date.")
            st.stop()

        st.markdown("---")
        st.markdown("### Logic")
        st.markdown('<div class="kc-note">Email unification uses <strong>Person - Email - Work</strong>, then <strong>Person - Email - Other</strong>, then extracts from <strong>Deal Title</strong> if needed.</div>', unsafe_allow_html=True)
        st.markdown('<div class="kc-note">Deduplication is done by <strong>Pipeline + Email</strong>. Same email can remain in different pipelines, but not twice inside the same pipeline.</div>', unsafe_allow_html=True)
        st.markdown('<div class="kc-note">Connected is TRUE when <strong>Deal - Reach Status</strong> or <strong>Deal - Label</strong> contains Connected, except Not Connected.</div>', unsafe_allow_html=True)
        st.markdown('<div class="kc-note"><strong>Expired</strong> has two won views: first payment after deal creation, and Deal Status = Won.</div>', unsafe_allow_html=True)
        st.markdown('<div class="kc-note"><strong>Cancelled</strong> uses Deal Status = Won and Deal - Value from the uploaded CSV.</div>', unsafe_allow_html=True)
        st.markdown('<div class="kc-note">Owner summaries exclude <strong>Pipedrive Krispcall</strong>. Attempted % uses the full deduped pipeline count, including Pipedrive Krispcall, as the denominator.</div>', unsafe_allow_html=True)
        st.markdown('<div class="kc-note">Refund Granted is no longer required.</div>', unsafe_allow_html=True)

    st.markdown('<hr class="kc-rule">', unsafe_allow_html=True)
    deals_file = st.file_uploader(
        "Upload deals file",
        type=["csv", "xlsx", "xls"],
        help="Deals file should include Deal - Pipeline, Deal - Status, Deal - Value, email fields, Deal - Reach Status, and Deal - Label.",
    )
    run = st.button("Run analysis", type="primary", disabled=deals_file is None)

    if run:
        if deals_file is None:
            st.warning("Upload the deals file to continue.")
            return

        progress = st.progress(0, text="Starting analysis...")
        status = st.empty()

        try:
            status.info("Reading deals file...")
            deals_df = read_uploaded_table(deals_file)
            progress.progress(20, text="Deals file loaded")

            project_id = str(get_secret(["mixpanel", "project_id"]))
            base_url = str(get_secret(["mixpanel", "base_url"]))

            status.info("Fetching Mixpanel event: New Payment Made")
            payments_raw = fetch_mixpanel_export(project_id, base_url, "New Payment Made", from_date, to_date)
            payments_deduped, payments_removed = dedupe_mixpanel_export(payments_raw)
            progress.progress(55, text="New Payment Made fetched")

            status.info("Processing deals, connected logic, deduplication, and summaries...")
            cancelled, expired, logs = run_analysis(deals_df, payments_deduped)
            logs.insert(0, f"Payments dedupe removed: {payments_removed:,}")
            logs.insert(1, f"Mixpanel payment date range used: {from_date.isoformat()} to {to_date.isoformat()}")
            progress.progress(85, text="Summaries complete")

            workbook_bytes = build_workbook(cancelled, expired, logs)
            filename = f"krispcall_deals_recovery_{from_date.isoformat()}_{to_date.isoformat()}.xlsx"
            progress.progress(100, text="Ready")
            status.success("Analysis complete.")

            st.session_state["analysis_payload"] = {
                "cancelled": cancelled,
                "expired": expired,
                "logs": logs,
                "payments_count": len(payments_deduped),
                "workbook_bytes": workbook_bytes,
                "filename": filename,
            }

        except Exception as exc:
            progress.empty()
            status.error(f"Run failed: {exc}")
            st.exception(exc)
            return

    payload = st.session_state.get("analysis_payload")
    if payload:
        render_results(payload)


if __name__ == "__main__":
    main()
