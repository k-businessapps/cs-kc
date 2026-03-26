from __future__ import annotations

import base64
import hmac
import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
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


@dataclass
class PipelineResult:
    pipeline_name: str
    enriched_df: pd.DataFrame
    summary_df: pd.DataFrame
    alt_summary_df: Optional[pd.DataFrame]
    total_deals: int
    won_deals: int
    deal_value_sum: float
    refund_value_sum: float
    net_value_sum: float
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



def parse_deal_created(series: pd.Series) -> pd.Series:
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
        return "Cancelled"
    if "expire" in text:
        return "Expired"
    return None



def format_money(value: float) -> str:
    return f"{value:,.2f}"



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
    work = work.sort_values(["email", "event_time_npt"], kind="mergesort")
    return work



def prep_refund_df(df: pd.DataFrame) -> pd.DataFrame:
    work = safe_copy(df)
    if work.empty:
        return work
    email_col = pick_col(work, ["User Email", "$email", "email", "Email", "user.email"])
    amount_col = pick_col(work, ["Refund Amount", "refund_amount", "Amount", "amount"])
    desc_col = pick_col(work, ["Refunded Transaction Description", "Refunded Transaction description", "description"])

    work["email"] = work[email_col].apply(extract_first_email) if email_col else None
    work["Refund Amount"] = pd.to_numeric(work[amount_col], errors="coerce") if amount_col else pd.NA
    work["Refunded Transaction Description"] = work[desc_col].astype(str) if desc_col else ""
    work = work[work["email"].notna()].copy()
    work = work.sort_values(["email", "event_time_npt"], kind="mergesort")
    return work


# -----------------------------
# Deal processing
# -----------------------------
def standardize_deals(deals_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    work = deals_df.copy()

    created_col = pick_col(work, ["Deal - Deal created on", "Deal created on", "Created on", "Deal Created On"])
    email_col = pick_col(work, ["Person - Email", "Person Email", "Email"])
    title_col = pick_col(work, ["Deal - Title", "Deal Title", "Title"])
    owner_col = pick_col(work, ["Deal - Owner", "Deal Owner", "Owner"])
    pipeline_col = pick_col(work, ["Deal - Pipeline", "Deal Pipeline", "Pipeline"])
    status_col = pick_col(work, ["Deal - Status", "Deal Status", "Status"])
    raw_deal_value_col = pick_col(work, ["Deal - Deal value", "Deal Value", "Deal - Value"])

    missing = {
        "Deal - Deal created on": created_col,
        "Deal - Owner": owner_col,
        "Deal - Pipeline": pipeline_col,
    }
    missing_required = [label for label, col in missing.items() if col is None]
    if missing_required:
        raise ValueError("Missing required columns: " + ", ".join(missing_required))

    work["Deal_Created_NPT"] = parse_deal_created(work[created_col])
    work["Person_Email_Extracted"] = work[email_col].apply(extract_first_email) if email_col else None
    work["Deal_Title_Email_Extracted"] = work[title_col].apply(extract_first_email) if title_col else None
    work["Unified_Email"] = work["Person_Email_Extracted"].combine_first(work["Deal_Title_Email_Extracted"])
    work["Pipeline_Group"] = work[pipeline_col].apply(canonical_pipeline)
    work["Deal_Owner"] = work[owner_col].astype(str).str.strip()
    work["Deal_Status_Normalized"] = work[status_col].astype(str).str.strip() if status_col else ""
    work["Raw_Deal_Value"] = pd.to_numeric(work[raw_deal_value_col], errors="coerce") if raw_deal_value_col else pd.NA

    mapping = {
        "created_col": created_col,
        "email_col": email_col or "",
        "title_col": title_col or "",
        "owner_col": owner_col,
        "pipeline_col": pipeline_col,
        "status_col": status_col or "",
        "raw_deal_value_col": raw_deal_value_col or "",
    }
    return work, mapping



def dedupe_pipeline(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, int]:
    if df.empty:
        return df.copy(), 0, 0
    before = len(df)
    deduped = (
        df[df["Unified_Email"].notna()].copy()
        .sort_values(["Unified_Email", "Deal_Created_NPT"], kind="mergesort")
        .drop_duplicates(subset=["Unified_Email"], keep="first")
        .copy()
    )
    return deduped, before, len(deduped)



def build_event_maps(payments_df: pd.DataFrame, refunds_df: pd.DataFrame) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
    pay_map = {email: grp.sort_values("event_time_npt", kind="mergesort").reset_index(drop=True) for email, grp in payments_df.groupby("email", sort=False)} if not payments_df.empty else {}
    refund_map = {email: grp.sort_values("event_time_npt", kind="mergesort").reset_index(drop=True) for email, grp in refunds_df.groupby("email", sort=False)} if not refunds_df.empty else {}
    return pay_map, refund_map



def enrich_pipeline(df: pd.DataFrame, pay_map: Dict[str, pd.DataFrame], refund_map: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    records = []
    for _, row in df.iterrows():
        email = row.get("Unified_Email")
        created = row.get("Deal_Created_NPT")

        deal_value = math.nan
        first_payment_time = pd.NaT
        first_payment_desc = None
        won = False
        refund_value = 0.0

        if pd.notna(created) and email in pay_map:
            p_df = pay_map[email]
            p_match = p_df[p_df["event_time_npt"] > created]
            if not p_match.empty:
                first = p_match.iloc[0]
                deal_value = money_or_zero(first.get("Amount"))
                first_payment_time = first.get("event_time_npt")
                first_payment_desc = first.get("Amount Description")
                won = True

        if won and pd.notna(created) and email in refund_map:
            r_df = refund_map[email]
            window_end = created + timedelta(days=15)
            r_match = r_df[(r_df["event_time_npt"] > created) & (r_df["event_time_npt"] <= window_end)]
            refund_value = float(pd.to_numeric(r_match.get("Refund Amount", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())

        out = row.to_dict()
        out["Deal_Value"] = None if pd.isna(deal_value) else float(deal_value)
        out["Refund_Value"] = float(refund_value)
        out["Won"] = bool(won)
        out["Net_Value"] = (0.0 if pd.isna(deal_value) else float(deal_value)) - float(refund_value)
        out["First_Payment_Time_NPT"] = first_payment_time
        out["First_Payment_Description"] = first_payment_desc
        records.append(out)

    enriched = pd.DataFrame(records)
    if not enriched.empty:
        enriched["Won"] = enriched["Won"].fillna(False)
        enriched["Refund_Value"] = pd.to_numeric(enriched["Refund_Value"], errors="coerce").fillna(0.0)
        enriched["Net_Value"] = pd.to_numeric(enriched["Net_Value"], errors="coerce").fillna(0.0)
        enriched["Deal_Value"] = pd.to_numeric(enriched["Deal_Value"], errors="coerce")
    return enriched



def build_summary(enriched_df: pd.DataFrame) -> pd.DataFrame:
    cols = ["Deal_Owner", "Attempted_Count", "Won_Count", "Won_%", "Deal_Value", "Refund_Value", "Net_Value"]
    if enriched_df.empty:
        return pd.DataFrame(columns=cols)

    work = enriched_df.copy()
    grouped = (
        work.groupby("Deal_Owner", dropna=False)
        .agg(
            Attempted_Count=("Unified_Email", "size"),
            Won_Count=("Won", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())),
            Deal_Value=("Deal_Value", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).sum())),
            Refund_Value=("Refund_Value", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).sum())),
            Net_Value=("Net_Value", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).sum())),
        )
        .reset_index()
    )
    grouped["Won_%"] = (grouped["Won_Count"] / grouped["Attempted_Count"].replace(0, pd.NA) * 100).fillna(0.0)
    grouped = grouped[cols].sort_values(["Net_Value", "Won_Count"], ascending=[False, False], kind="mergesort")
    return grouped



def build_cancelled_status_summary(enriched_df: pd.DataFrame) -> pd.DataFrame:
    cols = ["Deal_Owner", "Attempted_Count", "Won_Count", "Won_%", "Deal_Value"]
    if enriched_df.empty:
        return pd.DataFrame(columns=cols)

    work = enriched_df.copy()
    status_won = work["Deal_Status_Normalized"].astype(str).str.strip().str.lower().eq("won")
    grouped = (
        work.groupby("Deal_Owner", dropna=False)
        .agg(
            Attempted_Count=("Unified_Email", "size"),
            Won_Count=("Deal_Status_Normalized", lambda s: int(s.astype(str).str.strip().str.lower().eq("won").sum())),
            Deal_Value=("Raw_Deal_Value", lambda s: float(pd.to_numeric(s.where(status_won.loc[s.index]), errors="coerce").fillna(0).sum())),
        )
        .reset_index()
    )
    grouped["Won_%"] = (grouped["Won_Count"] / grouped["Attempted_Count"].replace(0, pd.NA) * 100).fillna(0.0)
    grouped = grouped[cols].sort_values(["Deal_Value", "Won_Count"], ascending=[False, False], kind="mergesort")
    return grouped



def pipeline_result(name: str, source_df: pd.DataFrame, pay_map: Dict[str, pd.DataFrame], refund_map: Dict[str, pd.DataFrame]) -> PipelineResult:
    deduped, before, after = dedupe_pipeline(source_df)
    enriched = enrich_pipeline(deduped, pay_map, refund_map)
    summary = build_summary(enriched)
    alt_summary = build_cancelled_status_summary(enriched) if name == "Cancelled" else None
    won_count = int(enriched["Won"].fillna(False).astype(bool).sum()) if not enriched.empty else 0
    deal_sum = float(pd.to_numeric(enriched.get("Deal_Value", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not enriched.empty else 0.0
    refund_sum = float(pd.to_numeric(enriched.get("Refund_Value", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not enriched.empty else 0.0
    net_sum = float(pd.to_numeric(enriched.get("Net_Value", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not enriched.empty else 0.0
    return PipelineResult(
        pipeline_name=name,
        enriched_df=enriched,
        summary_df=summary,
        alt_summary_df=alt_summary,
        total_deals=len(enriched),
        won_deals=won_count,
        deal_value_sum=deal_sum,
        refund_value_sum=refund_sum,
        net_value_sum=net_sum,
        deduped_from=before,
        deduped_to=after,
    )



def run_analysis(
    deals_df: pd.DataFrame,
    payments_df: pd.DataFrame,
    refunds_df: pd.DataFrame,
) -> Tuple[PipelineResult, PipelineResult, List[str]]:
    logs: List[str] = []

    deals_std, mapping = standardize_deals(deals_df)
    logs.append(f"Deals rows loaded: {len(deals_std):,}")
    unified_missing = int(deals_std["Unified_Email"].isna().sum())
    if unified_missing:
        logs.append(f"Deals with no usable email from Person - Email or Deal Title: {unified_missing:,}")

    payments_prepped = prep_payment_df(payments_df)
    refunds_prepped = prep_refund_df(refunds_df)
    logs.append(f"Payment events usable after cleanup: {len(payments_prepped):,}")
    logs.append(f"Refund events usable after cleanup: {len(refunds_prepped):,}")

    pay_map, refund_map = build_event_maps(payments_prepped, refunds_prepped)

    cancelled_source = deals_std[deals_std["Pipeline_Group"] == "Cancelled"].copy()
    expired_source = deals_std[deals_std["Pipeline_Group"] == "Expired"].copy()
    logs.append(f"Cancelled pipeline rows before dedupe: {len(cancelled_source):,}")
    logs.append(f"Expired pipeline rows before dedupe: {len(expired_source):,}")

    cancelled = pipeline_result("Cancelled", cancelled_source, pay_map, refund_map)
    expired = pipeline_result("Expired", expired_source, pay_map, refund_map)

    logs.append(f"Cancelled dedupe kept {cancelled.deduped_to:,} of {cancelled.deduped_from:,} rows")
    logs.append(f"Expired dedupe kept {expired.deduped_to:,} of {expired.deduped_from:,} rows")
    logs.append("Refunds are counted only for won deals, using refunds that happened within 15 days after the deal creation timestamp in Nepal time.")
    logs.append("Summary tables now include Attempted Count, Won Count, Won %, Deal Value, Refund Value, and Net Value.")
    logs.append("Cancelled pipeline also includes a second summary based only on Deal - Status == Won and Deal - Deal value, ignoring Mixpanel.")
    logs.append(f"Detected columns. Created: {mapping['created_col']}, Owner: {mapping['owner_col']}, Pipeline: {mapping['pipeline_col']}")
    if mapping.get("email_col"):
        logs.append(f"Primary email column used: {mapping['email_col']}")
    if mapping.get("title_col"):
        logs.append(f"Fallback title column used for email extraction: {mapping['title_col']}")
    if mapping.get("raw_deal_value_col"):
        logs.append(f"Cancelled alternate summary uses raw deal value column: {mapping['raw_deal_value_col']}")

    return cancelled, expired, logs


# -----------------------------
# Excel export with XlsxWriter
# -----------------------------
def display_columns(enriched_df: pd.DataFrame) -> List[str]:
    preferred = [
        "Unified_Email",
        "Deal_Created_NPT",
        "Deal_Owner",
        "Pipeline_Group",
        "Deal_Status_Normalized",
        "Raw_Deal_Value",
        "Deal_Value",
        "Refund_Value",
        "Won",
        "Net_Value",
        "First_Payment_Time_NPT",
        "First_Payment_Description",
    ]
    keep = [c for c in preferred if c in enriched_df.columns]
    passthrough = [c for c in enriched_df.columns if c not in keep]
    return keep + passthrough



def summary_with_total(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    total = {df.columns[0]: "TOTAL"}
    for col in df.columns[1:]:
        if pd.api.types.is_numeric_dtype(df[col]):
            total[col] = float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())
        else:
            total[col] = ""
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
        worksheet.set_column(idx, idx, min(max(12, max_len + 2), 38))
        if col_name in {"Deal_Value", "Refund_Value", "Net_Value", "Raw_Deal_Value"}:
            worksheet.set_column(idx, idx, min(max(12, max_len + 2), 18), money_fmt)
        if col_name == "Won_%":
            worksheet.set_column(idx, idx, min(max(12, max_len + 2), 14), percent_fmt)

    worksheet.freeze_panes(1, 0)
    if not out.empty and str(out.iloc[-1, 0]).strip().upper() == "TOTAL":
        last_row = len(out)
        worksheet.set_row(last_row, None, total_fmt)



def build_workbook(cancelled: PipelineResult, expired: PipelineResult) -> bytes:
    out = BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        write_sheet(writer, "Cancelled_Enriched", cancelled.enriched_df[display_columns(cancelled.enriched_df)] if not cancelled.enriched_df.empty else cancelled.enriched_df)
        write_sheet(writer, "Expired_Enriched", expired.enriched_df[display_columns(expired.enriched_df)] if not expired.enriched_df.empty else expired.enriched_df)
        write_sheet(writer, "Cancelled_Summary", summary_with_total(cancelled.summary_df))
        if cancelled.alt_summary_df is not None:
            write_sheet(writer, "Cancelled_Status_Summary", summary_with_total(cancelled.alt_summary_df))
        write_sheet(writer, "Expired_Summary", summary_with_total(expired.summary_df))
    return out.getvalue()


# -----------------------------
# UI
# -----------------------------
def render_metric_row(result: PipelineResult) -> None:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric(f"{result.pipeline_name} deals", f"{result.total_deals:,}")
    c2.metric(f"{result.pipeline_name} won", f"{result.won_deals:,}")
    c3.metric(f"{result.pipeline_name} deal value", format_money(result.deal_value_sum))
    c4.metric(f"{result.pipeline_name} net value", format_money(result.net_value_sum))



def render_pipeline_tab(result: PipelineResult) -> None:
    st.markdown(
        f'<div class="kc-panel"><div class="kc-chip">Deduped {result.deduped_from:,} → {result.deduped_to:,}</div>'
        f'<div class="kc-chip">Attempted leads: {result.total_deals:,}</div>'
        f'<div class="kc-chip">Won deals: {result.won_deals:,}</div>'
        f'<div class="kc-chip">Refund total: {format_money(result.refund_value_sum)}</div></div>',
        unsafe_allow_html=True,
    )
    st.markdown("#### Enriched rows")
    st.dataframe(result.enriched_df[display_columns(result.enriched_df)] if not result.enriched_df.empty else result.enriched_df, use_container_width=True, hide_index=True)
    st.markdown("#### Summary by Deal Owner")
    st.dataframe(summary_with_total(result.summary_df), use_container_width=True, hide_index=True)
    if result.pipeline_name == "Cancelled" and result.alt_summary_df is not None:
        st.markdown("#### Cancelled pipeline status summary")
        st.caption("Uses Deal - Status = Won and Deal - Deal value only. Mixpanel is ignored in this table.")
        st.dataframe(summary_with_total(result.alt_summary_df), use_container_width=True, hide_index=True)



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
            '<p>Upload the deals file once. The app pulls <strong>New Payment Made</strong> and <strong>Refund Granted</strong> directly from Mixpanel, one after the other, then builds pipeline-level outputs for Cancelled and Expired deals.</p></div>',
            unsafe_allow_html=True,
        )

    with st.sidebar:
        st.markdown("### Mixpanel export range")
        today = datetime.now(KTM).date()
        default_from = today.replace(day=1)
        from_date = st.date_input("From date", value=default_from)
        to_date = st.date_input("To date", value=today)
        if from_date > to_date:
            st.error("From date cannot be later than To date.")
            st.stop()

        st.markdown("---")
        st.markdown("### Logic")
        st.markdown('<div class="kc-note">Email unification uses <strong>Person - Email</strong> first, then extracts email from <strong>Deal Title</strong> if needed.</div>', unsafe_allow_html=True)
        st.markdown('<div class="kc-note">Each pipeline is deduped by unified email. Current rule keeps the earliest deal created timestamp per email inside that pipeline.</div>', unsafe_allow_html=True)
        st.markdown('<div class="kc-note">Main summaries use Mixpanel. Deal Value is the first payment after deal creation. Refund Value is the sum of refunds within 15 days after deal creation, only when the deal is won.</div>', unsafe_allow_html=True)
        st.markdown('<div class="kc-note">Cancelled has an extra summary that ignores Mixpanel and uses only <strong>Deal - Status</strong> and <strong>Deal - Deal value</strong>.</div>', unsafe_allow_html=True)

    st.markdown('<hr class="kc-rule">', unsafe_allow_html=True)
    deals_file = st.file_uploader(
        "Upload deals file",
        type=["csv", "xlsx", "xls"],
        help="Deals file is the primary input. Mixpanel events are fetched automatically.",
    )
    run = st.button("Run analysis", type="primary", disabled=deals_file is None)

    if not run:
        return

    if deals_file is None:
        st.warning("Upload the deals file to continue.")
        return

    progress = st.progress(0, text="Starting analysis...")
    status = st.empty()

    try:
        status.info("Reading deals file...")
        deals_df = read_uploaded_table(deals_file)
        progress.progress(10, text="Deals file loaded")

        project_id = str(get_secret(["mixpanel", "project_id"]))
        base_url = str(get_secret(["mixpanel", "base_url"]))

        status.info("Fetching Mixpanel event 1 of 2: New Payment Made")
        payments_raw = fetch_mixpanel_export(project_id, base_url, "New Payment Made", from_date, to_date)
        payments_deduped, payments_removed = dedupe_mixpanel_export(payments_raw)
        progress.progress(35, text="New Payment Made fetched")

        status.info("Fetching Mixpanel event 2 of 2: Refund Granted")
        refunds_raw = fetch_mixpanel_export(project_id, base_url, "Refund Granted", from_date, to_date)
        refunds_deduped, refunds_removed = dedupe_mixpanel_export(refunds_raw)
        progress.progress(60, text="Refund Granted fetched")

        status.info("Processing deals, deduplication, and enrichment...")
        cancelled, expired, logs = run_analysis(deals_df, payments_deduped, refunds_deduped)
        logs.insert(0, f"Payments dedupe removed: {payments_removed:,}")
        logs.insert(1, f"Refunds dedupe removed: {refunds_removed:,}")
        logs.insert(2, f"Mixpanel date range used: {from_date.isoformat()} to {to_date.isoformat()}")
        progress.progress(85, text="Enrichment complete")

        workbook_bytes = build_workbook(cancelled, expired)
        filename = f"krispcall_deals_recovery_{from_date.isoformat()}_{to_date.isoformat()}.xlsx"
        progress.progress(100, text="Ready")
        status.success("Analysis complete.")

    except Exception as exc:
        progress.empty()
        status.error(f"Run failed: {exc}")
        st.exception(exc)
        return

    overview_tab, cancelled_tab, expired_tab, export_tab, logs_tab = st.tabs(
        ["Overview", "Cancelled", "Expired", "Export", "Logs"]
    )

    with overview_tab:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Payments events", f"{len(payments_deduped):,}")
        c2.metric("Refund events", f"{len(refunds_deduped):,}")
        c3.metric("Cancelled net", format_money(cancelled.net_value_sum))
        c4.metric("Expired net", format_money(expired.net_value_sum))
        render_metric_row(cancelled)
        render_metric_row(expired)
        st.markdown(
            '<div class="kc-panel"><span class="kc-chip">Cancelled summary tab included</span><span class="kc-chip">Cancelled status summary included</span><span class="kc-chip">Expired summary tab included</span></div>',
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
            '<div class="kc-panel"><span class="kc-chip">Cancelled_Enriched</span><span class="kc-chip">Expired_Enriched</span><span class="kc-chip">Cancelled_Summary</span><span class="kc-chip">Cancelled_Status_Summary</span><span class="kc-chip">Expired_Summary</span></div>',
            unsafe_allow_html=True,
        )

    with logs_tab:
        st.dataframe(pd.DataFrame({"log": logs}), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
