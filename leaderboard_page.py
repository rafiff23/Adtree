import os
import pandas as pd
import streamlit as st
import psycopg2
from datetime import timezone
from zoneinfo import ZoneInfo
from textwrap import dedent

TABLE_FULL = "leaderboard.creator_dec_leaderboard_all_level"
WIB = ZoneInfo("Asia/Jakarta")

# -----------------------------
# STYLE (HTML/CSS)
# -----------------------------
LEADERBOARD_CSS = """
<style>
.leaderboard-wrap {
  position: relative;
  background: radial-gradient(1200px 600px at 30% 10%, rgba(90,120,255,0.18), transparent 55%),
              radial-gradient(900px 500px at 80% 0%, rgba(0,255,200,0.10), transparent 45%),
              linear-gradient(180deg, #0b1020 0%, #080b14 100%);
  padding: 18px 18px 28px 18px;
  border-radius: 18px;
  border: 1px solid rgba(255,255,255,0.06);
}

.lb-title {
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  margin-bottom: 10px;
}

.lb-title h2 {
  margin: 0;
  color: rgba(255,255,255,0.92);
  font-weight: 800;
}

.lb-sub {
  margin-top: 4px;
  color: rgba(255,255,255,0.55);
  font-size: 13px;
}

/* Last updated */
.last-updated {
  font-size: 12px;
  color: rgba(255,255,255,0.70);
  background: rgba(255,255,255,0.08);
  border: 1px solid rgba(255,255,255,0.12);
  padding: 6px 10px;
  border-radius: 999px;
  white-space: nowrap;
}

/* Slots header */
.slots-head {
  display:flex;
  justify-content:space-between;
  align-items:flex-end;
  margin-top: 14px;
  margin-bottom: 10px;
}
.slots-head .title {
  font-size: 16px;
  font-weight: 800;
  color: rgba(255,255,255,0.92);
}
.slots-head .note {
  font-size: 12px;
  color: rgba(255,255,255,0.55);
  margin-top: 2px;
}

/* Slots grid */
.slots-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
  gap: 10px;
  margin-bottom: 18px;
}

/* Slot card */
.slot-card {
  border-radius: 14px;
  padding: 10px 10px;
  border: 1px solid rgba(255,255,255,0.10);
  background: rgba(255,255,255,0.06);
  min-height: 92px;
  position: relative;
}

/* Empty = dark gray */
.slot-empty {
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
}

/* Filled = blue */
.slot-filled {
  background: linear-gradient(180deg, rgba(40,120,255,0.28), rgba(255,255,255,0.06));
  border: 1px solid rgba(40,120,255,0.55);
  box-shadow: 0 10px 22px rgba(40,120,255,0.10);
}

/* Top right tag */
.slot-tag {
  position:absolute;
  top: 8px;
  right: 8px;
  font-size: 11px;
  padding: 4px 8px;
  border-radius: 999px;
  background: rgba(0,0,0,0.25);
  color: rgba(255,255,255,0.80);
  border: 1px solid rgba(255,255,255,0.10);
}

.slot-title {
  font-size: 12px;
  color: rgba(255,255,255,0.65);
  margin-bottom: 6px;
}

.slot-name {
  font-size: 13px;
  font-weight: 800;
  color: rgba(255,255,255,0.92);
  line-height: 1.15;
  margin-bottom: 2px;
}

.slot-user {
  font-size: 12px;
  color: rgba(255,255,255,0.60);
  margin-bottom: 6px;
}

.slot-metric {
  font-size: 12px;
  color: rgba(255,255,255,0.78);
}

.small-note {
  color: rgba(255,255,255,0.55);
  font-size: 12px;
  margin-top: 6px;
}
</style>
"""

# -----------------------------
# DB CONNECTION (pandas-safe)
# -----------------------------
def get_pandas_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB", "adtree"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "4dtr33"),
    )

# -----------------------------
# HELPERS
# -----------------------------
def _format_idr(n):
    if pd.isna(n):
        return "-"
    try:
        return f"Rp{int(n):,}".replace(",", ".")
    except Exception:
        return str(n)

def _slot_count_by_level(level: str) -> int:
    if level in ("0", "1", "2"):
        return 30
    if level == "3":
        return 4
    if level == "4":
        return 1
    return 0

def _eligible_statuses(level: str) -> set:
    if level == "0":
        return {"Layer 1", "Layer 2"}
    return {"Dapat Hadiah"}

def _safe_str(x):
    return "" if pd.isna(x) else str(x)

def _html(s: str) -> str:
    """Remove indentation so Streamlit Markdown doesn't treat it as code block."""
    return dedent(s).strip()

# -----------------------------
# LAST UPDATED (WIB)
# -----------------------------
@st.cache_data(ttl=60)
def get_last_updated_wib():
    conn = get_pandas_connection()
    try:
        df = pd.read_sql_query(
            f"SELECT MAX(last_updated) AS last_updated FROM {TABLE_FULL}",
            conn
        )
    finally:
        conn.close()

    ts = df.loc[0, "last_updated"]
    if pd.isna(ts):
        return None

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    return ts.astimezone(WIB)

# -----------------------------
# DATA LOADERS
# -----------------------------
@st.cache_data(ttl=60)
def _load_usernames(level: str) -> list[str]:
    conn = get_pandas_connection()
    try:
        df = pd.read_sql_query(
            f"""
            SELECT DISTINCT username
            FROM {TABLE_FULL}
            WHERE level = %s
            ORDER BY username;
            """,
            conn,
            params=[int(level)]
        )
    finally:
        conn.close()

    usernames = df["username"].dropna().astype(str).tolist()
    return ["All"] + usernames

@st.cache_data(ttl=60)
def _load_level_data(level: str) -> pd.DataFrame:
    conn = get_pandas_connection()
    try:
        df = pd.read_sql_query(
            f"""
            SELECT
              creator_name,
              username,
              post_count,
              redemption_gmv_idr,
              hadiah_idr,
              status
            FROM {TABLE_FULL}
            WHERE level = %s
            ORDER BY redemption_gmv_idr DESC NULLS LAST,
                     hadiah_idr DESC NULLS LAST,
                     post_count DESC NULLS LAST;
            """,
            conn,
            params=[int(level)]
        )
    finally:
        conn.close()

    df.insert(0, "Rank", range(1, len(df) + 1))
    return df.rename(columns={
        "creator_name": "Creator Name",
        "username": "Username",
        "post_count": "Post",
        "redemption_gmv_idr": "GMV",
        "hadiah_idr": "Hadiah",
        "status": "Status",
    })

def _render_slots(level: str, df_level: pd.DataFrame):
    slot_count = _slot_count_by_level(level)
    eligible = _eligible_statuses(level)

    df_eligible = df_level[df_level["Status"].isin(eligible)].copy()
    df_fill = df_eligible.head(slot_count)

    cards = []
    for i in range(slot_count):
        slot_no = i + 1

        if i < len(df_fill):
            r = df_fill.iloc[i]
            name = _safe_str(r["Creator Name"]) or "-"
            user = _safe_str(r["Username"]) or "-"
            gmv = _format_idr(r["GMV"])
            status = _safe_str(r["Status"]) or "-"

            cards.append(_html(f"""
<div class="slot-card slot-filled">
  <div class="slot-tag">Slot {slot_no}</div>
  <div class="slot-title">Filled</div>
  <div class="slot-name">{name}</div>
  <div class="slot-user">@{user}</div>
  <div class="slot-metric">GMV: {gmv}</div>
  <div class="slot-metric">Status: {status}</div>
</div>
"""))
        else:
            cards.append(_html(f"""
<div class="slot-card slot-empty">
  <div class="slot-tag">Slot {slot_no}</div>
  <div class="slot-title">Available</div>
  <div class="slot-name">-</div>
  <div class="slot-user">@-</div>
  <div class="slot-metric">GMV: -</div>
  <div class="slot-metric">Status: -</div>
</div>
"""))

    filled_cnt = len(df_fill)
    note = "Eligible status: Layer 1 / Layer 2" if level == "0" else "Eligible status: Dapat Hadiah"

    st.markdown(
        _html(f"""
<div class="slots-head">
  <div>
    <div class="title">Reward Slots</div>
    <div class="note">{filled_cnt}/{slot_count} filled · {note}</div>
  </div>
</div>
<div class="slots-grid">
  {''.join(cards)}
</div>
"""),
        unsafe_allow_html=True
    )

# -----------------------------
# UI
# -----------------------------
def render():
    st.markdown(LEADERBOARD_CSS, unsafe_allow_html=True)
    st.markdown('<div class="leaderboard-wrap">', unsafe_allow_html=True)

    last_ts = get_last_updated_wib()
    last_ts_str = last_ts.strftime("%d %b %Y · %H:%M WIB") if last_ts else "-"

    st.markdown(
        _html(f"""
<div class="lb-title">
  <div>
    <h2>Leaderboard</h2>
    <p class="lb-sub">Sorted by GMV (highest → lowest). Slots show eligible creators per level.</p>
  </div>
  <div class="last-updated">Last updated: {last_ts_str}</div>
</div>
"""),
        unsafe_allow_html=True
    )

    c_util_1, _ = st.columns([1, 5])
    with c_util_1:
        if st.button("Clear cache"):
            st.cache_data.clear()
            st.rerun()

    c1, c2 = st.columns([1, 2])
    with c1:
        level = st.selectbox("Level", ["0", "1", "2", "3", "4"], index=0)
    with c2:
        username_selected = st.selectbox("Username (table filter)", _load_usernames(level), index=0)

    df_level = _load_level_data(level)
    if df_level.empty:
        st.warning("No rows found for this level.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    _render_slots(level, df_level)

    st.subheader(f"All Creators (Level {level})")

    df_table = df_level.copy()
    if username_selected != "All":
        df_table = df_table[df_table["Username"] == username_selected].copy()

    df_table["GMV"] = df_table["GMV"].apply(_format_idr)
    df_table["Hadiah"] = df_table["Hadiah"].apply(_format_idr)

    st.dataframe(df_table, use_container_width=True, hide_index=True)
    st.markdown('<p class="small-note">Tip: Slots are filled based on eligibility status + sorted by GMV.</p>', unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

render()
