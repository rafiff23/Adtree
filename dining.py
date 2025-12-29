import os
import pandas as pd
import streamlit as st
import psycopg2
from datetime import timezone
from zoneinfo import ZoneInfo
from textwrap import dedent

TABLE_FULL = "leaderboard.creator_dec_leaderboard_dining_bonus"
WIB = ZoneInfo("Asia/Jakarta")

# How many reward slots for Dining Bonus
SLOT_COUNT = 30  # change if needed

# -----------------------------
# STYLE (HTML/CSS) - SAME LOOK
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

.slot-highlight {
  color: #4da3ff;
  font-weight: 800;
}

.slot-reward {
  color: #7bb8ff;
  font-weight: 800;
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

def _safe_str(x):
    return "" if pd.isna(x) else str(x)

def _html(s: str) -> str:
    """Remove indentation so Streamlit Markdown doesn't treat it as a code block."""
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
def _load_creator_names() -> list[str]:
    conn = get_pandas_connection()
    try:
        df = pd.read_sql_query(
            f"""
            SELECT DISTINCT creator_name
            FROM {TABLE_FULL}
            WHERE creator_name IS NOT NULL AND creator_name <> ''
            ORDER BY creator_name;
            """,
            conn
        )
    finally:
        conn.close()

    names = df["creator_name"].dropna().astype(str).tolist()
    return ["All"] + names

@st.cache_data(ttl=60)
def _load_dining_bonus_data() -> pd.DataFrame:
    """
    Dining Bonus table (no level).
    Sort by Penjualan Dining DESC, then Bonus DESC.
    """
    conn = get_pandas_connection()
    try:
        df = pd.read_sql_query(
            f"""
            SELECT
              creator_name,
              penjualan_dining_idr,
              syarat_penjualan_idr,
              kurang_penjualan_idr,
              status,
              bonus_idr
            FROM {TABLE_FULL}
            ORDER BY penjualan_dining_idr DESC NULLS LAST,
                     bonus_idr DESC NULLS LAST;
            """,
            conn
        )
    finally:
        conn.close()

    df.insert(0, "Rank", range(1, len(df) + 1))

    # Keep naming consistent for UI
    return df.rename(columns={
        "creator_name": "Creator Name",
        "penjualan_dining_idr": "Penjualan Dining",
        "syarat_penjualan_idr": "Syarat Penjualan",
        "kurang_penjualan_idr": "Kurang Penjualan",
        "status": "Status",
        "bonus_idr": "Bonus",
    })

def _render_slots(df_all: pd.DataFrame):
    """
    Slots filled by eligible creators:
    - eligible when Status == 'Dapat Hadiah'
    Fill order follows table sort (Penjualan Dining desc).
    """
    eligible = {"Dapat Hadiah"}  # change here if your CSV uses another exact wording
    df_eligible = df_all[df_all["Status"].isin(eligible)].copy()
    df_fill = df_eligible.head(SLOT_COUNT)

    cards = []
    for i in range(SLOT_COUNT):
        slot_no = i + 1

        if i < len(df_fill):
            r = df_fill.iloc[i]
            name = _safe_str(r["Creator Name"]) or "-"
            penjualan = _format_idr(r["Penjualan Dining"])
            bonus = _format_idr(r["Bonus"])

            cards.append(_html(f"""
<div class="slot-card slot-filled">
  <div class="slot-tag">Slot {slot_no}</div>
  <div class="slot-title slot-highlight">Dapat Hadiah</div>

  <div class="slot-name">{name}</div>

  <div class="slot-metric">
    Penjualan Dining: {penjualan}
  </div>

  <div class="slot-metric slot-reward">
    Total Bonus: {bonus}
  </div>
</div>
"""))
        else:
            cards.append(_html(f"""
<div class="slot-card slot-empty">
  <div class="slot-tag">Slot {slot_no}</div>
  <div class="slot-title">Available</div>
  <div class="slot-name">-</div>
  <div class="slot-metric">Penjualan Dining: -</div>
  <div class="slot-metric">Total Bonus: -</div>
</div>
"""))

    filled_cnt = len(df_fill)
    note = "Eligible status: Dapat Hadiah"

    st.markdown(
        _html(f"""
<div class="slots-head">
  <div>
    <div class="title">Reward Slots</div>
    <div class="note">{filled_cnt}/{SLOT_COUNT} filled · {note}</div>
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
    <h2>Dining Bonus Leaderboard</h2>
    <p class="lb-sub">Sorted by Penjualan Dining (highest → lowest). Slots show eligible creators.</p>
  </div>
  <div class="last-updated">Last updated: {last_ts_str}</div>
</div>
"""),
        unsafe_allow_html=True
    )

    # Utilities
    c_util_1, _ = st.columns([1, 5])
    with c_util_1:
        if st.button("Clear cache"):
            st.cache_data.clear()
            st.rerun()

    # Filter for table only
    col1, col2 = st.columns([2, 4])
    with col1:
        creator_selected = st.selectbox("Creator Name (table filter)", _load_creator_names(), index=0)
    with col2:
        st.caption(f"Slots: {SLOT_COUNT} · Filled if Status = 'Dapat Hadiah'")

    df_all = _load_dining_bonus_data()
    if df_all.empty:
        st.warning("No rows found in Dining Bonus table.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    # Slots (based on eligibility)
    _render_slots(df_all)

    # Table
    st.subheader("All Creators")

    df_table = df_all.copy()
    if creator_selected != "All":
        df_table = df_table[df_table["Creator Name"] == creator_selected].copy()

    # Format IDR columns
    for col in ["Penjualan Dining", "Syarat Penjualan", "Kurang Penjualan", "Bonus"]:
        if col in df_table.columns:
            df_table[col] = df_table[col].apply(_format_idr)

    st.dataframe(df_table, use_container_width=True, hide_index=True)
    st.markdown('<p class="small-note">Tip: Slots are filled based on eligibility status + sorted by Penjualan Dining.</p>', unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

render()
