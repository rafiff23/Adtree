import os
import pandas as pd
import streamlit as st
import psycopg2

TABLE_FULL = "leaderboard.creator_dec_leaderboard_all_level"

# -----------------------------
# STYLE (HTML/CSS)
# -----------------------------
LEADERBOARD_CSS = """
<style>
/* Page background */
.leaderboard-wrap {
  background: radial-gradient(1200px 600px at 30% 10%, rgba(90,120,255,0.18), transparent 55%),
              radial-gradient(900px 500px at 80% 0%, rgba(0,255,200,0.10), transparent 45%),
              linear-gradient(180deg, #0b1020 0%, #080b14 100%);
  padding: 18px 18px 28px 18px;
  border-radius: 18px;
  border: 1px solid rgba(255,255,255,0.06);
}

/* Header */
.lb-title {
  display:flex;
  align-items:center;
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

/* Level info box */
.level-info {
  margin-top: 14px;
  margin-bottom: 18px;
  padding: 14px 16px;
  border-radius: 14px;
  background: rgba(255,255,255,0.06);
  border: 1px solid rgba(255,255,255,0.10);
}

.level-info h4 {
  margin: 0 0 8px 0;
  font-weight: 800;
  color: rgba(255,255,255,0.92);
}

.level-info table {
  width: 100%;
  border-collapse: collapse;
}

.level-info th,
.level-info td {
  padding: 6px 8px;
  font-size: 13px;
  color: rgba(255,255,255,0.85);
  text-align: left;
}

.level-info th {
  color: rgba(255,255,255,0.55);
  font-weight: 700;
}

.level-note {
  margin-top: 8px;
  font-size: 12px;
  color: rgba(255,255,255,0.55);
}

/* Podium */
.podium {
  display:flex;
  gap: 14px;
  align-items:flex-end;
  justify-content:space-between;
  margin-bottom: 16px;
}

.card {
  flex: 1;
  border-radius: 18px;
  padding: 14px;
  background: rgba(255,255,255,0.05);
  border: 1px solid rgba(255,255,255,0.08);
  box-shadow: 0 10px 24px rgba(0,0,0,0.30);
  position: relative;
}

.card.rank1 {
  transform: translateY(-10px);
  background: linear-gradient(180deg, rgba(255,200,0,0.14), rgba(255,255,255,0.05));
}

.card.rank2 {
  background: linear-gradient(180deg, rgba(100,180,255,0.14), rgba(255,255,255,0.05));
}

.card.rank3 {
  background: linear-gradient(180deg, rgba(70,255,170,0.14), rgba(255,255,255,0.05));
}

.avatar {
  width: 54px;
  height: 54px;
  border-radius: 999px;
  display:flex;
  align-items:center;
  justify-content:center;
  font-size: 20px;
  font-weight: 900;
  background: rgba(255,255,255,0.10);
  color: white;
  margin-bottom: 8px;
}

.badge {
  position:absolute;
  top: 12px;
  right: 12px;
  font-size: 12px;
  padding: 6px 10px;
  border-radius: 999px;
  background: rgba(255,255,255,0.10);
}

.crown {
  position:absolute;
  top: -18px;
  left: 50%;
  transform: translateX(-50%);
  font-size: 26px;
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
# Helpers
# -----------------------------
def _format_idr(n):
    if pd.isna(n):
        return ""
    return f"Rp{int(n):,}".replace(",", ".")

def _podium_card(rank: int, row: pd.Series) -> str:
    name = row["Creator Name"]
    username = row["Username"]
    gmv = _format_idr(row["GMV"])
    initial = (name[:1] if name else username[:1]).upper()
    crown = '<div class="crown">👑</div>' if rank == 1 else ""

    return f"""
    <div class="card rank{rank}">
      {crown}
      <div class="badge">#{rank}</div>
      <div class="avatar">{initial}</div>
      <div><strong>{name}</strong></div>
      <div style="opacity:.6">@{username}</div>
      <div style="font-size:20px;font-weight:900;margin-top:6px">{gmv}</div>
    </div>
    """

# -----------------------------
# Level Info Renderer
# -----------------------------
def _render_level_info(level: str):
    if level == "All":
        return

    configs = {
        "0": """
        <h4>LEVEL L0 — Silakan Cek Rank Ini</h4>
        <table>
          <tr><th>Layer</th><th>Minimal Penjualan</th><th>Hadiah</th><th>Minimal Video</th></tr>
          <tr><td>Layer 1</td><td>Rp2.500.000</td><td>Rp150.000</td><td>20</td></tr>
          <tr><td>Layer 2</td><td>Rp5.000.000</td><td>Rp250.000</td><td>20</td></tr>
        </table>
        <div class="level-note">Periode Desember · Redemption GMV · 30 Kreator</div>
        """,
        "1": """
        <h4>LEVEL L1 — Silakan Cek Rank Ini</h4>
        <table>
          <tr><th>Minimal Penjualan</th><th>Hadiah</th></tr>
          <tr><td>Rp8.000.000</td><td>Rp500.000</td></tr>
        </table>
        <div class="level-note">Minimal GMV 8.000.000 · 30 Kreator</div>
        """,
        "2": """
        <h4>LEVEL L2 — Silakan Cek Rank Ini</h4>
        <table>
          <tr><th>Kriteria</th><th>Nilai</th></tr>
          <tr><td>Gaji Pokok</td><td>Rp750.000</td></tr>
          <tr><td>Minimal Penjualan</td><td>Rp45.000.000</td></tr>
          <tr><td>Kuota Kreator</td><td>30</td></tr>
        </table>
        """,
        "3": """
        <h4>LEVEL L3 — Silakan Cek Rank Ini</h4>
        <table>
          <tr><th>Kriteria</th><th>Nilai</th></tr>
          <tr><td>Gaji Pokok</td><td>Rp1.500.000</td></tr>
          <tr><td>Minimal Penjualan</td><td>Rp75.000.000</td></tr>
          <tr><td>Kuota Kreator</td><td>4</td></tr>
        </table>
        """,
        "4": """
        <h4>LEVEL L4 — Silakan Cek Rank Ini</h4>
        <table>
          <tr><th>Kriteria</th><th>Nilai</th></tr>
          <tr><td>Gaji Pokok</td><td>Rp2.000.000</td></tr>
          <tr><td>Minimal Penjualan</td><td>Rp200.000.000</td></tr>
          <tr><td>Kuota Kreator</td><td>1</td></tr>
        </table>
        """
    }

    st.markdown(f'<div class="level-info">{configs[level]}</div>', unsafe_allow_html=True)

# -----------------------------
# Data loaders
# -----------------------------
@st.cache_data(ttl=60)
def _load_usernames(level: str):
    conn = get_pandas_connection()
    where = "" if level == "All" else "WHERE level = %s"
    df = pd.read_sql_query(
        f"SELECT DISTINCT username FROM {TABLE_FULL} {where} ORDER BY username",
        conn,
        params=None if level == "All" else [int(level)]
    )
    conn.close()
    return ["All"] + df["username"].tolist()

@st.cache_data(ttl=60)
def _load_leaderboard(level: str, username: str):
    where = []
    params = []

    if level != "All":
        where.append("level = %s")
        params.append(int(level))
    if username != "All":
        where.append("username = %s")
        params.append(username)

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    conn = get_pandas_connection()
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
        {where_sql}
        ORDER BY redemption_gmv_idr DESC NULLS LAST
        """,
        conn,
        params=params
    )
    conn.close()

    df.insert(0, "Rank", range(1, len(df) + 1))
    df = df.rename(columns={
        "creator_name": "Creator Name",
        "redemption_gmv_idr": "GMV",
        "hadiah_idr": "Hadiah",
        "post_count": "Post",
        "username": "Username",
        "status": "Status"
    })
    return df

# -----------------------------
# UI
# -----------------------------
def render():
    st.markdown(LEADERBOARD_CSS, unsafe_allow_html=True)
    st.markdown('<div class="leaderboard-wrap">', unsafe_allow_html=True)

    st.markdown("""
    <div class="lb-title">
      <div>
        <h2>Leaderboard</h2>
        <p class="lb-sub">Sorted by GMV · Top 3 podium</p>
      </div>
    </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns([1, 2])
    with col1:
        level = st.selectbox("Level", ["All", "0", "1", "2", "3", "4"])
    with col2:
        username = st.selectbox("Username", _load_usernames(level))

    _render_level_info(level)

    df = _load_leaderboard(level, username)

    if df.empty:
        st.warning("No data")
        return

    top3 = df.head(3)
    st.markdown(f"""
    <div class="podium">
      {_podium_card(2, top3.iloc[1]) if len(top3)>1 else ""}
      {_podium_card(1, top3.iloc[0])}
      {_podium_card(3, top3.iloc[2]) if len(top3)>2 else ""}
    </div>
    """, unsafe_allow_html=True)

    df["GMV"] = df["GMV"].apply(_format_idr)
    df["Hadiah"] = df["Hadiah"].apply(_format_idr)

    st.dataframe(df, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

render()
