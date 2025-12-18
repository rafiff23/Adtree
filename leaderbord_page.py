import pandas as pd
import streamlit as st
from db import get_connection

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
  display:flex; align-items:center; justify-content:space-between;
  margin-bottom: 10px;
}
.lb-title h2 {
  margin: 0; color: rgba(255,255,255,0.92);
  font-weight: 800; letter-spacing: 0.2px;
}
.lb-sub {
  margin: 0; margin-top: 4px;
  color: rgba(255,255,255,0.55);
  font-size: 13px;
}

/* Podium row */
.podium {
  display:flex;
  gap: 14px;
  align-items:flex-end;
  justify-content:space-between;
  margin-top: 12px;
  margin-bottom: 16px;
}

.card {
  flex: 1;
  border-radius: 18px;
  padding: 14px 14px 14px 14px;
  background: rgba(255,255,255,0.05);
  border: 1px solid rgba(255,255,255,0.08);
  box-shadow: 0 10px 24px rgba(0,0,0,0.30);
  backdrop-filter: blur(8px);
  position: relative;
}

.card.rank1 { 
  transform: translateY(-10px);
  background: linear-gradient(180deg, rgba(255,200,0,0.14), rgba(255,255,255,0.05));
  border: 1px solid rgba(255,200,0,0.30);
}
.card.rank2 {
  background: linear-gradient(180deg, rgba(100,180,255,0.14), rgba(255,255,255,0.05));
  border: 1px solid rgba(100,180,255,0.26);
}
.card.rank3 {
  background: linear-gradient(180deg, rgba(70,255,170,0.14), rgba(255,255,255,0.05));
  border: 1px solid rgba(70,255,170,0.22);
}

/* Avatar */
.avatar {
  width: 54px; height: 54px;
  border-radius: 999px;
  display:flex; align-items:center; justify-content:center;
  font-size: 20px;
  font-weight: 900;
  color: rgba(255,255,255,0.92);
  background: rgba(255,255,255,0.10);
  border: 2px solid rgba(255,255,255,0.15);
  margin-bottom: 8px;
}

.badge {
  position:absolute; top: 12px; right: 12px;
  font-size: 12px; font-weight: 800;
  padding: 6px 10px;
  border-radius: 999px;
  background: rgba(255,255,255,0.08);
  border: 1px solid rgba(255,255,255,0.10);
  color: rgba(255,255,255,0.80);
}

.name {
  font-weight: 800;
  color: rgba(255,255,255,0.92);
  margin-top: 2px;
  margin-bottom: 2px;
}
.user {
  color: rgba(255,255,255,0.55);
  font-size: 12px;
  margin-bottom: 10px;
}
.score {
  font-size: 22px;
  font-weight: 900;
  letter-spacing: 0.3px;
  color: rgba(255,255,255,0.94);
}

/* Crown for #1 */
.crown {
  position:absolute;
  top: -18px;
  left: 50%;
  transform: translateX(-50%);
  font-size: 26px;
}

.small-note {
  color: rgba(255,255,255,0.55);
  font-size: 12px;
  margin-top: 6px;
}
</style>
"""

def _format_idr(n):
    if pd.isna(n):
        return ""
    try:
        return f"Rp{int(n):,}".replace(",", ".")
    except:
        return str(n)

# ---------------------------------------------------------
# NEW: load distinct usernames for dropdown (by level)
# ---------------------------------------------------------
@st.cache_data(ttl=60)
def _load_usernames(level_filter: str) -> list[str]:
    where = []
    params = []

    # filter level
    if level_filter != "All":
        where.append("level = %s")
        params.append(int(level_filter))

    # SAFEGUARD: buang header-row yang keinsert
    # (ini yang bikin kamu lihat 'username', 'creator_name', dll sebagai value)
    where.append("COALESCE(NULLIF(TRIM(username), ''), '') <> ''")
    where.append("LOWER(TRIM(username)) <> 'username'")
    where.append("LOWER(TRIM(creator_name)) <> 'creator_name'")

    where_sql = "WHERE " + " AND ".join(where)

    sql = f"""
        SELECT DISTINCT username
        FROM {TABLE_FULL}
        {where_sql}
        ORDER BY username ASC;
    """

    conn = get_connection()
    try:
        dfu = pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()

    usernames = dfu["username"].dropna().astype(str).tolist()
    return ["All"] + usernames

# ---------------------------------------------------------
# REVISED: leaderboard loader uses username dropdown
# ---------------------------------------------------------
@st.cache_data(ttl=60)
def _load_leaderboard(level_filter: str, username_selected: str) -> pd.DataFrame:
    """
    Pull rows, apply filters, order by GMV desc.
    Rank computed on filtered result.
    """
    where = []
    params = []

    # filter level
    if level_filter != "All":
        where.append("level = %s")
        params.append(int(level_filter))

    # filter username (exact match because dropdown)
    if username_selected and username_selected != "All":
        where.append("username = %s")
        params.append(username_selected)

    # SAFEGUARD: buang header-row yang keinsert sebagai data
    where.append("LOWER(TRIM(username)) <> 'username'")
    where.append("LOWER(TRIM(creator_name)) <> 'creator_name'")
    where.append("LOWER(TRIM(status)) <> 'status'")

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    sql = f"""
        SELECT
            creator_name,
            username,
            post_count,
            redemption_gmv_idr,
            status,
            hadiah_idr
        FROM {TABLE_FULL}
        {where_sql}
        ORDER BY redemption_gmv_idr DESC NULLS LAST,
                 hadiah_idr DESC NULLS LAST,
                 post_count DESC NULLS LAST;
    """

    conn = get_connection()
    try:
        df = pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()

    # Compute rank based on sorted order
    df.insert(0, "Rank", range(1, len(df) + 1))

    # Rename for UI
    df = df.rename(columns={
        "redemption_gmv_idr": "GMV",
        "hadiah_idr": "Hadiah",
        "post_count": "Post",
        "creator_name": "Creator Name",
        "username": "Username",
        "status": "Status",
    })

    return df

def _podium_card(rank: int, row: pd.Series) -> str:
    name = row.get("Creator Name", "") or ""
    username = row.get("Username", "") or ""
    gmv = _format_idr(row.get("GMV"))

    initial = (name.strip()[:1] if name and str(name).strip() else (username.strip()[:1] if username else "?")).upper()

    rank_class = f"rank{rank}"
    crown = '<div class="crown">👑</div>' if rank == 1 else ""

    return f"""
      <div class="card {rank_class}">
        {crown}
        <div class="badge">#{rank}</div>
        <div class="avatar">{initial}</div>
        <div class="name">{name if name else "-"}</div>
        <div class="user">@{username if username else "-"}</div>
        <div class="score">{gmv}</div>
      </div>
    """

def render():
    st.markdown(LEADERBOARD_CSS, unsafe_allow_html=True)
    st.markdown('<div class="leaderboard-wrap">', unsafe_allow_html=True)

    st.markdown("""
      <div class="lb-title">
        <div>
          <h2>Leaderboard</h2>
          <p class="lb-sub">Sorted by GMV (highest → lowest). Top 3 shown in podium + also included in the table.</p>
        </div>
      </div>
    """, unsafe_allow_html=True)

    # -----------------------------
    # FILTERS
    # - Level selectbox
    # - Username dropdown (depends on level)
    # -----------------------------
    c1, c2 = st.columns([1, 2])
    with c1:
        level_filter = st.selectbox("Level", ["All", "0", "1", "2", "3", "4"], index=0)

    # username options depends on selected level
    usernames = _load_usernames(level_filter)
    with c2:
        username_selected = st.selectbox("Username", usernames, index=0)

    df = _load_leaderboard(level_filter, username_selected)

    if df.empty:
        st.warning("No rows found for your filter.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    # -----------------------------
    # PODIUM TOP 3
    # -----------------------------
    top3 = df.head(3).copy()

    cards_html = []
    for r in [2, 1, 3]:
        if len(top3) >= r:
            cards_html.append(_podium_card(r, top3.iloc[r - 1]))
        else:
            cards_html.append(
                f'<div class="card rank{r}"><div class="badge">#{r}</div><div class="avatar">?</div><div class="name">-</div><div class="user">@-</div><div class="score">-</div></div>'
            )

    st.markdown(f"""
      <div class="podium">
        {cards_html[0]}
        {cards_html[1]}
        {cards_html[2]}
      </div>
    """, unsafe_allow_html=True)

    # -----------------------------
    # TABLE
    # -----------------------------
    st.subheader("All Creators")

    df_table = df.copy()
    df_table["GMV"] = df_table["GMV"].apply(_format_idr)
    df_table["Hadiah"] = df_table["Hadiah"].apply(_format_idr)

    st.dataframe(df_table, use_container_width=True, hide_index=True)

    st.markdown('<p class="small-note">Tip: If you’re importing multiple times, TRUNCATE first so the table doesn’t duplicate rows.</p>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)
