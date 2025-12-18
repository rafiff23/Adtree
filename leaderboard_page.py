import pandas as pd
import streamlit as st
import psycopg2
import os

TABLE_FULL = "leaderboard.creator_dec_leaderboard_all_level"


# =========================================
# CUSTOM CONNECTION FOR PANDAS (no RealDictCursor)
# =========================================
def get_pandas_connection():
    """
    Connection WITHOUT RealDictCursor for pandas compatibility.
    RealDictCursor breaks pd.read_sql_query()!
    """
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB", "adtree"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "4dtr33"),
        # NO cursor_factory here!
    )
    return conn


# =========================================
# 1) DEBUG: DB IDENTITY
# =========================================
def load_db_identity() -> pd.DataFrame:
    conn = get_pandas_connection()
    try:
        sql = """
        SELECT
          current_database() AS database,
          current_user AS db_user,
          inet_server_addr() AS server_ip,
          inet_server_port() AS server_port,
          version() AS pg_version;
        """
        return pd.read_sql_query(sql, conn)
    finally:
        conn.close()


# =========================================
# 2) LOAD USERNAME OPTIONS (dropdown)
# =========================================
@st.cache_data(ttl=60)
def load_usernames(level_filter: str) -> list[str]:
    where = []
    params = []

    if level_filter != "All":
        where.append("level = %s")
        params.append(int(level_filter))

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    sql = f"""
        SELECT DISTINCT username
        FROM {TABLE_FULL}
        {where_sql}
        ORDER BY username;
    """

    conn = get_pandas_connection()
    try:
        df = pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()

    usernames = df["username"].dropna().astype(str).tolist()
    return ["All"] + usernames


# =========================================
# 3) MAIN LOADER
# =========================================
@st.cache_data(ttl=60)
def load_leaderboard(level_filter: str, username_filter: str) -> pd.DataFrame:
    where = []
    params = []

    if level_filter != "All":
        where.append("level = %s")
        params.append(int(level_filter))

    if username_filter != "All":
        where.append("username = %s")
        params.append(username_filter)

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    sql = f"""
        SELECT
          rank_no,
          creator_name,
          username,
          post_count,
          redemption_gmv_idr,
          status,
          hadiah_idr,
          level,
          imported_at
        FROM {TABLE_FULL}
        {where_sql}
        ORDER BY redemption_gmv_idr DESC NULLS LAST, hadiah_idr DESC NULLS LAST, post_count DESC NULLS LAST
        LIMIT 500;
    """

    conn = get_pandas_connection()
    try:
        df = pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()

    return df


# =========================================
# 4) UI
# =========================================
def render():
    st.title("Leaderboard (Debug Fixed)")

    c0, c1 = st.columns([1, 3])
    with c0:
        if st.button("Clear cache"):
            st.cache_data.clear()
            st.rerun()

    with st.expander("DB Identity", expanded=True):
        st.dataframe(load_db_identity(), hide_index=True, use_container_width=True)

    st.subheader("Filters")

    col1, col2 = st.columns([1, 2])
    with col1:
        level_filter = st.selectbox("Level", ["All", "0", "1", "2", "3", "4"], index=0)

    usernames = load_usernames(level_filter)
    with col2:
        username_filter = st.selectbox("Username", usernames, index=0)

    df = load_leaderboard(level_filter, username_filter)

    st.subheader("Raw Data Preview")
    st.write("Rows:", len(df))
    st.write("Dtypes:", df.dtypes.astype(str).to_dict())
    
    if not df.empty:
        st.write("First row:", df.iloc[0].to_dict())
    
    st.dataframe(df, use_container_width=True)

    st.subheader("Sanity Check")
    if not df.empty:
        st.write("rank_no sample:", df["rank_no"].head(5).tolist())
        st.write("creator_name sample:", df["creator_name"].head(5).tolist())
        st.write("username sample:", df["username"].head(5).tolist())


render()