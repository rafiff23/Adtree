import os
import pandas as pd
import streamlit as st
import psycopg2
from datetime import date
from zoneinfo import ZoneInfo

# -----------------------------
# CONFIG
# -----------------------------
SCHEMA = "leaderboard"
CAMPAIGN_NAME = "Newly Creator Campaign - Januari 2026"

TABLE_SUB = f"{SCHEMA}.campaign_submissions"
TABLE_CAMPAIGN = f"{SCHEMA}.campaigns"

WIB = ZoneInfo("Asia/Jakarta")

# -----------------------------
# DB CONNECTION
# -----------------------------
def get_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB", "adtree"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "4dtr33"),
    )

# -----------------------------
# GET CAMPAIGN ID
# -----------------------------
@st.cache_data(ttl=300)
def get_campaign_id():
    conn = get_conn()
    try:
        df = pd.read_sql(
            f"""
            SELECT campaign_id
            FROM {TABLE_CAMPAIGN}
            WHERE campaign_name = %s
            LIMIT 1
            """,
            conn,
            params=[CAMPAIGN_NAME],
        )
    finally:
        conn.close()

    if df.empty:
        raise ValueError("Campaign not found")

    return int(df.loc[0, "campaign_id"])

# -----------------------------
# LOAD LEADERBOARD DATA
# -----------------------------
@st.cache_data(ttl=60)
def load_leaderboard(campaign_id, start_date, end_date):
    conn = get_conn()
    try:
        df = pd.read_sql(
            f"""
            WITH base AS (
                SELECT
                    tiktok_id,
                    COALESCE(NULLIF(full_name, ''), tiktok_id) AS full_name,
                    post_link
                FROM {TABLE_SUB}
                WHERE campaign_id = %s
                  AND submitted_at::date BETWEEN %s AND %s
                  AND tiktok_id IS NOT NULL
                  AND tiktok_id <> ''
            ),
            agg AS (
                SELECT
                    tiktok_id,
                    COUNT(post_link) AS total_post
                FROM base
                GROUP BY tiktok_id
            ),
            ranked AS (
                SELECT
                    a.tiktok_id,
                    a.total_post,
                    RANK() OVER (ORDER BY a.total_post DESC) AS rank
                FROM agg a
            )
            SELECT
                r.rank,
                b.full_name,
                b.tiktok_id,
                b.post_link,
                r.total_post
            FROM base b
            JOIN ranked r
              ON b.tiktok_id = r.tiktok_id
            ORDER BY r.rank, b.tiktok_id;
            """,
            conn,
            params=[campaign_id, start_date, end_date],
        )
    finally:
        conn.close()

    return df


# -----------------------------
# UI
# -----------------------------
def render():
    st.title("Newly Creator Campaign Leaderboard")

    # Period selector
    period = st.radio(
        "Select Period",
        ["Week 3 (15–23 Jan)", "Week 4 (24–31 Jan)"],
        horizontal=True,
    )

    if period.startswith("Week 3"):
        start_date = date(2026, 1, 15)
        end_date = date(2026, 1, 23)
    else:
        start_date = date(2026, 1, 24)
        end_date = date(2026, 1, 31)

    try:
        campaign_id = get_campaign_id()
    except Exception as e:
        st.error(str(e))
        return

    df = load_leaderboard(campaign_id, start_date, end_date)

    if df.empty:
        st.warning("No submissions found for this period.")
        return

    # Optional: rank creators by total_post
    df["rank"] = (
        df[["tiktok_id", "total_post"]]
        .drop_duplicates()
        .sort_values("total_post", ascending=False)
        .reset_index(drop=True)
        .index + 1
    )

    # Final table
    df_table = df[
        [
            "rank",
            "full_name",
            "tiktok_id",
            "post_link",
            "total_post",
        ]
    ]

    st.dataframe(df_table, use_container_width=True)

# -----------------------------
# RUN
# -----------------------------
