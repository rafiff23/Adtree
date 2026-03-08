import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from db import get_connection

# ---------------------------------------
# CONFIG
# ---------------------------------------

SCHEMA = "leaderboard"
TABLE = "tiktok_go_video_transactions"

EXPORT_COLS = [
    "id",
    "industry_source",
    "item_url",
    "item_create_date",
    "uniq_id",
    "status_qc"
]

# ---------------------------------------
# PAGE TITLE
# ---------------------------------------

st.title("📥 TikTok GO QC Tool")

st.write(
"""
Workflow:

1. Download the dataset
2. Perform QC in Google Sheets
3. Update the **status_qc** column
4. Export as CSV
5. Upload back here to update the database
"""
)

# ---------------------------------------
# DOWNLOAD DATA
# ---------------------------------------

def render():
    st.header("1️⃣ Download Data From Database")


    @st.cache_data
    def load_data():
        conn = get_connection()
        query = f"""
            SELECT {", ".join(EXPORT_COLS)}
            FROM {SCHEMA}.{TABLE}
            ORDER BY item_create_date DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df


    df = load_data()

    st.write(f"Total rows: **{len(df):,}**")

    st.dataframe(df.head(20))

    csv = df.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="⬇️ Download CSV",
        data=csv,
        file_name="tiktok_go_qc_download.csv",
        mime="text/csv"
    )

    # ---------------------------------------
    # IMPORT CSV
    # ---------------------------------------

    st.header("2️⃣ Upload QC Result")

    uploaded_file = st.file_uploader(
        "Upload CSV with updated status_qc",
        type=["csv"]
    )

    if uploaded_file:

        df_upload = pd.read_csv(uploaded_file)

        required_cols = ["id", "status_qc"]

        missing = [c for c in required_cols if c not in df_upload.columns]

        if missing:
            st.error(f"Missing required columns: {missing}")
            st.stop()

        st.success(f"File loaded with {len(df_upload)} rows")

        st.dataframe(df_upload.head())

        if st.button("🚀 Update Database"):

            conn = get_connection()
            cur = conn.cursor()

            update_query = f"""
            UPDATE {SCHEMA}.{TABLE} t
            SET status_qc = data.status_qc
            FROM (VALUES %s) AS data(id, status_qc)
            WHERE t.id = data.id
            """

            data = list(
                df_upload[["id", "status_qc"]]
                .itertuples(index=False, name=None)
            )

            execute_values(
                cur,
                update_query,
                data
            )

            conn.commit()
            cur.close()
            conn.close()

            st.success("✅ Database updated successfully!")