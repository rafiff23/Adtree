import streamlit as st
import pandas as pd
from psycopg2.extras import execute_values
from db import get_connection

# Configuration constants
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

def fetch_content_submissions():
    conn = get_connection()
    sql = f"""
        SELECT {", ".join(EXPORT_COLS)}
        FROM {SCHEMA}.{TABLE}
        ORDER BY item_create_date DESC
    """
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                # ✅ Fix: use cursor.description to extract real column names
                columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=columns)  # ✅ return DataFrame directly
    finally:
        conn.close()

def render():
    """Main UI function called by creator.py"""
    st.title("📥 TikTok GO QC Tool")

    st.write(
        """
        **Workflow:**
        1. Download the dataset below.
        2. Perform QC in Google Sheets or Excel.
        3. Update the **status_qc** column.
        4. Export your changes as a CSV.
        5. Upload the CSV back here to update the database.
        """
    )

    # --- Section 1: Download ---
    st.header("1️⃣ Download Data From Database")
    
    try:
        df = fetch_content_submissions() # Fetch data from content_submissions view
        st.write(f"Total rows available: **{len(df):,}**")
        st.dataframe(df.head(20))

        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Download CSV for QC",
            data=csv,
            file_name="tiktok_go_qc_download.csv",
            mime="text/csv"
        )
    except Exception as e:
        st.error(f"Error loading data: {e}")

    # --- Section 2: Upload ---
    st.header("2️⃣ Upload QC Result2")

    uploaded_file = st.file_uploader(
        "Upload CSV with updated status_qc",
        type=["csv"]
    )

    if uploaded_file:

        df_upload = pd.read_csv(uploaded_file, dtype=str)

        df_upload["id"] = df_upload["id"].str.strip()
        df_upload["status_qc"] = df_upload["status_qc"].replace("", None)

        required_cols = ["id", "status_qc"]
        missing = [c for c in required_cols if c not in df_upload.columns]

        if missing:
            st.error(f"Missing required columns: {missing}")
            return

        # keep only numeric ids
        df_upload = df_upload[df_upload["id"].str.match(r"^\d+$")]

        st.success(f"File loaded successfully with {len(df_upload)} rows.")
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

            data_to_update = [
                (int(row.id), row.status_qc)
                for row in df_upload.itertuples(index=False)
            ]
            bad_ids = []

            for row in df_upload["id"]:
                try:
                    val = int(row)
                    if val > 9223372036854775807:
                        bad_ids.append(row)
                except:
                    bad_ids.append(row)

            if bad_ids:
                st.error(f"Invalid IDs detected: {bad_ids[:10]}")
                st.stop()
            execute_values(cur, update_query, data_to_update)

            conn.commit()
            cur.close()
            conn.close()

            st.success("✅ Database updated successfully!")