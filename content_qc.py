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

@st.cache_data
def load_data():
    """Fetches the QC dataset from the database."""
    conn = get_connection()
    
    # Joining columns for the SELECT statement
    query = f"""
        SELECT {", ".join(EXPORT_COLS)}
        FROM {SCHEMA}.{TABLE}
        ORDER BY item_create_date DESC
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df

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
        df = load_data()
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
    st.header("2️⃣ Upload QC Result")

    uploaded_file = st.file_uploader(
        "Upload CSV with updated status_qc",
        type=["csv"]
    )

    if uploaded_file:
        df_upload = pd.read_csv(uploaded_file)
        
        # Validation
        required_cols = ["id", "status_qc"]
        missing = [c for c in required_cols if c not in df_upload.columns]

        if missing:
            st.error(f"Missing required columns: {missing}")
            return

        st.success(f"File loaded successfully with {len(df_upload)} rows.")
        st.dataframe(df_upload.head())

        if st.button("🚀 Update Database"):
            try:
                conn = get_connection()
                cur = conn.cursor()

                # Bulk update using PostgreSQL VALUES syntax
                update_query = f"""
                UPDATE {SCHEMA}.{TABLE} t
                SET status_qc = data.status_qc
                FROM (VALUES %s) AS data(id, status_qc)
                WHERE t.id = data.id
                """

                # Prepare data tuple for execute_values
                data_to_update = list(
                    df_upload[["id", "status_qc"]]
                    .itertuples(index=False, name=None)
                )

                execute_values(cur, update_query, data_to_update)

                conn.commit()
                cur.close()
                conn.close()

                st.success("✅ Database updated successfully!")
                # Clear cache so the table refreshes on next load
                st.cache_data.clear()
                
            except Exception as e:
                st.error(f"Database error: {e}")