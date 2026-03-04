import pandas as pd
import streamlit as st
import psycopg2
from psycopg2.extras import execute_values

# ======================================================
# DATABASE CONFIG (YOUR CONFIG)
# ======================================================

DB_USER = "postgres"
DB_PASSWORD = "4dtr33"
DB_HOST = "localhost"
DB_PORT = "5433"
DB_NAME = "adtree"

def get_connection():
    return psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME
    )

# ======================================================
# CONFIG
# ======================================================

SCHEMA_NAME = "leaderboard"
TABLE_NAME = "tiktok_go_video_transactions"

SHEETS = {
    "Accommodation  video transactio": "accommodation",
    "Attraction video transaction pe": "attraction",
    "FnB  video transaction performa": "fnb"
}

FINAL_COLUMNS = [
    "industry_source",
    "item_id",
    "item_url",
    "item_create_date",
    "item_mcn_name_fix",
    "author_id",
    "uniq_id",
    "author_actual_sales_power",
    "poi_id",
    "poi_name_en",
    "poi_l1_asci_name",
    "poi_l2_asci_name",
    "poi_vv",
    "ctr",
    "cvr",
    "pay_amount_usd",
    "fulfill_amount_usd",
    "order_count",
    "aov",
    "close_loop_merchant_name",
    "poi_item_publish_cnt_db"
]

# ======================================================
# STREAMLIT UI
# ======================================================

def render():
    st.title("📊 TikTok Go Video Transaction Importer")

    uploaded_file = st.file_uploader("Upload XLSX File", type=["xlsx"])


    if uploaded_file:

        try:
            xls = pd.ExcelFile(uploaded_file)
            all_data = []

            for sheet_name, industry_name in SHEETS.items():

                if sheet_name not in xls.sheet_names:
                    st.error(f"Sheet '{sheet_name}' not found in uploaded file.")
                    st.stop()

                df = pd.read_excel(xls, sheet_name=sheet_name)

                # ======================================================
                # STANDARDIZE COLUMN NAMES
                # ======================================================

                if industry_name in ["accommodation", "attraction"]:

                    df = df.rename(columns={
                        "(primary key)item_id": "item_id",
                        "item URL": "item_url",
                        "alliance_open_loop_pay_amount_dollar": "pay_amount_usd",
                        "alliance_open_loop_fulfill_amount_dollar": "fulfill_amount_usd",
                        "alliance_open_loop_pay_order_cnt": "order_count",
                        "AOV": "aov",
                        "CTR": "ctr",
                        "CVR": "cvr"
                    })

                    df["close_loop_merchant_name"] = None

                elif industry_name == "fnb":

                    df = df.rename(columns={
                        "(primary key)item_id": "item_id",
                        "item URL": "item_url",
                        "alliance_close_loop_pay_pay_amount_dollar": "pay_amount_usd",
                        "alliance_close_loop_fulfill_pay_amount_dollar": "fulfill_amount_usd",
                        "alliance_close_loop_pay_shop_order_cnt": "order_count",
                        "Pay AOV": "aov",
                        "Close Loop CVR - Supply POI Content Source": "cvr",
                        "CTR": "ctr",
                        "close_loop_has_service_merchant_names": "close_loop_merchant_name"
                    })

                # ======================================================
                # ADD INDUSTRY COLUMN
                # ======================================================

                df["industry_source"] = industry_name

                # ======================================================
                # DATA CLEANING
                # ======================================================

                numeric_cols = [
                    "author_actual_sales_power",
                    "poi_vv",
                    "ctr",
                    "cvr",
                    "pay_amount_usd",
                    "fulfill_amount_usd",
                    "order_count",
                    "aov",
                    "poi_item_publish_cnt_db"
                ]

                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")

                if "item_create_date" in df.columns:
                    df["item_create_date"] = pd.to_datetime(
                        df["item_create_date"],
                        errors="coerce"
                    )

                # ======================================================
                # ENSURE ALL REQUIRED COLUMNS EXIST
                # ======================================================

                for col in FINAL_COLUMNS:
                    if col not in df.columns:
                        df[col] = None

                df = df[FINAL_COLUMNS]

                all_data.append(df)

            # ======================================================
            # MERGE ALL SHEETS
            # ======================================================

            final_df = pd.concat(all_data, ignore_index=True)

            st.success(f"Total rows ready for insert: {len(final_df)}")

            st.dataframe(final_df.head())

            # ======================================================
            # INSERT BUTTON
            # ======================================================

            if st.button("Import to Database"):

                conn = get_connection()
                cursor = conn.cursor()

                insert_query = f"""
                    INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
                        {",".join(FINAL_COLUMNS)}
                    )
                    VALUES %s
                    ON CONFLICT (item_id, industry_source)
                    DO NOTHING
                """

                values = [tuple(row) for row in final_df.to_numpy()]

                execute_values(cursor, insert_query, values)

                conn.commit()
                cursor.close()
                conn.close()

                st.success("✅ Import completed successfully.")

        except Exception as e:
            st.error(f"Error occurred: {str(e)}")