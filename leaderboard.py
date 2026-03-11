import pandas as pd
import streamlit as st
import psycopg2
from psycopg2.extras import execute_values
from datetime import date

# ======================================================
# DATABASE CONFIG
# ======================================================

DB_USER = "postgres"
DB_PASSWORD = "4dtr33"
DB_HOST = "localhost"
DB_PORT = "5432"
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

# Columns that identify a unique row (GROUP BY key)
GROUP_KEY_COLS = [
    "item_id",
    "industry_source",
    "uniq_id",
    "author_id",
]

# Numeric columns → take MAX when deduplicating
NUMERIC_COLS = [
    "author_actual_sales_power",
    "poi_vv",
    "ctr",
    "cvr",
    "pay_amount_usd",
    "fulfill_amount_usd",
    "order_count",
    "aov",
    "poi_item_publish_cnt_db",
]

# Text/metadata columns → take first value
META_COLS = [
    "item_url",
    "item_create_date",
    "item_mcn_name_fix",
    "poi_id",
    "poi_name_en",
    "poi_l1_asci_name",
    "poi_l2_asci_name",
    "close_loop_merchant_name",
]

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
    "poi_item_publish_cnt_db",
    "report_month",
    "report_week",
    "cutoff_date",
    "fulfill_amount_usd_weekly",
]

# ======================================================
# HELPERS
# ======================================================

def generate_month_options():
    today = date.today()
    months = []
    for i in range(-5, 3):
        month = today.month + i
        year = today.year
        while month < 1:
            month += 12
            year -= 1
        while month > 12:
            month -= 12
            year += 1
        months.append(f"{year}-{month:02d}")
    return months


def month_str_to_date(month_str):
    year, month = map(int, month_str.split("-"))
    return date(year, month, 1)


def fetch_previous_cumulative(conn, report_month_date, report_week):
    """
    Fetch previous week's fulfill_amount_usd keyed by (uniq_id, industry_source).
    Week 1 → returns empty dict (baseline = 0).
    """
    if report_week == 1:
        return {}

    prev_week = report_week - 1
    sql = f"""
        SELECT uniq_id, industry_source, fulfill_amount_usd
        FROM {SCHEMA_NAME}.{TABLE_NAME}
        WHERE report_month = %s
        AND report_week = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (report_month_date, prev_week))
        rows = cur.fetchall()

    return {(r[0], r[1]): (r[2] or 0) for r in rows}


def deduplicate_df(df):
    """
    Deduplicate rows with same GROUP_KEY_COLS.
    Numeric cols → MAX, meta cols → first value.
    """
    agg_dict = {}

    for col in NUMERIC_COLS:
        if col in df.columns:
            agg_dict[col] = "max"

    for col in META_COLS:
        if col in df.columns:
            agg_dict[col] = "first"

    valid_keys = [k for k in GROUP_KEY_COLS if k in df.columns]
    deduped = df.groupby(valid_keys, as_index=False, dropna=False).agg(agg_dict)

    before = len(df)
    after = len(deduped)
    if before != after:
        st.warning(f"⚠️ Deduplicated {before - after} duplicate rows (kept MAX values).")

    return deduped


def load_and_transform_xlsx(uploaded_file):
    xls = pd.ExcelFile(uploaded_file)
    all_data = []

    for sheet_name, industry_name in SHEETS.items():
        if sheet_name not in xls.sheet_names:
            st.error(f"❌ Sheet '{sheet_name}' not found in uploaded file.")
            st.stop()

        df = pd.read_excel(xls, sheet_name=sheet_name)

        # ---- Column Standardization ----
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

        df["industry_source"] = industry_name

        # ---- Clean Numerics ----
        for col in NUMERIC_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # ---- Ensure all base columns exist ----
        base_cols = GROUP_KEY_COLS + NUMERIC_COLS + META_COLS
        for col in base_cols:
            if col not in df.columns:
                df[col] = None

        all_data.append(df)

    merged = pd.concat(all_data, ignore_index=True)

    # ---- Deduplicate within the XLSX ----
    merged = deduplicate_df(merged)

    return merged


# ======================================================
# RENDER
# ======================================================

def render():
    st.title("📊 TikTok Go Video Transaction Importer")

    # --------------------------------------------------
    # IMPORT SETTINGS
    # --------------------------------------------------
    st.subheader("Import Settings")

    col1, col2, col3 = st.columns(3)

    with col1:
        month_options = generate_month_options()
        current_month_str = date.today().strftime("%Y-%m")
        default_idx = month_options.index(current_month_str) if current_month_str in month_options else 0
        selected_month = st.selectbox("Month", month_options, index=default_idx)

    with col2:
        selected_week = st.selectbox("Week", [1, 2, 3, 4, 5])

    with col3:
        cutoff_date = st.date_input("Cutoff Date", value=date.today())

    report_month_date = month_str_to_date(selected_month)

    st.info(
        f"📌 Importing: **{selected_month}** · **Week {selected_week}** · "
        f"Cutoff **{cutoff_date}**"
    )

    # --------------------------------------------------
    # FILE UPLOAD
    # --------------------------------------------------
    uploaded_file = st.file_uploader("Upload XLSX File", type=["xlsx"])

    if not uploaded_file:
        return

    try:
        # ---- Load, Transform & Deduplicate ----
        final_df = load_and_transform_xlsx(uploaded_file)

        # ---- Attach import metadata ----
        final_df["report_month"] = report_month_date
        final_df["report_week"] = selected_week
        final_df["cutoff_date"] = cutoff_date
        final_df["item_create_date"] = cutoff_date

        # ---- Preview ----
        st.success(f"✅ Rows ready for import: **{len(final_df)}**")

        with st.expander("🔍 Preview Data (first 20 rows)", expanded=False):
            preview_cols = [
                "item_id", "industry_source", "uniq_id",
                "fulfill_amount_usd", "pay_amount_usd", "order_count"
            ]
            st.dataframe(final_df[preview_cols].head(20), use_container_width=True)

        # --------------------------------------------------
        # IMPORT BUTTON
        # --------------------------------------------------
        if st.button("💾 Import to Database", type="primary"):

            conn = get_connection()

            try:
                # ---- Step 1: Fetch previous week cumulative ----
                prev_cumulative = fetch_previous_cumulative(
                    conn, report_month_date, selected_week
                )

                if selected_week == 1:
                    st.info("ℹ️ Week 1 — weekly value = cumulative (no previous baseline).")
                else:
                    matched = sum(
                        1 for _, row in final_df.iterrows()
                        if (row["uniq_id"], row["industry_source"]) in prev_cumulative
                    )
                    st.info(
                        f"ℹ️ Week {selected_week} — delta vs Week {selected_week - 1}. "
                        f"Matched **{matched}/{len(final_df)}** rows with previous week."
                    )

                # ---- Step 2: Calculate weekly delta ----
                def calc_weekly(row):
                    key = (row["uniq_id"], row["industry_source"])
                    prev = prev_cumulative.get(key, 0)
                    current = row["fulfill_amount_usd"] if pd.notna(row["fulfill_amount_usd"]) else 0
                    return max(current - prev, 0)

                final_df["fulfill_amount_usd_weekly"] = final_df.apply(calc_weekly, axis=1)

                # ---- Step 3: Delete existing rows for same month + week ----
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM {SCHEMA_NAME}.{TABLE_NAME} "
                        f"WHERE report_month = %s AND report_week = %s",
                        (report_month_date, selected_week)
                    )
                    deleted_count = cur.rowcount

                if deleted_count > 0:
                    st.warning(
                        f"🗑️ Deleted **{deleted_count}** existing rows for "
                        f"{selected_month} Week {selected_week}."
                    )

                # ---- Step 4: Insert ----
                insert_query = f"""
                    INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
                        {", ".join(FINAL_COLUMNS)}
                    )
                    VALUES %s
                """

                values = [
                    tuple(
                        row[col] if pd.notna(row[col]) else None
                        for col in FINAL_COLUMNS
                    )
                    for _, row in final_df.iterrows()
                ]

                with conn.cursor() as cur:
                    execute_values(cur, insert_query, values)

                conn.commit()

                st.success(
                    f"✅ Import completed! "
                    f"**{len(final_df)}** rows inserted for "
                    f"**{selected_month}** · Week **{selected_week}** · "
                    f"Cutoff **{cutoff_date}**"
                )

                # ---- Summary stats ----
                st.subheader("📊 Import Summary")
                summary = final_df.groupby("industry_source").agg(
                    rows=("uniq_id", "count"),
                    total_fulfill_cumulative=("fulfill_amount_usd", "sum"),
                    total_fulfill_weekly=("fulfill_amount_usd_weekly", "sum"),
                ).reset_index()
                st.dataframe(summary, use_container_width=True, hide_index=True)

            except Exception as e:
                conn.rollback()
                st.error(f"❌ Import failed, rolled back. Error: {str(e)}")

            finally:
                conn.close()

    except Exception as e:
        st.error(f"❌ Error reading file: {str(e)}")