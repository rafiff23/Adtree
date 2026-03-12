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
TABLE_NAME  = "tiktok_go_video_transactions"

SHEETS = {
    "Accommodation  video transactio": "accommodation",
    "Attraction video transaction pe": "attraction",
    "FnB  video transaction performa": "fnb"
}

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

META_COLS = [
    "item_url",
    "item_create_date",
    "item_mcn_name_fix",
    "poi_id",
    "poi_name_en",
    "poi_l1_asci_name",
    "poi_l2_asci_name",
    "close_loop_merchant_name",
    "author_id",
    "uniq_id",
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
        year  = today.year
        while month < 1:  month += 12; year -= 1
        while month > 12: month -= 12; year += 1
        months.append(f"{year}-{month:02d}")
    return months

def month_str_to_date(month_str):
    year, month = map(int, month_str.split("-"))
    return date(year, month, 1)

def fetch_previous_cumulative(conn, report_month_date, report_week):
    if report_week == 1:
        return {}
    sql = f"""
        SELECT uniq_id, industry_source, fulfill_amount_usd
        FROM {SCHEMA_NAME}.{TABLE_NAME}
        WHERE report_month = %s AND report_week = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (report_month_date, report_week - 1))
        rows = cur.fetchall()
    return {(r[0], r[1]): (r[2] or 0) for r in rows}


def deduplicate_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Per sheet, duplicates share the same item_id + industry_source.
    Strategy: groupby (item_id, industry_source), SUM numeric cols, keep first for meta cols.
    This correctly accumulates values across duplicate rows.
    """
    before = len(df)

    # Cast item_id to string to avoid float/int mixing
    df["item_id"] = df["item_id"].astype(str).str.strip()
    df["item_id"] = df["item_id"].replace("nan", None).replace("", None)

    # Temp safe keys for groupby (avoids dropping nulls)
    df["_item_id_safe"]   = df["item_id"].fillna("__NULL__")
    df["_industry_safe"]  = df["industry_source"].fillna("__NULL__")

    agg_dict = {}
    for col in NUMERIC_COLS:
        if col in df.columns:
            agg_dict[col] = "sum"
    for col in META_COLS:
        if col in df.columns:
            agg_dict[col] = "first"

    deduped = (
        df.groupby(["_item_id_safe", "_industry_safe"], as_index=False, sort=False)
        .agg(agg_dict)
    )

    # Restore original key values
    deduped["item_id"]        = deduped["_item_id_safe"].replace("__NULL__", None)
    deduped["industry_source"] = deduped["_industry_safe"].replace("__NULL__", None)
    deduped = deduped.drop(columns=["_item_id_safe", "_industry_safe"])
    df.drop(columns=["_item_id_safe", "_industry_safe"], inplace=True)

    after = len(deduped)
    if before != after:
        st.warning(f"⚠️ Deduplicated **{before - after}** duplicate rows (summed numeric values).")

    return deduped.reset_index(drop=True)


def load_and_transform_xlsx(uploaded_file):
    xls = pd.ExcelFile(uploaded_file)
    all_data = []

    for sheet_name, industry_name in SHEETS.items():
        if sheet_name not in xls.sheet_names:
            st.error(f"❌ Sheet '{sheet_name}' not found in uploaded file.")
            st.stop()

        # Read item_id as string to avoid float conversion
        df = pd.read_excel(xls, sheet_name=sheet_name, dtype={"(primary key)item_id": str})

        # ---- Column rename ----
        if industry_name in ["accommodation", "attraction"]:
            df = df.rename(columns={
                "(primary key)item_id":                     "item_id",
                "item URL":                                  "item_url",
                "alliance_open_loop_pay_amount_dollar":      "pay_amount_usd",
                "alliance_open_loop_fulfill_amount_dollar":  "fulfill_amount_usd",
                "alliance_open_loop_pay_order_cnt":          "order_count",
                "AOV": "aov", "CTR": "ctr", "CVR": "cvr"
            })
            df["close_loop_merchant_name"] = None

        elif industry_name == "fnb":
            df = df.rename(columns={
                "(primary key)item_id":                          "item_id",
                "item URL":                                       "item_url",
                "alliance_close_loop_pay_pay_amount_dollar":     "pay_amount_usd",
                "alliance_close_loop_fulfill_pay_amount_dollar": "fulfill_amount_usd",
                "alliance_close_loop_pay_shop_order_cnt":        "order_count",
                "Pay AOV":                                        "aov",
                "Close Loop CVR - Supply POI Content Source":     "cvr",
                "CTR":                                            "ctr",
                "close_loop_has_service_merchant_names":          "close_loop_merchant_name"
            })

        df["industry_source"] = industry_name

        # ---- Clean numerics ----
        for col in NUMERIC_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # ---- Ensure all columns exist ----
        for col in META_COLS + ["item_id"]:
            if col not in df.columns:
                df[col] = None

        # ---- Deduplicate within this sheet ----
        df = deduplicate_df(df)

        all_data.append(df)

    return pd.concat(all_data, ignore_index=True)


# ======================================================
# RENDER
# ======================================================

def render():
    st.title("📊 TikTok Go Video Transaction Importer")

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

    st.info(f"📌 Importing: **{selected_month}** · **Week {selected_week}** · Cutoff **{cutoff_date}**")

    uploaded_file = st.file_uploader("Upload XLSX File", type=["xlsx"])
    if not uploaded_file:
        return

    try:
        final_df = load_and_transform_xlsx(uploaded_file)

        # ---- Attach metadata ----
        final_df["report_month"]             = report_month_date
        final_df["report_week"]              = selected_week
        final_df["cutoff_date"]              = cutoff_date
        final_df["item_create_date"]         = cutoff_date
        final_df["fulfill_amount_usd_weekly"] = None  # calculated before insert

        # Ensure all final columns exist
        for col in FINAL_COLUMNS:
            if col not in final_df.columns:
                final_df[col] = None

        st.success(f"✅ Rows ready for import: **{len(final_df)}**")

        with st.expander("🔍 Preview (first 20 rows)", expanded=False):
            preview_cols = ["item_id", "industry_source", "uniq_id", "fulfill_amount_usd", "pay_amount_usd", "order_count"]
            st.dataframe(final_df[preview_cols].head(20), use_container_width=True)

        # ---- Safety check: show remaining dupes if any ----
        dupes = final_df.duplicated(subset=["item_id", "industry_source"], keep=False)
        if dupes.any():
            st.error(f"❌ {dupes.sum()} duplicate (item_id + industry_source) rows still exist. Cannot import.")
            st.dataframe(final_df[dupes][["item_id", "industry_source", "uniq_id", "fulfill_amount_usd"]].head(20))
            return

        # ======================================================
        # IMPORT BUTTON
        # ======================================================
        if st.button("💾 Import to Database", type="primary"):
            conn = get_connection()
            try:
                # Step 1: Previous week cumulative
                prev_cumulative = fetch_previous_cumulative(conn, report_month_date, selected_week)

                if selected_week == 1:
                    st.info("ℹ️ Week 1 — weekly value = cumulative (no previous baseline).")
                else:
                    matched = sum(
                        1 for _, row in final_df.iterrows()
                        if (row["uniq_id"], row["industry_source"]) in prev_cumulative
                    )
                    st.info(f"ℹ️ Week {selected_week} — delta vs Week {selected_week - 1}. Matched **{matched}/{len(final_df)}** rows.")

                # Step 2: Calculate weekly delta
                def calc_weekly(row):
                    key     = (row["uniq_id"], row["industry_source"])
                    prev    = prev_cumulative.get(key, 0)
                    current = row["fulfill_amount_usd"] if pd.notna(row["fulfill_amount_usd"]) else 0
                    return max(current - prev, 0)

                final_df["fulfill_amount_usd_weekly"] = final_df.apply(calc_weekly, axis=1)

                # Step 3: Delete existing rows for same month + week
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM {SCHEMA_NAME}.{TABLE_NAME} WHERE report_month = %s AND report_week = %s",
                        (report_month_date, selected_week)
                    )
                    deleted_count = cur.rowcount

                if deleted_count > 0:
                    st.warning(f"🗑️ Deleted **{deleted_count}** existing rows for {selected_month} Week {selected_week}.")

                # Step 4: Insert
                insert_query = f"""
                    INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} ({", ".join(FINAL_COLUMNS)})
                    VALUES %s
                """
                values = [
                    tuple(
                        None if (val is None or (isinstance(val, float) and pd.isna(val))) else val
                        for val in (row[col] for col in FINAL_COLUMNS)
                    )
                    for _, row in final_df.iterrows()
                ]

                with conn.cursor() as cur:
                    execute_values(cur, insert_query, values)

                conn.commit()

                st.success(
                    f"✅ **{len(final_df)}** rows inserted for "
                    f"**{selected_month}** · Week **{selected_week}** · Cutoff **{cutoff_date}**"
                )

                # Summary
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
