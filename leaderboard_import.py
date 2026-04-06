import pandas as pd
import streamlit as st
from psycopg2.extras import execute_values
from datetime import date
from db import get_connection

# ======================================================
# CONFIG
# ======================================================

SCHEMA_NAME   = "leaderboard"
TABLE_RAW     = "tiktok_go_video_transactions"
TABLE_SUMMARY = "tiktok_go_video_summary"

# CSV header → DB column.
# Adjust keys here if the exact CSV column headers differ.
CSV_TO_DB = {
    "Location industry":      "industry_source",
    "Post type":              "post_type",
    "Creator type":           "creator_type",
    "Post ID":                "item_id",
    "Post title":             "post_title",
    "Post date":              "item_create_date",
    "Duration":               "duration",
    "Task type":              "task_type",
    "Location ID":            "poi_id",
    "Location name":          "poi_name_en",
    "Location city":          "poi_l2_asci_name",
    "Merchant name":          "close_loop_merchant_name",
    "Creator name":           "uniq_id",
    "Creator ID":             "author_id",
    "Creator binding status": "creator_binding_status",
    "Creator city":           "creator_city",
    "Creator level":          "creator_level",
    "Sales value":            "fulfill_amount_usd",
    "Orders":                 "order_count",
    "Redemption amount":      "redemption_amount",
    "Redeemed orders":        "redeemed_orders",
    "Video views":            "poi_vv",
    "CTR":                    "ctr",
    "CVR":                    "cvr",
    "AOV":                    "aov",
    "Video completion":       "video_completion",
    "Like rate":              "like_rate",
    "Comment rate":           "comment_rate",
}

NUMERIC_COLS = [
    "fulfill_amount_usd", "order_count", "redemption_amount", "redeemed_orders",
    "poi_vv", "ctr", "cvr", "aov", "video_completion", "like_rate", "comment_rate",
]

META_COLS = [
    "industry_source", "post_type", "creator_type", "post_title", "item_create_date",
    "duration", "task_type", "poi_id", "poi_name_en", "poi_l2_asci_name",
    "close_loop_merchant_name", "uniq_id", "author_id",
    "creator_binding_status", "creator_city", "creator_level",
]

# Columns inserted into the raw transactions table (excludes id, imported_at, status_qc — DB defaults)
FINAL_COLUMNS = [
    "industry_source", "item_id", "post_type", "creator_type", "post_title",
    "item_create_date", "duration", "task_type",
    "poi_id", "poi_name_en", "poi_l2_asci_name", "close_loop_merchant_name",
    "uniq_id", "author_id", "creator_binding_status", "creator_city", "creator_level",
    "fulfill_amount_usd", "order_count", "redemption_amount", "redeemed_orders",
    "poi_vv", "ctr", "cvr", "aov", "video_completion", "like_rate", "comment_rate",
    "fulfill_amount_usd_weekly",
    "report_month", "report_week", "start_date", "cutoff_date",
]

SUMMARY_COLUMNS = [
    "industry_source", "author_id", "uniq_id", "poi_id",
    "total_post", "poi_vv", "ctr", "cvr",
    "fulfill_amount_usd", "fulfill_amount_usd_weekly", "order_count", "aov",
    "redemption_amount", "redeemed_orders", "video_completion", "like_rate", "comment_rate",
    "creator_level", "creator_city", "creator_binding_status",
    "report_month", "report_week", "start_date", "cutoff_date",
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


def deduplicate_df(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate by item_id — sum numeric cols, keep first for meta cols."""
    before = len(df)
    df["_id_safe"] = df["item_id"].fillna("__NULL__")

    agg_dict = {}
    for col in NUMERIC_COLS:
        if col in df.columns:
            agg_dict[col] = "sum"
    for col in META_COLS:
        if col in df.columns:
            agg_dict[col] = "first"

    deduped = df.groupby("_id_safe", as_index=False, sort=False).agg(agg_dict)
    deduped["item_id"] = deduped["_id_safe"].replace("__NULL__", None)
    deduped = deduped.drop(columns=["_id_safe"])
    df.drop(columns=["_id_safe"], inplace=True)

    after = len(deduped)
    if before != after:
        st.warning(f"⚠️ Deduplicated **{before - after}** duplicate rows (summed numeric values).")
    return deduped.reset_index(drop=True)


def load_and_transform_csv(uploaded_file) -> pd.DataFrame:
    df = pd.read_excel(uploaded_file, dtype={"Post ID": str})
    df = df.rename(columns=CSV_TO_DB)

    if "item_id" in df.columns:
        df["item_id"] = df["item_id"].astype(str).str.strip()

    if "item_create_date" in df.columns:
        df["item_create_date"] = pd.to_datetime(
            df["item_create_date"].astype(str).str.strip().str[:8],
            format="%Y%m%d",
            errors="coerce",
        ).dt.date

    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in META_COLS + ["item_id"]:
        if col not in df.columns:
            df[col] = None

    df = deduplicate_df(df)
    return df


# ======================================================
# RENDER
# ======================================================

def render():
    st.title("📊 TikTok Go Video Transaction Importer")

    st.subheader("Import Settings")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        month_options = generate_month_options()
        current_month_str = date.today().strftime("%Y-%m")
        default_idx = month_options.index(current_month_str) if current_month_str in month_options else 0
        selected_month = st.selectbox("Month", month_options, index=default_idx)

    with col2:
        selected_week = st.selectbox("Week", [1, 2, 3, 4, 5])

    with col3:
        start_date = st.date_input("Start Date", value=date.today().replace(day=1))

    with col4:
        cutoff_date = st.date_input("Cutoff Date", value=date.today())

    report_month_date = month_str_to_date(selected_month)
    st.info(f"📌 Importing: **{selected_month}** · **Week {selected_week}** · **{start_date}** → **{cutoff_date}**")

    uploaded_file = st.file_uploader("Upload Excel File", type=["xlsx"])
    if not uploaded_file:
        return

    try:
        final_df = load_and_transform_csv(uploaded_file)

        # Attach period metadata
        final_df["report_month"] = report_month_date
        final_df["report_week"]  = selected_week
        final_df["start_date"]   = start_date
        final_df["cutoff_date"]  = cutoff_date

        # Data is daily (not cumulative) — weekly value = current value directly, no delta needed
        final_df["fulfill_amount_usd_weekly"] = final_df["fulfill_amount_usd"]

        for col in FINAL_COLUMNS:
            if col not in final_df.columns:
                final_df[col] = None

        industries = final_df["industry_source"].dropna().unique().tolist()
        st.success(f"✅ Rows ready: **{len(final_df)}** · Industries: **{', '.join(industries)}**")

        with st.expander("🔍 Preview (first 20 rows)", expanded=False):
            preview_cols = ["industry_source", "item_id", "uniq_id", "poi_name_en",
                            "fulfill_amount_usd", "order_count", "item_create_date"]
            st.dataframe(
                final_df[[c for c in preview_cols if c in final_df.columns]].head(20),
                use_container_width=True,
            )

        # Safety check — should already be clean after dedup, but guard anyway
        dupes = final_df.duplicated(subset=["item_id"], keep=False)
        if dupes.any():
            st.error(f"❌ {dupes.sum()} duplicate item_id rows still exist. Cannot import.")
            st.dataframe(
                final_df[dupes][["item_id", "industry_source", "uniq_id", "fulfill_amount_usd"]].head(20)
            )
            return

        # ======================================================
        # IMPORT BUTTON
        # ======================================================
        if st.button("💾 Import to Database", type="primary"):
            conn = get_connection()
            try:
                # ── Step 1: Delete existing raw rows for this month/week/industry ──
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM {SCHEMA_NAME}.{TABLE_RAW} "
                        "WHERE report_month = %s AND report_week = %s AND industry_source = ANY(%s)",
                        (report_month_date, selected_week, industries),
                    )
                    deleted_raw = cur.rowcount
                if deleted_raw > 0:
                    st.warning(f"🗑️ Deleted **{deleted_raw}** existing raw rows for {selected_month} Week {selected_week}.")

                # ── Step 2: Insert raw rows ──
                insert_raw = f"""
                    INSERT INTO {SCHEMA_NAME}.{TABLE_RAW} ({", ".join(FINAL_COLUMNS)})
                    VALUES %s
                """
                raw_values = [
                    tuple(
                        None if (val is None or (isinstance(val, float) and pd.isna(val))) else val
                        for val in (row[col] for col in FINAL_COLUMNS)
                    )
                    for _, row in final_df.iterrows()
                ]
                with conn.cursor() as cur:
                    execute_values(cur, insert_raw, raw_values)

                # ── Step 3: Delete existing summary rows ──
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM {SCHEMA_NAME}.{TABLE_SUMMARY} "
                        "WHERE report_month = %s AND report_week = %s AND industry_source = ANY(%s)",
                        (report_month_date, selected_week, industries),
                    )
                    deleted_summary = cur.rowcount
                if deleted_summary > 0:
                    st.warning(f"🗑️ Deleted **{deleted_summary}** existing summary rows.")

                # ── Step 4: Count total_post (videos whose post_date is within start–cutoff) ──
                item_dates = pd.to_datetime(final_df["item_create_date"])
                final_df["_in_date_range"] = (
                    (item_dates >= pd.to_datetime(start_date)) &
                    (item_dates <= pd.to_datetime(cutoff_date))
                ).astype(int)

                # ── Step 5: Aggregate into summary ──
                summary_df = final_df.groupby(
                    ["industry_source", "author_id", "uniq_id", "poi_id"],
                    as_index=False,
                ).agg(
                    total_post                = ("_in_date_range",        "sum"),
                    poi_vv                    = ("poi_vv",                 "sum"),
                    ctr                       = ("ctr",                    "mean"),
                    cvr                       = ("cvr",                    "mean"),
                    fulfill_amount_usd        = ("fulfill_amount_usd",     "sum"),
                    fulfill_amount_usd_weekly = ("fulfill_amount_usd_weekly", "sum"),
                    order_count               = ("order_count",            "sum"),
                    aov                       = ("aov",                    "mean"),
                    redemption_amount         = ("redemption_amount",      "sum"),
                    redeemed_orders           = ("redeemed_orders",        "sum"),
                    video_completion          = ("video_completion",       "mean"),
                    like_rate                 = ("like_rate",              "mean"),
                    comment_rate              = ("comment_rate",           "mean"),
                    creator_level             = ("creator_level",          "first"),
                    creator_city              = ("creator_city",           "first"),
                    creator_binding_status    = ("creator_binding_status", "first"),
                )
                summary_df["report_month"] = report_month_date
                summary_df["report_week"]  = selected_week
                summary_df["start_date"]   = start_date
                summary_df["cutoff_date"]  = cutoff_date

                insert_summary = f"""
                    INSERT INTO {SCHEMA_NAME}.{TABLE_SUMMARY} ({", ".join(SUMMARY_COLUMNS)})
                    VALUES %s
                """
                summary_values = [
                    tuple(
                        None if (val is None or (isinstance(val, float) and pd.isna(val))) else val
                        for val in (row[col] for col in SUMMARY_COLUMNS)
                    )
                    for _, row in summary_df.iterrows()
                ]
                with conn.cursor() as cur:
                    execute_values(cur, insert_summary, summary_values)

                conn.commit()
                st.success(
                    f"✅ **{len(final_df)}** raw rows · **{len(summary_df)}** summary rows imported — "
                    f"**{selected_month}** Week **{selected_week}** · Cutoff **{cutoff_date}**"
                )

                st.subheader("📊 Import Summary")
                display = final_df.groupby("industry_source").agg(
                    videos          = ("item_id",           "count"),
                    total_sales_usd = ("fulfill_amount_usd", "sum"),
                    total_orders    = ("order_count",        "sum"),
                ).reset_index()
                st.dataframe(display, use_container_width=True, hide_index=True)

            except Exception as e:
                conn.rollback()
                st.error(f"❌ Import failed, rolled back. Error: {str(e)}")
            finally:
                conn.close()

    except Exception as e:
        st.error(f"❌ Error reading file: {str(e)}")
