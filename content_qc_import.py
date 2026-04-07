#!/usr/bin/env python3
"""
Standalone Content QC importer.
Run:  python content_qc_import.py
"""

import os
import re
import sys

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values


# ─────────────────────────────────────────────────────────────────────────────
# DB connection
# ─────────────────────────────────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5433"),
        dbname=os.getenv("PG_DB", "adtree"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "4dtr33"),
        cursor_factory=RealDictCursor,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Column mapping
# ─────────────────────────────────────────────────────────────────────────────

_METRIC_COLS = [
    "creator_level", "sales_value", "orders", "redemption_amount",
    "redeemed_orders", "video_views", "ctr", "cvr", "aov",
    "video_completion_rate", "like_rate", "comment_rate",
]

_ALL_DATA_COLS = [
    "post_id",
    "location_industry", "post_type", "creator_type", "post_title",
    "post_date", "duration", "task_type", "location_id", "location_name",
    "location_city", "merchant_name", "creator_name", "creator_id",
    "creator_binding", "creator_city",
] + _METRIC_COLS

_CSV_TO_DB = {
    "location_indu":                "location_industry",
    "location_industry":            "location_industry",
    "post_type":                    "post_type",
    "creator_type":                 "creator_type",
    "post_id":                      "post_id",
    "post_title":                   "post_title",
    "post_date":                    "post_date",
    "duration":                     "duration",
    "task_type":                    "task_type",
    "location_id":                  "location_id",
    "location_nam":                 "location_name",
    "location_name":                "location_name",
    "location_city":                "location_city",
    "merchant_nan":                 "merchant_name",
    "merchant_name":                "merchant_name",
    "creator_name":                 "creator_name",
    "creator_id":                   "creator_id",
    "creator_bindi":                "creator_binding",
    "creator_binding":              "creator_binding",
    "creator_binding_status":       "creator_binding",
    "creator_city":                 "creator_city",
    "creator_level":                "creator_level",
    "sales_value":                  "sales_value",
    "orders":                       "orders",
    "redemption_a":                 "redemption_amount",
    "redemption_amount":            "redemption_amount",
    "redeemed_ord":                 "redeemed_orders",
    "redeemed_orders":              "redeemed_orders",
    "video_views":                  "video_views",
    "ctr":                          "ctr",
    "cvr":                          "cvr",
    "aov":                          "aov",
    "video_comple":                 "video_completion_rate",
    "video_completion_rate":        "video_completion_rate",
    "like_rate":                    "like_rate",
    "comment_rate":                 "comment_rate",
}


# ─────────────────────────────────────────────────────────────────────────────
# Parsing helpers
# ─────────────────────────────────────────────────────────────────────────────

def _norm_col(col: str) -> str:
    return col.lower().strip().replace(" ", "_").replace("-", "_")


def _parse_post_date(val):
    s = str(val).strip()
    if re.match(r"^\d{8}$", s):
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    try:
        return pd.to_datetime(s).strftime("%Y-%m-%d")
    except Exception:
        return None


def _parse_creator_level(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    m = re.search(r"\d+", str(val))
    return int(m.group()) if m else None


def prepare_df(df_raw: pd.DataFrame):
    rename, unmapped = {}, []
    for col in df_raw.columns:
        norm = _norm_col(col)
        if norm in _CSV_TO_DB:
            rename[col] = _CSV_TO_DB[norm]
        else:
            unmapped.append(col)

    df = df_raw.rename(columns=rename)
    known = set(_CSV_TO_DB.values())
    df = df[[c for c in df.columns if c in known]].copy()

    if "post_date" in df.columns:
        df["post_date"] = df["post_date"].apply(_parse_post_date)

    if "creator_level" in df.columns:
        df["creator_level"] = df["creator_level"].apply(_parse_creator_level)

    num_cols = [c for c in _METRIC_COLS if c in df.columns and c != "creator_level"]
    for col in num_cols:
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", "").str.replace("%", "").str.strip(),
            errors="coerce",
        )

    return df, unmapped


# ─────────────────────────────────────────────────────────────────────────────
# Upsert
# ─────────────────────────────────────────────────────────────────────────────

def upsert_posts(rows: list):
    if not rows:
        return 0, 0

    cols = [c for c in _ALL_DATA_COLS if c in rows[0]]
    metrics_present = [c for c in _METRIC_COLS if c in cols]

    col_list   = ", ".join(cols)
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in metrics_present)
    if update_set:
        update_set += ", metrics_updated_at = NOW()"
    else:
        update_set = "metrics_updated_at = NOW()"

    sql = f"""
        INSERT INTO public.content_qc_posts ({col_list}, imported_at, metrics_updated_at)
        VALUES %s
        ON CONFLICT (post_id) DO UPDATE
            SET {update_set}
        RETURNING (xmax = 0) AS is_insert
    """

    def clean(row):
        return tuple(
            None if isinstance(row.get(c), float) and pd.isna(row.get(c)) else row.get(c)
            for c in cols
        )

    # execute_values expects a template with the extra literal NOW() columns
    template = "(" + ", ".join(["%s"] * len(cols)) + ", NOW(), NOW())"

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, [clean(r) for r in rows], template=template)
                results = cur.fetchall()
                inserted = sum(1 for r in results if r["is_insert"])
                updated  = len(results) - inserted
        return inserted, updated
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=== Content QC Importer ===\n")

    # Ask for file path
    file_path = input("Excel file path (.xlsx): ").strip().strip("'\"")
    if not file_path:
        print("No file provided. Exiting.")
        sys.exit(0)
    if not os.path.exists(file_path):
        print(f"ERROR: File not found: {file_path}")
        sys.exit(1)

    # Ask for sheet name
    sheet = input("Sheet name [Data]: ").strip() or "Data"

    # Load
    print(f"\nReading '{file_path}' (sheet: {sheet}) …")
    try:
        df_raw = pd.read_excel(file_path, sheet_name=sheet, dtype=str)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    print(f"  Loaded {len(df_raw):,} rows × {len(df_raw.columns)} columns")

    # Prepare
    df, unmapped = prepare_df(df_raw)

    if unmapped:
        print(f"  WARNING: Unrecognised columns (ignored): {unmapped}")

    if "post_id" not in df.columns:
        print("ERROR: 'Post ID' column not found in file.")
        sys.exit(1)

    before = len(df)
    df = df[df["post_id"].notna() & (df["post_id"].str.strip() != "")].copy()
    df["post_id"] = df["post_id"].str.strip()
    if len(df) < before:
        print(f"  WARNING: {before - len(df)} row(s) dropped (empty Post ID).")

    if df.empty:
        print("ERROR: No valid data to import.")
        sys.exit(1)

    print(f"  {len(df):,} rows ready.\n")

    # Preview
    print("Preview (first 5 rows):")
    print(df[["post_id", "post_title", "post_date", "creator_name"]].head(5).to_string(index=False))
    print()

    confirm = input("Import to database? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        sys.exit(0)

    # Upsert
    rows = [
        {k: (None if isinstance(v, float) and pd.isna(v) else v)
         for k, v in row.items()}
        for row in df.to_dict("records")
    ]

    print("Importing …")
    try:
        inserted, updated = upsert_posts(rows)
    except Exception as e:
        print(f"ERROR: Import failed: {e}")
        sys.exit(1)

    print(f"\nDone!  {inserted:,} inserted · {updated:,} updated.")


if __name__ == "__main__":
    main()
