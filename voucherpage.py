import re
import pandas as pd
import streamlit as st
from psycopg2.extras import execute_values
from db import get_connection

# ======================================================
# CONFIG
# ======================================================

SCHEMA_NAME = "voucher_campaign"
TABLE_NAME = "campaign_registrations"

REQUIRED_COLS = [
    "Username",
    "Nomor Telpon",
    "Lokasi Outlet",
    "Code Voucher Oden",
    "Code Voucher Tea Series",
    "Code Voucher Matcha Series"
]

def full_table():
    return f"{SCHEMA_NAME}.{TABLE_NAME}"

# ======================================================
# CLEANERS
# ======================================================

def clean_phone(v):
    if pd.isna(v):
        return None

    s = str(v).strip()
    s = re.sub(r"\D", "", s)

    if s.startswith("62"):
        s = "0" + s[2:]
    elif s.startswith("8"):
        s = "0" + s

    if s == "":
        return None

    return s


def clean_text(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s == "" or s == ".":
        return None
    return s


def pick_col(df, name):
    name = name.strip().lower()

    for col in df.columns:
        col_clean = re.sub(r"\s+", " ", col.lower()).strip()
        if name in col_clean:
            return col

    raise ValueError(f"Missing required column: {name}")


# ======================================================
# NORMALIZER
# ======================================================

def normalize(df):
    out = pd.DataFrame()

    out["username"] = df[pick_col(df, "Username")].apply(clean_text)
    out["nomor_telpon"] = df[pick_col(df, "Nomor Telpon")].apply(clean_phone)
    out["lokasi_outlet"] = df[pick_col(df, "Lokasi Outlet")].apply(clean_text)
    out["code_voucher_oden"] = df[pick_col(df, "Code Voucher Oden")].apply(clean_text)
    out["code_voucher_tea_series"] = df[pick_col(df, "Code Voucher Tea Series")].apply(clean_text)
    out["code_voucher_matcha_series"] = df[pick_col(df, "Code Voucher Matcha Series")].apply(clean_text)

    return out


# ======================================================
# DDL
# ======================================================

def ensure_table(cur):
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {full_table()} (
            username TEXT,
            nomor_telpon TEXT,
            lokasi_outlet TEXT,
            code_voucher_oden TEXT,
            code_voucher_tea_series TEXT,
            code_voucher_matcha_series TEXT,
            imported_at TIMESTAMPTZ DEFAULT NOW(),
            last_updated TIMESTAMPTZ DEFAULT NOW()
        );
    """)


# ======================================================
# UI
# ======================================================

def render():
    st.set_page_config(page_title="Voucher Import", page_icon="🎟️")
    st.title("🎟️ Voucher Campaign Import")

    uploaded = st.file_uploader("Upload Voucher CSV", type=["csv"])

    if uploaded is None:
        st.info("Upload a CSV file to begin.")
        st.stop()
    
    df = pd.read_csv(uploaded)
    df.columns = df.columns.str.replace('\n', ' ', regex=False)
    df.columns = df.columns.str.strip()

    st.subheader("Raw Preview")
    st.dataframe(df.head(20), use_container_width=True)

    # Normalize
    try:
        df_norm = normalize(df)
    except Exception as e:
        st.error(f"Normalization failed: {e}")
        st.stop()

    st.subheader("Normalized Preview")
    st.dataframe(df_norm.head(20), use_container_width=True)
    st.caption(f"Rows to insert: {len(df_norm):,}")

    clear_first = st.checkbox("Clear table first (TRUNCATE)", value=True)

    if st.button("🚀 Import to Database", type="primary"):

        conn = get_connection()

        try:
            with conn:
                with conn.cursor() as cur:

                    ensure_table(cur)

                    if clear_first:
                        cur.execute(f"TRUNCATE TABLE {full_table()};")

                    rows = [
                        tuple(None if pd.isna(x) else x for x in r)
                        for r in df_norm.to_numpy()
                    ]

                    execute_values(
                        cur,
                        f"""
                        INSERT INTO {full_table()}
                        (username, nomor_telpon, lokasi_outlet,
                        code_voucher_oden, code_voucher_tea_series,
                        code_voucher_matcha_series)
                        VALUES %s
                        """,
                        rows,
                        page_size=5000
                    )

            st.success(f"Successfully inserted {len(df_norm):,} rows.")

        except Exception as e:
            st.error(f"Import failed: {e}")

        finally:
            conn.close()