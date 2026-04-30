import re
import pandas as pd
import streamlit as st
from psycopg2.extras import execute_values
from db import get_connection

# ======================================================
# HELPERS
# ======================================================

def clean_phone(v):
    if pd.isna(v) if isinstance(v, float) else not v:
        return None
    s = re.sub(r"\D", "", str(v).strip())
    if s.startswith("62"):
        s = "0" + s[2:]
    elif s.startswith("8"):
        s = "0" + s
    return s or None


def clean_text(v):
    if pd.isna(v) if isinstance(v, float) else v is None:
        return None
    s = str(v).strip()
    return None if s in ("", ".") else s


def pick_col(df, name):
    kw = name.strip().lower()
    for col in df.columns:
        if kw in re.sub(r"\s+", " ", col.lower()).strip():
            return col
    raise ValueError(f"Kolom yang mengandung '{name}' tidak ditemukan.")


def to_rows(df):
    return [
        tuple(None if (isinstance(x, float) and pd.isna(x)) else x for x in r)
        for r in df.to_numpy()
    ]


# ======================================================
# TABS
# ======================================================

def _beanspot_tab():
    uploaded = st.file_uploader("Upload CSV Beanspot", type=["csv"])
    if uploaded is None:
        st.info("Upload file CSV untuk memulai.")
        return

    df = pd.read_csv(uploaded)
    df.columns = df.columns.str.replace("\n", " ", regex=False).str.strip()

    st.subheader("Raw Preview")
    st.dataframe(df.head(20), use_container_width=True)

    try:
        norm = pd.DataFrame({
            "username":                  df[pick_col(df, "username")].apply(clean_text),
            "nomor_telpon":              df[pick_col(df, "nomor telpon")].apply(clean_phone),
            "lokasi_outlet":             df[pick_col(df, "lokasi outlet")].apply(clean_text),
            "code_voucher_oden":         df[pick_col(df, "code voucher oden")].apply(clean_text),
            "code_voucher_tea_series":   df[pick_col(df, "code voucher tea")].apply(clean_text),
            "code_voucher_matcha_series":df[pick_col(df, "code voucher matcha")].apply(clean_text),
        })
        norm["username"] = norm["username"].str.strip().str.lower()
    except Exception as e:
        st.error(f"Normalisasi gagal: {e}")
        return

    st.subheader("Normalized Preview")
    st.dataframe(norm.head(20), use_container_width=True)
    st.caption(f"Total rows: {len(norm):,}")

    clear_first = st.checkbox("TRUNCATE tabel sebelum import", value=True, key="bs_clear")

    if st.button("🚀 Import ke Database", type="primary", key="bs_import"):
        conn = get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("CREATE SCHEMA IF NOT EXISTS voucher_campaign;")
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS voucher_campaign.campaign_registrations (
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
                    if clear_first:
                        cur.execute("TRUNCATE TABLE voucher_campaign.campaign_registrations;")
                    execute_values(
                        cur,
                        """
                        INSERT INTO voucher_campaign.campaign_registrations
                        (username, nomor_telpon, lokasi_outlet,
                         code_voucher_oden, code_voucher_tea_series, code_voucher_matcha_series)
                        VALUES %s
                        """,
                        to_rows(norm),
                        page_size=5000,
                    )
            st.success(f"Berhasil import {len(norm):,} rows.")
        except Exception as e:
            st.error(f"Import gagal: {e}")
        finally:
            conn.close()


def _lawson_tab():
    st.markdown("""
    **Kolom CSV yang dibutuhkan:**
    Link Akun TikTok · Nama Akun TikTok · Lokasi Outlet · Tanggal Visit · No. Telephone · Kode Voucher Kyoto Oden
    """)

    uploaded = st.file_uploader("Upload CSV LAWSON Kyoto Oden", type=["csv"])
    if uploaded is None:
        st.info("Upload file CSV untuk memulai.")
        return

    df = pd.read_csv(uploaded)
    df.columns = df.columns.str.replace("\n", " ", regex=False).str.strip()

    st.subheader("Raw Preview")
    st.dataframe(df.head(20), use_container_width=True)

    try:
        norm = pd.DataFrame({
            "link_akun_tiktok": df[pick_col(df, "link akun")].apply(clean_text),
            "username":         df[pick_col(df, "nama akun")].apply(clean_text),
            "lokasi_outlet":    df[pick_col(df, "lokasi outlet")].apply(clean_text),
            "tanggal_visit":    df[pick_col(df, "tanggal visit")].apply(clean_text),
            "nomor_telpon":     df[pick_col(df, "no. telephone")].apply(clean_phone),
            "kode_voucher_oden":df[pick_col(df, "kode voucher")].apply(clean_text),
        })
        norm["username"] = norm["username"].str.strip().str.lower()
    except Exception as e:
        st.error(f"Normalisasi gagal: {e}")
        return

    st.subheader("Normalized Preview")
    st.dataframe(norm.head(20), use_container_width=True)
    st.caption(f"Total rows: {len(norm):,}")

    clear_first = st.checkbox("TRUNCATE tabel sebelum import", value=True, key="ls_clear")

    if st.button("🚀 Import ke Database", type="primary", key="ls_import"):
        conn = get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("CREATE SCHEMA IF NOT EXISTS voucher_campaign;")
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS voucher_campaign.lawson_kyoto_oden (
                            link_akun_tiktok TEXT,
                            username TEXT,
                            lokasi_outlet TEXT,
                            tanggal_visit TEXT,
                            nomor_telpon TEXT,
                            kode_voucher_oden TEXT,
                            imported_at TIMESTAMPTZ DEFAULT NOW()
                        );
                    """)
                    if clear_first:
                        cur.execute("TRUNCATE TABLE voucher_campaign.lawson_kyoto_oden;")
                    execute_values(
                        cur,
                        """
                        INSERT INTO voucher_campaign.lawson_kyoto_oden
                        (link_akun_tiktok, username, lokasi_outlet, tanggal_visit,
                         nomor_telpon, kode_voucher_oden)
                        VALUES %s
                        """,
                        to_rows(norm),
                        page_size=5000,
                    )
            st.success(f"Berhasil import {len(norm):,} rows.")
        except Exception as e:
            st.error(f"Import gagal: {e}")
        finally:
            conn.close()


# ======================================================
# ENTRY POINT
# ======================================================

def render():
    st.title("🎟️ Voucher Campaign Import")

    tab_bs, tab_ls = st.tabs(["Beanspot", "LAWSON Kyoto Oden"])

    with tab_bs:
        _beanspot_tab()

    with tab_ls:
        _lawson_tab()
