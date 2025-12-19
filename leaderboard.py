import re
import pandas as pd
import streamlit as st
from psycopg2.extras import execute_values
from db import get_connection

SCHEMA_NAME = "leaderboard"

# =========================================
# 1) IMPORT MODES (3 tables)
# =========================================
IMPORT_MODES = {
    "Main Leaderboard": {
        "table": "creator_dec_leaderboard_all_level",
        "required_cols": ["No", "Creator Name", "Username", "Post", "Redemption GMV", "Status", "Hadiah", "Level"],
    },
    "All Industry Bonus": {
        "table": "creator_dec_leaderboard_all_industry_bonus",
        "required_cols": [
            "Username", "GMV", "Order Accommodation", "Order Dining", "Order things to Do",
            "Syarat penjualan", "Kurang penjualan", "Status", "Bonus"
        ],
    },
    "Dining Bonus": {
        "table": "creator_dec_leaderboard_dining_bonus",
        "required_cols": ["Creator Name", "Penjualan Dining", "Syarat penjualan", "Kurang penjualan", "Status", "Bonus"],
    },
}

def full_table(table_name: str) -> str:
    return f"{SCHEMA_NAME}.{table_name}"

# =========================================
# 2) CLEANERS
# =========================================
def _clean_int(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return None
    s = re.sub(r"[,\s]", "", s)           # remove commas/spaces
    s = re.sub(r"[^0-9\-]", "", s)        # keep digits and minus
    if s in ("", "-"):
        return None
    try:
        return int(s)
    except:
        return None

def _clean_idr(v):
    """
    Works for:
    - 'Rp334,022,643' -> 334022643
    - '56,000,000' -> 56000000
    - '-Rp21,455,789' -> -21455789
    - 'Rp0' -> 0
    """
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return None

    s = s.replace("Rp", "").replace("rp", "")
    s = s.replace(".", "")
    s = s.replace(" ", "")
    s = s.replace(",", "")
    s = re.sub(r"[^0-9\-]", "", s)

    if s in ("", "-"):
        return None
    try:
        return int(s)
    except:
        return None

# =========================================
# 3) NORMALIZER (case-insensitive picker)
# =========================================
def _pick_col(df: pd.DataFrame, col_name: str) -> str:
    col_map = {c.strip().lower(): c for c in df.columns}
    key = col_name.strip().lower()
    if key not in col_map:
        raise ValueError(f"Missing required column: '{col_name}'")
    return col_map[key]

def _normalize_main_leaderboard(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["rank_no"] = df[_pick_col(df, "No")].apply(_clean_int)
    out["creator_name"] = df[_pick_col(df, "Creator Name")].astype(str).str.strip()
    out["username"] = df[_pick_col(df, "Username")].astype(str).str.strip()
    out["post_count"] = df[_pick_col(df, "Post")].apply(_clean_int)
    out["redemption_gmv_idr"] = df[_pick_col(df, "Redemption GMV")].apply(_clean_idr)
    out["status"] = df[_pick_col(df, "Status")].astype(str).str.strip()
    out["hadiah_idr"] = df[_pick_col(df, "Hadiah")].apply(_clean_idr)
    out["level"] = df[_pick_col(df, "Level")].apply(_clean_int)

    out["creator_name"] = out["creator_name"].replace({".": None, "": None})
    out["username"] = out["username"].replace({".": None, "": None})
    return out

def _normalize_all_industry_bonus(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["username"] = df[_pick_col(df, "Username")].astype(str).str.strip()
    out["gmv_idr"] = df[_pick_col(df, "GMV")].apply(_clean_idr)
    out["order_accommodation"] = df[_pick_col(df, "Order Accommodation")].apply(_clean_int)
    out["order_dining"] = df[_pick_col(df, "Order Dining")].apply(_clean_int)
    out["order_things_to_do"] = df[_pick_col(df, "Order things to Do")].apply(_clean_int)
    out["syarat_penjualan_idr"] = df[_pick_col(df, "Syarat penjualan")].apply(_clean_idr)
    out["kurang_penjualan_idr"] = df[_pick_col(df, "Kurang penjualan")].apply(_clean_idr)
    out["status"] = df[_pick_col(df, "Status")].astype(str).str.strip()
    out["bonus_idr"] = df[_pick_col(df, "Bonus")].apply(_clean_idr)

    out["username"] = out["username"].replace({".": None, "": None})
    return out

def _normalize_dining_bonus(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["creator_name"] = df[_pick_col(df, "Creator Name")].astype(str).str.strip()
    out["penjualan_dining_idr"] = df[_pick_col(df, "Penjualan Dining")].apply(_clean_idr)
    out["syarat_penjualan_idr"] = df[_pick_col(df, "Syarat penjualan")].apply(_clean_idr)
    out["kurang_penjualan_idr"] = df[_pick_col(df, "Kurang penjualan")].apply(_clean_idr)
    out["status"] = df[_pick_col(df, "Status")].astype(str).str.strip()
    out["bonus_idr"] = df[_pick_col(df, "Bonus")].apply(_clean_idr)

    out["creator_name"] = out["creator_name"].replace({".": None, "": None})
    return out

# =========================================
# 4) DDL + INSERT HELPERS
# =========================================
def _ensure_schema(cur):
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")

def _ensure_table(cur, mode_key: str):
    tbl = full_table(IMPORT_MODES[mode_key]["table"])

    if mode_key == "Main Leaderboard":
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tbl} (
                rank_no              INTEGER,
                creator_name         TEXT,
                username             TEXT,
                post_count           INTEGER,
                redemption_gmv_idr   BIGINT,
                status               TEXT,
                hadiah_idr           BIGINT,
                level                INTEGER,
                imported_at          TIMESTAMPTZ DEFAULT NOW(),
                last_updated         TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS last_updated TIMESTAMPTZ DEFAULT NOW();")

    elif mode_key == "All Industry Bonus":
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tbl} (
                username                TEXT,
                gmv_idr                 BIGINT,
                order_accommodation     INTEGER,
                order_dining            INTEGER,
                order_things_to_do      INTEGER,
                syarat_penjualan_idr    BIGINT,
                kurang_penjualan_idr    BIGINT,
                status                  TEXT,
                bonus_idr               BIGINT,
                imported_at             TIMESTAMPTZ DEFAULT NOW(),
                last_updated            TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS last_updated TIMESTAMPTZ DEFAULT NOW();")

    elif mode_key == "Dining Bonus":
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tbl} (
                creator_name            TEXT,
                penjualan_dining_idr    BIGINT,
                syarat_penjualan_idr    BIGINT,
                kurang_penjualan_idr    BIGINT,
                status                  TEXT,
                bonus_idr               BIGINT,
                imported_at             TIMESTAMPTZ DEFAULT NOW(),
                last_updated            TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS last_updated TIMESTAMPTZ DEFAULT NOW();")

def _truncate(cur, mode_key: str):
    tbl = full_table(IMPORT_MODES[mode_key]["table"])
    cur.execute(f"TRUNCATE TABLE {tbl};")

def _insert(cur, mode_key: str, df_norm: pd.DataFrame):
    tbl = full_table(IMPORT_MODES[mode_key]["table"])

    if mode_key == "Main Leaderboard":
        cols = ["rank_no","creator_name","username","post_count","redemption_gmv_idr","status","hadiah_idr","level"]

    elif mode_key == "All Industry Bonus":
        cols = [
            "username","gmv_idr","order_accommodation","order_dining","order_things_to_do",
            "syarat_penjualan_idr","kurang_penjualan_idr","status","bonus_idr"
        ]

    elif mode_key == "Dining Bonus":
        cols = ["creator_name","penjualan_dining_idr","syarat_penjualan_idr","kurang_penjualan_idr","status","bonus_idr"]

    rows = [tuple(None if pd.isna(x) else x for x in r) for r in df_norm[cols].to_numpy()]
    sql = f"INSERT INTO {tbl} ({', '.join(cols)}) VALUES %s"
    execute_values(cur, sql, rows, page_size=5000)

# =========================================
# 5) UI
# =========================================
def render():
    st.title("Leaderboard Import")
    st.caption("Choose the CSV type first. All data will be stored under the `leaderboard` schema, but in different tables.")

    if "import_mode" not in st.session_state:
        st.session_state.import_mode = "Main Leaderboard"

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Main Leaderboard", use_container_width=True):
            st.session_state.import_mode = "Main Leaderboard"
    with c2:
        if st.button("All Industry Bonus", use_container_width=True):
            st.session_state.import_mode = "All Industry Bonus"
    with c3:
        if st.button("Dining Bonus", use_container_width=True):
            st.session_state.import_mode = "Dining Bonus"

    mode = st.session_state.import_mode
    tbl = full_table(IMPORT_MODES[mode]["table"])
    required = IMPORT_MODES[mode]["required_cols"]

    st.info(f"Selected mode: **{mode}**  →  Target table: `{tbl}`")

    uploaded = st.file_uploader(f"Upload CSV for: {mode}", type=["csv"])
    if uploaded is None:
        st.info("Upload a CSV file to start.")
        return

    df = pd.read_csv(uploaded)

    # Validate columns (case-insensitive)
    missing = [c for c in required if c.lower() not in [x.strip().lower() for x in df.columns]]
    if missing:
        st.error(f"CSV is missing required columns: {missing}")
        st.write("Detected columns:", list(df.columns))
        return

    st.subheader("Raw Preview")
    st.write("Columns:", list(df.columns))
    st.dataframe(df.head(20), use_container_width=True)

    # Normalize based on selected mode
    try:
        if mode == "Main Leaderboard":
            df_norm = _normalize_main_leaderboard(df)
        elif mode == "All Industry Bonus":
            df_norm = _normalize_all_industry_bonus(df)
        else:
            df_norm = _normalize_dining_bonus(df)
    except Exception as e:
        st.error(f"Normalization failed: {e}")
        return

    st.subheader("Normalized Preview (will be inserted)")
    st.dataframe(df_norm.head(20), use_container_width=True)
    st.caption(f"Rows to insert: {len(df_norm):,}")

    clear_first = st.checkbox("Clear table first (TRUNCATE)", value=True)

    if st.button("🚀 Import to DB", type="primary"):
        conn = get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    _ensure_schema(cur)
                    _ensure_table(cur, mode)
                    if clear_first:
                        _truncate(cur, mode)
                    _insert(cur, mode, df_norm)

            st.success(f"Done. Inserted {len(df_norm):,} rows into {tbl}.")
        except Exception as e:
            st.error(f"Import failed: {e}")
        finally:
            conn.close()
