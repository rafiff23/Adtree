import re
import pandas as pd
import streamlit as st
from psycopg2.extras import execute_values

from db import get_connection  # ✅ reuse existing connection

SCHEMA_NAME = "leaderboard"  # (spelled exactly as you requested)
TABLE_NAME = "creator_dec_leaderboard_all_level"
FULL_TABLE = f"{SCHEMA_NAME}.{TABLE_NAME}"

REQUIRED_COLS = [
    "No",
    "Creator Name",
    "Username",
    "Post",
    "Redemption GMV",
    "Status",
    "Hadiah",
    "Level",
]

def _clean_int(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return None
    s = re.sub(r"[,\s]", "", s)
    s = re.sub(r"[^0-9\-]", "", s)
    if s in ("", "-"):
        return None
    try:
        return int(s)
    except:
        return None

def _clean_idr(v):
    """'Rp334,022,643' -> 334022643 (BIGINT)"""
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return None
    s = s.replace("Rp", "").replace("rp", "")
    s = re.sub(r"[.\s]", "", s)  # remove dots/spaces
    s = s.replace(",", "")
    s = re.sub(r"[^0-9\-]", "", s)
    if s in ("", "-"):
        return None
    try:
        return int(s)
    except:
        return None

def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    # Case-insensitive column matching
    col_map = {c.strip().lower(): c for c in df.columns}

    def pick(col_name: str) -> str:
        key = col_name.strip().lower()
        if key not in col_map:
            raise ValueError(f"Missing required column: '{col_name}'")
        return col_map[key]

    out = pd.DataFrame()
    out["rank_no"] = df[pick("No")].apply(_clean_int)
    out["creator_name"] = df[pick("Creator Name")].astype(str).str.strip()
    out["username"] = df[pick("Username")].astype(str).str.strip()
    out["post_count"] = df[pick("Post")].apply(_clean_int)
    out["redemption_gmv_idr"] = df[pick("Redemption GMV")].apply(_clean_idr)
    out["status"] = df[pick("Status")].astype(str).str.strip()
    out["hadiah_idr"] = df[pick("Hadiah")].apply(_clean_idr)
    out["level"] = df[pick("Level")].apply(_clean_int)

    # cleanup small annoying artifacts
    out["creator_name"] = out["creator_name"].replace({".": None, "": None})
    out["username"] = out["username"].replace({".": None, "": None})

    return out

def _ensure_schema_and_table(cur):
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
            rank_no              INTEGER,
            creator_name         TEXT,
            username             TEXT,
            post_count           INTEGER,
            redemption_gmv_idr   BIGINT,
            status               TEXT,
            hadiah_idr           BIGINT,
            level                INTEGER,
            imported_at          TIMESTAMPTZ DEFAULT NOW()
        );
    """)

def _truncate(cur):
    cur.execute(f"TRUNCATE TABLE {FULL_TABLE};")

def _insert(cur, df_norm: pd.DataFrame):
    cols = [
        "rank_no",
        "creator_name",
        "username",
        "post_count",
        "redemption_gmv_idr",
        "status",
        "hadiah_idr",
        "level",
    ]
    rows = [tuple(None if pd.isna(x) else x for x in r) for r in df_norm[cols].to_numpy()]
    sql = f"INSERT INTO {FULL_TABLE} ({', '.join(cols)}) VALUES %s"
    execute_values(cur, sql, rows, page_size=5000)

def render():
    st.header("Leaderboard Import (CSV → Postgres)")
    st.caption(f"Target table: `{FULL_TABLE}` (auto-create schema + table, then truncate + insert)")

    uploaded = st.file_uploader("Upload CSV", type=["csv"])

    if uploaded is None:
        st.info("Upload the leaderboard CSV to start.")
        return

    df = pd.read_csv(uploaded)

    # quick check
    missing = [c for c in REQUIRED_COLS if c.lower() not in [x.lower() for x in df.columns]]
    if missing:
        st.error(f"CSV missing columns: {missing}")
        st.write("Found columns:", list(df.columns))
        return

    st.subheader("Raw Preview")
    st.dataframe(df.head(20), use_container_width=True)

    df_norm = _normalize(df)

    st.subheader("Normalized Preview (will be inserted)")
    st.dataframe(df_norm.head(20), use_container_width=True)

    clear_first = st.checkbox("Clear table first (TRUNCATE)", value=True)

    if st.button("🚀 Import to DB", type="primary"):
        conn = get_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    _ensure_schema_and_table(cur)
                    if clear_first:
                        _truncate(cur)
                    _insert(cur, df_norm)
            st.success(f"Done. Inserted {len(df_norm):,} rows into {FULL_TABLE}.")
        except Exception as e:
            st.error(f"Import failed: {e}")
        finally:
            conn.close()
