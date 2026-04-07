import datetime
import streamlit as st
import pandas as pd

try:
    from streamlit_autorefresh import st_autorefresh
    _HAS_AUTOREFRESH = True
except ImportError:
    _HAS_AUTOREFRESH = False

from db import (
    prepare_content_qc_csv,
    upsert_content_qc_posts,
    fetch_content_qc_posts,
    save_content_qc_review,
)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_QC_BOOL_COLS = ["qc_hook", "qc_usp", "qc_product", "qc_cta", "qc_engaging"]
_QC_EDITABLE  = ["qc_type"] + _QC_BOOL_COLS + ["qc_quality", "qc_issue"]

_ISSUE_OPTIONS = [
    "Low Video Quality (Not HD)",
    "Unreadable Text / UI",
    "Missing Mandatory Elements",
    "Product Not Included",
    "No USP / No Review",
    "No UI",
    "Incorrect / Duplicate Link",
    "Non-Original Footage (Google / AI)",
    "Repetitive / Redundant Content",
    "Footage Not Aligned with Merchant",
    "Content Not Engaging",
    "Duplicate Content Submission",
    "Video Not Accessible",
]

_FINAL_STATUS_OPTS = ["All", "Unreviewed", "Very Good", "Good", "Fair", "Poor"]


def _norm_val(v):
    """Normalise a cell value for change-detection comparison."""
    if v is None or v == "" or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, list):
        return tuple(sorted(v)) if v else None
    return v


# ─────────────────────────────────────────────────────────────────────────────
# Username gate
# ─────────────────────────────────────────────────────────────────────────────

def _username_gate():
    if st.session_state.get("cqc_username"):
        return st.session_state["cqc_username"]

    st.title("📹 Content QC")
    st.info("Enter your name to get started.")
    name = st.text_input("Name:", key="cqc_name_input")
    if st.button("Start", type="primary") and name.strip():
        st.session_state["cqc_username"] = name.strip()
        st.rerun()
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Import tab
# ─────────────────────────────────────────────────────────────────────────────

def _render_import_tab():
    st.subheader("Import Data")

    uploaded = st.file_uploader("Upload Excel file", type=["xlsx"])
    if not uploaded:
        return

    try:
        df_raw = pd.read_excel(uploaded, sheet_name="Data", dtype=str)
    except Exception as e:
        st.error(f"Failed to read file: {e}")
        return

    st.caption(f"File loaded: **{len(df_raw):,} rows** · **{len(df_raw.columns)} columns**")

    df, unmapped = prepare_content_qc_csv(df_raw)

    if unmapped:
        st.warning(f"Unrecognised columns (ignored): {unmapped}")

    if "post_id" not in df.columns:
        st.error("Column **Post ID** not found. Make sure the file contains that column.")
        return

    before = len(df)
    df = df[df["post_id"].notna() & (df["post_id"].str.strip() != "")].copy()
    df["post_id"] = df["post_id"].str.strip()
    if len(df) < before:
        st.warning(f"{before - len(df)} rows dropped due to empty Post ID.")

    if df.empty:
        st.error("No valid data to import.")
        return

    st.write(f"Preview (first {min(10, len(df))} rows):")
    st.dataframe(df.head(10), use_container_width=True)

    if st.button("🚀 Import to Database", type="primary"):
        with st.spinner("Importing…"):
            rows = [
                {k: (None if isinstance(v, float) and pd.isna(v) else v)
                 for k, v in row.items()}
                for row in df.to_dict("records")
            ]
            try:
                inserted, updated = upsert_content_qc_posts(rows)
                st.success(
                    f"✅ Import complete! "
                    f"**{inserted}** new rows · **{updated}** rows updated."
                )
            except Exception as e:
                st.error(f"Import failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# QC Review tab
# ─────────────────────────────────────────────────────────────────────────────

def _load_qc_data(qc_filter, date_from, date_to, search):
    rows = fetch_content_qc_posts(
        qc_filter=qc_filter if qc_filter != "All" else None,
        date_from=date_from or None,
        date_to=date_to or None,
        search=search.strip() or None,
    )
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()

    if not df.empty:
        df["post_url"] = (
            "https://www.tiktok.com/@"
            + df["creator_id"].fillna("")
            + "/video/"
            + df["post_id"].fillna("")
        )
        for col in _QC_BOOL_COLS:
            if col in df.columns:
                df[col] = df[col].fillna(False).astype(bool)
        if "qc_issue" in df.columns:
            df["qc_issue"] = df["qc_issue"].apply(
                lambda v: [i.strip() for i in v.split(",") if i.strip()] if v else []
            )

    st.session_state["cqc_df"] = df
    if not df.empty:
        st.session_state["cqc_original_qc"] = (
            df.set_index("post_id")[["qc_updated_at"]].to_dict("index")
        )
    else:
        st.session_state["cqc_original_qc"] = {}


def _render_qc_tab(username: str):
    # ── Filters ───────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        qc_filter = st.selectbox("Final Status", _FINAL_STATUS_OPTS, key="cqc_qc_filter")
    with c2:
        date_from = st.date_input("From Date", value=datetime.date.today(), key="cqc_date_from")
    with c3:
        date_to = st.date_input("To Date", value=datetime.date.today(), key="cqc_date_to")
    with c4:
        search = st.text_input("Search (Post ID / Creator)", key="cqc_search")

    filter_key = (qc_filter, str(date_from), str(date_to), search.strip())
    if (
        "cqc_df" not in st.session_state
        or st.session_state.get("cqc_filter_key") != filter_key
        or st.session_state.pop("cqc_force_reload", False)
    ):
        try:
            _load_qc_data(qc_filter, date_from, date_to, search)
            st.session_state["cqc_filter_key"] = filter_key
        except Exception as e:
            st.error(f"Failed to load data: {e}")
            return

    df = st.session_state["cqc_df"]

    if df.empty:
        st.info("No posts match the current filter.")
        return

    total = len(df)

    # ── Row range ─────────────────────────────────────────────────────────────
    r1, r2 = st.columns(2)
    with r1:
        row_from = st.number_input(
            "From row", min_value=1, max_value=total, value=1, step=1, key="cqc_row_from"
        )
    with r2:
        row_to = st.number_input(
            "To row", min_value=1, max_value=total, value=min(1000, total), step=1, key="cqc_row_to"
        )

    row_from = int(row_from)
    row_to   = int(max(row_from, row_to))
    df = df.iloc[row_from - 1 : row_to]

    st.caption(
        f"Showing rows **{row_from}–{row_from + len(df) - 1}** of **{total:,}** total"
        " — edit QC cells directly, then save."
    )

    # ── Editable table ────────────────────────────────────────────────────────
    display_cols = [
        "post_url", "post_date", "creator_name",
        "qc_type",
        "qc_hook", "qc_usp", "qc_product", "qc_cta", "qc_engaging",
        "qc_quality", "qc_total_score",
        "qc_issue",
        "qc_final_status", "qc_updated_by",
    ]
    display_cols = [c for c in display_cols if c in df.columns]

    edited_df = st.data_editor(
        df[display_cols],
        column_config={
            "post_url":       st.column_config.LinkColumn("Post URL", display_text="Open ↗"),
            "post_date":      st.column_config.DateColumn("Post Date", disabled=True),
            "creator_name":   st.column_config.TextColumn("Creator", disabled=True),
            "qc_type":        st.column_config.SelectboxColumn(
                                  "Type", options=["Video", "Image"], required=False
                              ),
            "qc_hook":        st.column_config.CheckboxColumn("Hook (×3)"),
            "qc_usp":         st.column_config.CheckboxColumn("USP (×2)"),
            "qc_product":     st.column_config.CheckboxColumn("Product (×2)"),
            "qc_cta":         st.column_config.CheckboxColumn("CTA (×2)"),
            "qc_engaging":    st.column_config.CheckboxColumn("Engaging (×2)"),
            "qc_quality":     st.column_config.SelectboxColumn(
                                  "Quality (1-4)", options=[1, 2, 3, 4], required=False
                              ),
            "qc_total_score": st.column_config.NumberColumn(
                                  "Total Score", disabled=True, format="%.1f"
                              ),
            "qc_issue":       st.column_config.MultiselectColumn(
                                  "Issue", options=_ISSUE_OPTIONS, width="large"
                              ),
            "qc_final_status": st.column_config.TextColumn("Final Status", disabled=True),
            "qc_updated_by":  st.column_config.TextColumn("Updated By", disabled=True),
        },
        use_container_width=True,
        height=520,
        hide_index=True,
        key="cqc_editor",
    )

    # ── Detect changes ────────────────────────────────────────────────────────
    changed_idx = []
    for i in range(len(df)):
        for col in _QC_EDITABLE:
            if col not in df.columns or col not in edited_df.columns:
                continue
            if _norm_val(df.iloc[i][col]) != _norm_val(edited_df.iloc[i][col]):
                changed_idx.append(i)
                break

    if not changed_idx:
        return

    n = len(changed_idx)
    st.caption(f"**{n}** unsaved change(s)")

    if st.button(f"💾 Save {n} change(s)", type="primary", key="cqc_bulk_save"):
        saved, conflicts, errors = 0, [], []

        for i in changed_idx:
            post_id  = df.iloc[i]["post_id"]
            row      = edited_df.iloc[i]
            qc_data  = {col: _norm_val(row[col]) for col in _QC_EDITABLE if col in row.index}
            original = st.session_state["cqc_original_qc"].get(post_id, {})
            expected_at = original.get("qc_updated_at")

            ok, msg = save_content_qc_review(post_id, qc_data, username, expected_at)
            if ok:
                saved += 1
            elif "Conflict" in msg:
                conflicts.append(post_id)
            else:
                errors.append(f"{post_id}: {msg}")

        if saved:
            st.success(f"✅ {saved} row(s) saved.")
        if conflicts:
            st.warning(
                f"⚠️ {len(conflicts)} row(s) conflict — already updated by someone else. "
                f"Refresh and try again: {conflicts}"
            )
        if errors:
            st.error(f"Errors: {errors}")

        st.session_state["cqc_force_reload"] = True
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def render():
    username = _username_gate()
    if not username:
        return

    st.title("📹 Content QC")

    col_title, col_user = st.columns([6, 1])
    with col_user:
        if st.button("🔄 Switch User", key="cqc_reset_name"):
            for key in ["cqc_username", "cqc_df", "cqc_original_qc", "cqc_filter_key"]:
                st.session_state.pop(key, None)
            st.rerun()
    with col_title:
        st.caption(f"Logged in as: **{username}**")

    if _HAS_AUTOREFRESH:
        st_autorefresh(interval=30_000, key="cqc_autorefresh")
    else:
        if st.button("🔄 Refresh", key="cqc_manual_refresh"):
            st.rerun()

    tab_import, tab_qc = st.tabs(["📥 Import", "✅ QC Review"])

    with tab_import:
        _render_import_tab()

    with tab_qc:
        _render_qc_tab(username)
