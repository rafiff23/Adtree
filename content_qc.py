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
    get_content_qc_post_state,
    acquire_content_qc_lock,
    release_content_qc_lock,
    save_content_qc_status,
)

_QC_OPTIONS = ["", "Good", "Bad"]


# ─────────────────────────────────────────────────────────────────────────────
# Username gate
# ─────────────────────────────────────────────────────────────────────────────

def _username_gate() -> str | None:
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
        df_raw = pd.read_excel(uploaded, dtype=str)
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

def _render_qc_tab(username: str):
    with st.expander("🔍 Filter", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            qc_filter = st.selectbox(
                "QC Status", ["All", "Unreviewed", "Good", "Bad"], key="cqc_qc_filter"
            )
        with c2:
            date_from = st.date_input("From Date", value=None, key="cqc_date_from")
        with c3:
            date_to = st.date_input("To Date", value=None, key="cqc_date_to")
        with c4:
            search = st.text_input("Search (Post ID / Title / Creator)", key="cqc_search")

    try:
        rows = fetch_content_qc_posts(
            qc_filter=qc_filter if qc_filter != "All" else None,
            date_from=date_from or None,
            date_to=date_to or None,
            search=search.strip() or None,
        )
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return

    if not rows:
        st.info("No posts match the current filter.")
        return

    df = pd.DataFrame([dict(r) for r in rows])
    st.caption(f"Showing **{len(df):,}** posts")

    display = df[
        ["post_id", "post_title", "post_date", "creator_name",
         "creator_level", "video_views", "ctr", "cvr",
         "qc_status", "qc_updated_by", "locked_by"]
    ].copy()

    display["locked_by"] = display["locked_by"].apply(
        lambda v: f"🔒 {v}" if pd.notna(v) and v else ""
    )
    display["qc_status"] = display["qc_status"].fillna("—")

    st.dataframe(
        display.rename(columns={
            "post_id":       "Post ID",
            "post_title":    "Title",
            "post_date":     "Date",
            "creator_name":  "Creator",
            "creator_level": "Level",
            "video_views":   "Views",
            "ctr":           "CTR",
            "cvr":           "CVR",
            "qc_status":     "QC Status",
            "qc_updated_by": "Updated By",
            "locked_by":     "Lock",
        }),
        use_container_width=True,
        height=380,
    )

    st.divider()
    _render_edit_panel(username, df)


# ─────────────────────────────────────────────────────────────────────────────
# Edit panel
# ─────────────────────────────────────────────────────────────────────────────

def _render_edit_panel(username: str, df: pd.DataFrame):
    st.subheader("Edit QC Status")

    post_ids = df["post_id"].tolist()

    editing_id = st.session_state.get("cqc_editing_id")
    default_idx = post_ids.index(editing_id) if editing_id in post_ids else 0

    def _label(pid):
        title = df.loc[df["post_id"] == pid, "post_title"].values
        return f"{pid}  —  {title[0] if len(title) else ''}"

    selected_id = st.selectbox(
        "Select Post:",
        options=post_ids,
        index=default_idx,
        format_func=_label,
        key="cqc_post_select",
    )

    selected_row = df[df["post_id"] == selected_id].iloc[0]
    locked_by_other = (
        pd.notna(selected_row.get("locked_by"))
        and selected_row.get("locked_by") != username
    )

    if locked_by_other:
        st.warning(
            f"🔒 This post is currently being edited by **{selected_row['locked_by']}**. "
            "Please wait or choose another post."
        )
        if st.session_state.get("cqc_editing_id") == selected_id:
            st.session_state.pop("cqc_editing_id", None)
        return

    is_editing = st.session_state.get("cqc_editing_id") == selected_id

    if not is_editing:
        current_qc = selected_row.get("qc_status")
        display_qc = current_qc if pd.notna(current_qc) and current_qc else "Not reviewed"
        st.metric("Current QC Status", display_qc)

        if st.button("✏️ Edit QC Status", type="primary", key="cqc_start_edit"):
            ok, msg = acquire_content_qc_lock(selected_id, username)
            if ok:
                state = get_content_qc_post_state(selected_id)
                st.session_state["cqc_editing_id"] = selected_id
                st.session_state["cqc_edit_expected_at"] = (
                    state["qc_updated_at"] if state else None
                )
                cur_val = state["qc_status"] if state and state["qc_status"] else ""
                st.session_state["cqc_status_select"] = cur_val
                st.rerun()
            else:
                st.error(f"🔒 {msg}")
        return

    # Refresh lock on every rerun while editing
    ok, msg = acquire_content_qc_lock(selected_id, username)
    if not ok:
        st.error(f"Lock lost: {msg}")
        st.session_state.pop("cqc_editing_id", None)
        st.rerun()
        return

    st.info(f"Editing: **{selected_id}**")

    new_status = st.selectbox(
        "QC Status",
        options=_QC_OPTIONS,
        index=_QC_OPTIONS.index(st.session_state.get("cqc_status_select", "")),
        format_func=lambda x: "— Not reviewed —" if x == "" else x,
        key="cqc_status_select",
    )

    col_save, col_cancel = st.columns(2)

    with col_save:
        if st.button("💾 Save", type="primary", key="cqc_save"):
            ok, msg = save_content_qc_status(
                selected_id,
                new_status,
                username,
                st.session_state.get("cqc_edit_expected_at"),
            )
            release_content_qc_lock(selected_id, username)
            st.session_state.pop("cqc_editing_id", None)
            if ok:
                st.success(f"✅ QC Status saved: **{new_status or '(cleared)'}**")
            else:
                st.error(msg)
            st.rerun()

    with col_cancel:
        if st.button("❌ Cancel", key="cqc_cancel"):
            release_content_qc_lock(selected_id, username)
            st.session_state.pop("cqc_editing_id", None)
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
            editing = st.session_state.get("cqc_editing_id")
            if editing:
                release_content_qc_lock(editing, username)
            for key in ["cqc_username", "cqc_editing_id", "cqc_edit_expected_at"]:
                st.session_state.pop(key, None)
            st.rerun()
    with col_title:
        st.caption(f"Logged in as: **{username}**")

    # Auto-refresh every 30 s – paused while editing to avoid disrupting the form
    if _HAS_AUTOREFRESH and not st.session_state.get("cqc_editing_id"):
        st_autorefresh(interval=30_000, key="cqc_autorefresh")
    elif not _HAS_AUTOREFRESH:
        if st.button("🔄 Refresh", key="cqc_manual_refresh"):
            st.rerun()

    tab_import, tab_qc = st.tabs(["📥 Import", "✅ QC Review"])

    with tab_import:
        _render_import_tab()

    with tab_qc:
        _render_qc_tab(username)
