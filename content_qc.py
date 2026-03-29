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
    save_content_qc_status,
)


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
# QC Review tab  –  bulk data_editor approach
# ─────────────────────────────────────────────────────────────────────────────

def _load_qc_data(qc_filter, date_from, date_to, search):
    """Fetch from DB and cache in session state. Call when filters change or after save."""
    rows = fetch_content_qc_posts(
        qc_filter=qc_filter if qc_filter != "All" else None,
        date_from=date_from or None,
        date_to=date_to or None,
        search=search.strip() or None,
    )
    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()

    # Build TikTok URL column
    if not df.empty:
        df["post_url"] = (
            "https://www.tiktok.com/@"
            + df["creator_id"].fillna("")
            + "/video/"
            + df["post_id"].fillna("")
        )

    st.session_state["cqc_df"] = df
    # Store original qc_status + qc_updated_at per post_id for conflict detection
    if not df.empty:
        st.session_state["cqc_original_qc"] = (
            df.set_index("post_id")[["qc_status", "qc_updated_at"]].to_dict("index")
        )
    else:
        st.session_state["cqc_original_qc"] = {}


def _render_qc_tab(username: str):
    # ── Filters ───────────────────────────────────────────────────────────────
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

    # Reload when filters change or explicitly requested
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

    st.caption(f"Showing **{len(df):,}** posts — edit QC Status cells directly, then save.")

    # ── Editable table ────────────────────────────────────────────────────────
    display_cols = [
        "post_url", "post_id", "post_title", "post_date",
        "creator_name", "creator_level",
        "video_views", "ctr", "cvr", "like_rate", "comment_rate",
        "qc_status", "qc_updated_by", "locked_by",
    ]
    display_cols = [c for c in display_cols if c in df.columns]

    edited_df = st.data_editor(
        df[display_cols],
        column_config={
            "post_url":      st.column_config.LinkColumn("Post URL", display_text="Open ↗"),
            "post_id":       st.column_config.TextColumn("Post ID", disabled=True),
            "post_title":    st.column_config.TextColumn("Title", disabled=True),
            "post_date":     st.column_config.DateColumn("Date", disabled=True),
            "creator_name":  st.column_config.TextColumn("Creator", disabled=True),
            "creator_level": st.column_config.NumberColumn("Level", disabled=True),
            "video_views":   st.column_config.NumberColumn("Views", disabled=True, format="%d"),
            "ctr":           st.column_config.NumberColumn("CTR", disabled=True, format="%.2f%%"),
            "cvr":           st.column_config.NumberColumn("CVR", disabled=True, format="%.2f%%"),
            "like_rate":     st.column_config.NumberColumn("Like Rate", disabled=True, format="%.2f%%"),
            "comment_rate":  st.column_config.NumberColumn("Comment Rate", disabled=True, format="%.2f%%"),
            "qc_status":     st.column_config.SelectboxColumn(
                                 "QC Status", options=["Good", "Bad"], required=False
                             ),
            "qc_updated_by": st.column_config.TextColumn("Updated By", disabled=True),
            "locked_by":     st.column_config.TextColumn("🔒 Lock", disabled=True),
        },
        use_container_width=True,
        height=520,
        hide_index=True,
        key="cqc_editor",
    )

    # ── Detect changes ────────────────────────────────────────────────────────
    orig_status = df["qc_status"].fillna("").values
    new_status  = edited_df["qc_status"].fillna("").values
    changed_idx = [i for i, (o, n) in enumerate(zip(orig_status, new_status)) if o != n]

    if not changed_idx:
        return

    n = len(changed_idx)
    st.caption(f"**{n}** unsaved change(s)")

    if st.button(f"💾 Save {n} change(s)", type="primary", key="cqc_bulk_save"):
        saved, conflicts, errors = 0, [], []

        for i in changed_idx:
            post_id   = df.iloc[i]["post_id"]
            new_val   = edited_df.iloc[i]["qc_status"]
            new_val   = None if pd.isna(new_val) or new_val == "" else new_val
            locked_by = df.iloc[i].get("locked_by")

            if pd.notna(locked_by) and locked_by and locked_by != username:
                conflicts.append(f"{post_id} (locked by {locked_by})")
                continue

            original  = st.session_state["cqc_original_qc"].get(post_id, {})
            expected_at = original.get("qc_updated_at")

            ok, msg = save_content_qc_status(post_id, new_val, username, expected_at)
            if ok:
                saved += 1
            elif "Conflict" in msg:
                conflicts.append(post_id)
            else:
                errors.append(f"{post_id}: {msg}")

        if saved:
            st.success(f"✅ {saved} row(s) saved.")
        if conflicts:
            st.warning(f"⚠️ {len(conflicts)} skipped (conflict or locked): {conflicts}")
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

    # Auto-refresh every 30 s
    if _HAS_AUTOREFRESH:
        st_autorefresh(interval=30_000, key="cqc_autorefresh")
    elif not _HAS_AUTOREFRESH:
        if st.button("🔄 Refresh", key="cqc_manual_refresh"):
            st.rerun()

    tab_import, tab_qc = st.tabs(["📥 Import", "✅ QC Review"])

    with tab_import:
        _render_import_tab()

    with tab_qc:
        _render_qc_tab(username)
