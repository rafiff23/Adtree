import pandas as pd
import streamlit as st

from db import (
    get_connection,
    bulk_update_content_submissions,
)


# =================================================
# HELPERS FOR CONTENT_SUBMISSIONS
# =================================================

def fetch_content_submissions():
    """
    Fetch joined view for content_submissions:
    - joins creator_registry, agency_map, category_map, status_map
    """
    conn = get_connection()
    sql = """
        SELECT
            cs.id,
            cs.submission_date,
            cs.posting_date,
            cs.post_type,
            cs.link_post,
            cs.level,
            cs.notes,
            cs.reason,
            cs.creator_id,
            cr.tiktok_id,
            cr.full_name,
            cs.management_id,
            am.agency_name,
            cs.category_id,
            cat.category_name,
            cs.status_id,
            sm.status_name,
            cs.created_at
        FROM public.content_submissions cs
        LEFT JOIN public.creator_registry cr
            ON cs.creator_id = cr.id
        LEFT JOIN public.agency_map am
            ON cs.management_id = am.id
        LEFT JOIN public.category_map cat
            ON cs.category_id = cat.id
        LEFT JOIN public.status_map sm
            ON cs.status_id = sm.id
        ORDER BY cs.created_at DESC, cs.id DESC;
    """
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        return rows
    finally:
        conn.close()


def fetch_status_map():
    """
    Fetch status_map for dropdowns:
    id, status_name
    """
    conn = get_connection()
    sql = """
        SELECT id, status_name
        FROM public.status_map
        ORDER BY id;
    """
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=["id", "status_name"])
        return df
    finally:
        conn.close()


# =================================================
# RENDER FUNCTION (USED BY creator.py)
# =================================================

def render():
    st.title("Content Submissions")
    
    # ============ EDITING MODE TOGGLE ============
    col_title, col_toggle = st.columns([3, 1])
    
    with col_title:
        st.write(
            "Monitor submissions with the read-only table below. "
            "Need to update statuses? Open the **Edit Submissions** expander and toggle the lock."
        )
    
    with col_toggle:
        if "editing_mode" not in st.session_state:
            st.session_state.editing_mode = False
        
        editing_mode = st.toggle(
            "🔒 Lock Data",
            value=st.session_state.editing_mode,
            help="Turn this ON before editing to prevent auto-refresh. Turn OFF when done to see new submissions.",
            key="editing_mode_toggle"
        )
        st.session_state.editing_mode = editing_mode
    
    if st.session_state.editing_mode:
        st.warning("⚠️ **Editing Mode Active:** Data is locked and won't auto-refresh. "
                   "Click 'Apply Changes' when done, and the lock will automatically turn off.")
    
    # Fetch data with conditional caching based on editing mode
    if st.session_state.editing_mode:
        @st.cache_data
        def fetch_locked_submissions():
            return fetch_content_submissions()
        submissions_rows = fetch_locked_submissions()
    else:
        submissions_rows = fetch_content_submissions()
    
    if not submissions_rows:
        st.info("No content submissions available.")
        return

    sub_df = pd.DataFrame(submissions_rows)
    sub_df["submission_date"] = pd.to_datetime(sub_df["submission_date"]).dt.normalize()
    sub_df["posting_date"] = pd.to_datetime(sub_df["posting_date"]).dt.date

    # Fetch status map for dropdown options
    status_df = fetch_status_map()
    # Hide status_id = 1 from editable dropdown, but keep full df for mapping
    editable_status_df = status_df[status_df["id"] != 1].copy()
    status_options = editable_status_df["status_name"].tolist()

    # ---------- FILTERS ----------
    st.subheader("Filters")
    
    filter_col1, filter_col2 = st.columns(2)
    
    with filter_col1:
        tiktok_ids = ["(Show All)"] + sorted(sub_df["tiktok_id"].dropna().unique())
        tiktok_filter = st.selectbox("Filter by TikTok ID", tiktok_ids)
    
    with filter_col2:
        min_posting_date = sub_df["posting_date"].min()
        max_posting_date = sub_df["posting_date"].max()
        
        posting_date_range = st.date_input(
            "Filter by Posting Date Range",
            value=(min_posting_date, max_posting_date),
            min_value=min_posting_date,
            max_value=max_posting_date,
            help="Select start and end dates"
        )

    # Apply filters
    filtered_sub_df = sub_df.copy()
    
    if tiktok_filter != "(Show All)":
        filtered_sub_df = filtered_sub_df[filtered_sub_df["tiktok_id"] == tiktok_filter]
    
    if len(posting_date_range) == 2:
        start_date, end_date = posting_date_range
        filtered_sub_df = filtered_sub_df[
            (filtered_sub_df["posting_date"] >= start_date) & 
            (filtered_sub_df["posting_date"] <= end_date)
        ]

    # ---------- PRETTY VIEW-ONLY TABLE ----------
    st.subheader("📊 Submissions Overview (Read-Only)")
    
    view_columns = [
        "id", 
        "tiktok_id", 
        "agency_name", 
        "posting_date", 
        "category_name",
        "post_type", 
        "link_post", 
        "level", 
        "status_name", 
        "reason"
    ]
    
    view_df = filtered_sub_df[view_columns].copy()
    
    view_df["reason"] = view_df["reason"].fillna("—")
    view_df["level"] = view_df["level"].fillna(0).astype(int)
    view_df["category_name"] = view_df["category_name"].fillna("Uncategorized")
    
    st.dataframe(
        view_df,
        use_container_width=True,
        height=400,
        hide_index=True,
    )
    
    st.caption(f"📌 Showing **{len(view_df)}** submission(s)")

    # ---------- EDITABLE TABLE ----------
    with st.expander("✏️ Edit Submissions (Status & Reason)", expanded=False):
        
        if not st.session_state.editing_mode:
            st.info("💡 **Tip:** Turn on the '🔒 Lock Data' toggle above before editing.")
        
        st.write("**Instructions:** Edit the Status or Reason columns below, then click **Apply Changes** to save.")
        
        edit_columns = [
            "id", 
            "tiktok_id", 
            "full_name",
            "posting_date", 
            "link_post", 
            "status_name", 
            "reason"
        ]
        
        edit_df = filtered_sub_df[edit_columns].copy()
        edit_df["reason"] = edit_df["reason"].fillna("")
        
        edit_column_config = {
            "id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
            "tiktok_id": st.column_config.TextColumn("TikTok ID", disabled=True, width="medium"),
            "full_name": st.column_config.TextColumn("Full Name", disabled=True, width="medium"),
            "posting_date": st.column_config.DateColumn("Posting Date", disabled=True, width="small"),
            "link_post": st.column_config.LinkColumn(
                "Post Link", disabled=True, width="medium", display_text="🔗 View"
            ),
            "status_name": st.column_config.SelectboxColumn(
                "Status",
                options=status_options,
                required=True,
                width="medium",
                help="Change the submission status"
            ),
            "reason": st.column_config.TextColumn(
                "Reason",
                help="Explain why you changed the status (max 500 chars)",
                max_chars=500,
                width="large"
            ),
        }
        
        edited_df = st.data_editor(
            edit_df,
            column_config=edit_column_config,
            use_container_width=True,
            num_rows="fixed",
            hide_index=True,
            key="content_submissions_editor"
        )

        # Detect unsaved changes
        changes = []
        for idx in edited_df.index:
            original_status = edit_df.loc[idx, "status_name"]
            edited_status = edited_df.loc[idx, "status_name"]
            original_reason = edit_df.loc[idx, "reason"]
            edited_reason = edited_df.loc[idx, "reason"]

            if original_status != edited_status or original_reason != edited_reason:
                status_id = int(
                    status_df[status_df["status_name"] == edited_status]["id"].values[0]
                )
                changes.append({
                    "id": int(edited_df.loc[idx, "id"]),
                    "status_id": status_id,
                    "reason": edited_reason if edited_reason.strip() else None
                })

        if changes:
            st.warning("⚠️ You have unsaved changes. Click **Apply Changes** to save to database.")

        col_button, _ = st.columns([1, 3])
        with col_button:
            apply_button = st.button("💾 Apply Changes", type="primary", use_container_width=True)
        
        if apply_button:
            if changes:
                try:
                    bulk_update_content_submissions(changes)
                    st.success(f"✅ Successfully updated **{len(changes)}** submission(s)!")
                    st.session_state.editing_mode = False
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error updating submissions: {e}")
            else:
                st.info("ℹ️ No changes detected.")
