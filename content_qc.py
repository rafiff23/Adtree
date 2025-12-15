import pandas as pd
import streamlit as st

# Adjust import paths to match your project layout
from db import (
    bulk_update_content_submissions,
)
from content_submission import fetch_content_submissions, fetch_status_map


def render():
    st.title("Creator Content QC")

    # =========================
    # 0. SESSION STATE INITIALISATION
    # =========================
    if "qc_original" not in st.session_state:
        st.session_state.qc_original = None
    if "qc_editable" not in st.session_state:
        st.session_state.qc_editable = None

    # =========================
    # 1. LOAD BASE DATA (FROM DB)
    # =========================
    submissions_rows = fetch_content_submissions()
    if not submissions_rows:
        st.info("No content submissions available.")
        return

    sub_df = pd.DataFrame(submissions_rows)

    # Basic date handling – follow main content_submission logic style
    sub_df["submission_date"] = pd.to_datetime(sub_df["submission_date"])
    sub_df["posting_date"] = pd.to_datetime(sub_df["posting_date"]).dt.date

    # For display
    if "Time Submitted" not in sub_df.columns:
        sub_df["Time Submitted"] = sub_df["submission_date"]

    min_posting_date = sub_df["posting_date"].min()
    max_posting_date = sub_df["posting_date"].max()

    # =========================
    # 2. FILTER FORM (EXPORT SNAPSHOT)
    # =========================
    with st.form("qc_filters"):
        col1, col2, col3 = st.columns(3)

        with col1:
            start_date = st.date_input(
                "Start posting date",
                value=min_posting_date,
                min_value=min_posting_date,
                max_value=max_posting_date,
            )

        with col2:
            end_date = st.date_input(
                "End posting date",
                value=max_posting_date,
                min_value=min_posting_date,
                max_value=max_posting_date,
            )

        with col3:
            tiktok_filter = st.text_input(
                "Filter by TikTok ID (optional)",
                value="",
                placeholder="e.g. @creator123",
            )

        export_button = st.form_submit_button("📤 Export QC Data")

    # When user clicks export, we take a FROZEN snapshot
    if export_button:
        mask = (
            (sub_df["posting_date"] >= start_date)
            & (sub_df["posting_date"] <= end_date)
        )

        if tiktok_filter.strip():
            mask &= sub_df["tiktok_id"].astype(str).str.contains(
                tiktok_filter.strip(), case=False, na=False
            )

        filtered_df = sub_df.loc[mask].copy()

        if filtered_df.empty:
            st.session_state.qc_original = None
            st.session_state.qc_editable = None
            st.warning("No rows found for this filter. Try another date or TikTok ID.")
        else:
            # Keep only relevant columns for QC (only use columns that exist)
            desired_qc_columns = [
                "id",
                "tiktok_id",
                "full_name",
                "agency_name",
                "posting_date",
                "Time Submitted",
                "post_type",
                "link_post",
                "category_name",
                "status_name",
                "reason",
                # Add more QC-related columns here later if you want
                # e.g. "video_quality"
            ]
            qc_columns = [c for c in desired_qc_columns if c in filtered_df.columns]
            filtered_df = filtered_df[qc_columns].copy()

            # Normalize NaNs
            if "reason" in filtered_df.columns:
                filtered_df["reason"] = filtered_df["reason"].fillna("")

            # Store snapshot in session (this is your frozen QC batch)
            st.session_state.qc_original = filtered_df.copy()
            st.session_state.qc_editable = filtered_df.copy()

            st.success(
                f"Exported **{len(filtered_df)}** row(s) to QC workspace. "
                "New data coming into DB will NOT change this snapshot."
            )

    # =========================
    # 3. QC EDITOR (WORKING ON SNAPSHOT ONLY)
    # =========================
    qc_original = st.session_state.qc_original
    qc_editable = st.session_state.qc_editable

    if qc_original is None or qc_editable is None or qc_editable.empty:
        st.info("No QC batch loaded yet. Use the filters above and click **Export QC Data**.")
        return

    st.subheader("📝 QC Snapshot (Editable)")

    # Fetch status map for dropdown options
    status_df = fetch_status_map()
    # Same rule as in your main content_submission page: exclude id = 1
    editable_status_df = status_df[status_df["id"] != 1].copy()
    status_options = editable_status_df["status_name"].tolist()
    status_name_to_id = dict(
        zip(status_df["status_name"], status_df["id"])
    )

    # Configure editable columns
    edit_column_config = {}
    if "status_name" in qc_editable.columns:
        edit_column_config["status_name"] = st.column_config.SelectboxColumn(
            "Status",
            options=status_options,
            required=True,
            help="Set latest status for this submission.",
        )
    if "reason" in qc_editable.columns:
        edit_column_config["reason"] = st.column_config.TextColumn(
            "Reason",
            help="Explain why this status / quality is given.",
        )
    # Example for future:
    # if "video_quality" in qc_editable.columns:
    #     edit_column_config["video_quality"] = st.column_config.SelectboxColumn(
    #         "Video Quality",
    #         options=["Low", "Medium", "High"],
    #         required=False,
    #     )

    # Non-editable columns (only those that exist)
    non_editable_candidates = [
        "id",
        "tiktok_id",
        "full_name",
        "agency_name",
        "posting_date",
        "Time Submitted",
        "post_type",
        "link_post",
        "category_name",
    ]
    disabled_cols = [c for c in non_editable_candidates if c in qc_editable.columns]

    # User edits only the snapshot in memory
    edited_df = st.data_editor(
        qc_editable,
        column_config=edit_column_config,
        disabled=disabled_cols,
        use_container_width=True,
        num_rows="fixed",
        hide_index=True,
        key="qc_data_editor",
    )

    # Save edited snapshot back to session_state
    st.session_state.qc_editable = edited_df.copy()

    # =========================
    # 4. IMPORT BUTTON (WRITE BACK TO DB ONCE)
    # =========================
    if st.button("📥 Import QC Changes to DB", type="primary", use_container_width=True):
        qc_original = st.session_state.qc_original
        qc_editable = st.session_state.qc_editable

        if qc_original is None or qc_editable is None or qc_editable.empty:
            st.info("No QC batch loaded to import.")
            return

        original_idx = qc_original.set_index("id")
        edited_idx = qc_editable.set_index("id")

        changes = []

        for idx in edited_idx.index:
            row_orig = original_idx.loc[idx]
            row_new = edited_idx.loc[idx]

            # Handle status
            status_changed = False
            new_status_name = None
            if "status_name" in edited_idx.columns:
                orig_status_name = row_orig.get("status_name")
                new_status_name = row_new.get("status_name")
                status_changed = orig_status_name != new_status_name

            # Handle reason
            reason_changed = False
            new_reason_clean = None
            if "reason" in edited_idx.columns:
                orig_reason = row_orig.get("reason")
                new_reason = row_new.get("reason")

                orig_reason_clean = ("" if pd.isna(orig_reason) else str(orig_reason)).strip()
                new_reason_clean = ("" if pd.isna(new_reason) else str(new_reason)).strip()

                reason_changed = orig_reason_clean != new_reason_clean

            # Skip if nothing changed
            if not status_changed and not reason_changed:
                continue

            # Map status_name -> status_id if status changed
            status_id = None
            if status_changed:
                if new_status_name not in status_name_to_id:
                    # Safety guard: skip row if mapping unavailable
                    continue
                status_id = int(status_name_to_id[new_status_name])

            # Build change payload for this row
            change_row = {"id": int(idx)}

            if status_id is not None:
                change_row["status_id"] = status_id

            if reason_changed:
                change_row["reason"] = new_reason_clean if new_reason_clean else None

            changes.append(change_row)

        if not changes:
            st.info("No changes detected to import.")
            return

        try:
            bulk_update_content_submissions(changes)
            st.success(f"✅ Successfully updated **{len(changes)}** submission(s) in DB.")

            # After import, make the current edited snapshot the new baseline
            st.session_state.qc_original = st.session_state.qc_editable.copy()

        except Exception as e:
            st.error(f"❌ Error while importing QC changes to DB: {e}")
