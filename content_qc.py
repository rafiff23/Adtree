import pandas as pd
import streamlit as st

# Adjust import paths to match your project layout
from db import (
    get_connection,
    bulk_update_content_submissions,
)
from content_submission import fetch_content_submissions, fetch_status_map


def render():
    st.title("Creator Content QC")

    # =========================
    # 1. LOAD BASE DATA (FROM DB)
    # =========================
    submissions_rows = fetch_content_submissions()
    if not submissions_rows:
        st.info("No content submissions available.")
        return

    sub_df = pd.DataFrame(submissions_rows)

    # Match the same date handling as your main page
    sub_df["submission_date"] = pd.to_datetime(sub_df["submission_date"]).dt.normalize()
    sub_df["posting_date"] = pd.to_datetime(sub_df["posting_date"]).dt.date

    # =========================
    # 2. FILTER FORM (SNAPSHOT EXPORT)
    # =========================
    min_posting_date = sub_df["posting_date"].min()
    max_posting_date = sub_df["posting_date"].max()

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

    # Init session state containers for QC snapshot
    if "qc_original" not in st.session_state:
        st.session_state.qc_original = None
    if "qc_editable" not in st.session_state:
        st.session_state.qc_editable = None

    # When user clicks export, we take a FROZEN snapshot
    if export_button:
        filtered_df = sub_df[
            (sub_df["posting_date"] >= start_date)
            & (sub_df["posting_date"] <= end_date)
        ].copy()

        if tiktok_filter.strip():
            filtered_df = filtered_df[
                filtered_df["tiktok_id"].astype(str).str.contains(
                    tiktok_filter.strip(), case=False, na=False
                )
            ]

        if filtered_df.empty:
            st.warning("No rows found for this filter. Try another date or TikTok ID.")
        else:
            # Keep only relevant columns for QC
            qc_columns = [
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
                # later you can add "video_quality" here once it's in DB + SELECT
            ]
            filtered_df = filtered_df[qc_columns].copy()

            # Normalize NaNs
            filtered_df["reason"] = filtered_df["reason"].fillna("")

            # Store snapshot in session
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

    if qc_editable is None or qc_editable.empty:
        st.info("No QC batch loaded yet. Use the filters above and click **Export QC Data**.")
        return

    st.subheader("📝 QC Snapshot (Editable)")

    # Fetch status map for dropdown
    status_df = fetch_status_map()
    editable_status_df = status_df[status_df["id"] != 1].copy()  # same rule as main page
    status_options = editable_status_df["status_name"].tolist()
    status_name_to_id = dict(
        zip(status_df["status_name"], status_df["id"])
    )  # full map for updates

    # Configure editable columns
    edit_column_config = {
        "status_name": st.column_config.SelectboxColumn(
            "Status",
            options=status_options,
            required=True,
            help="Set latest status for this submission.",
        ),
        "reason": st.column_config.TextColumn(
            "Reason",
            help="Explain why this status / quality is given.",
        ),
        # Example for later when you add a quality column:
        # "video_quality": st.column_config.SelectboxColumn(
        #     "Video Quality",
        #     options=["Low", "Medium", "High"],
        #     required=False,
        # ),
    }

    # Non-editable columns
    disabled_cols = [
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

    # Update session with the edited snapshot
    st.session_state.qc_editable = edited_df.copy()

    # =========================
    # 4. IMPORT BUTTON (WRITE BACK TO DB)
    # =========================
    if st.button("📥 Import QC Changes to DB", type="primary", use_container_width=True):
        qc_original = st.session_state.qc_original
        qc_editable = st.session_state.qc_editable

        if qc_original is None or qc_editable is None or qc_editable.empty:
            st.info("No QC batch loaded to import.")
            return

        # Build diff ONLY when user clicks Import
        original_idx = qc_original.set_index("id")
        edited_idx = qc_editable.set_index("id")

        changes = []
        for idx in edited_idx.index:
            orig_status = original_idx.loc[idx, "status_name"]
            new_status = edited_idx.loc[idx, "status_name"]

            orig_reason = original_idx.loc[idx, "reason"]
            new_reason = edited_idx.loc[idx, "reason"]

            # Normalize None / NaN / whitespace
            orig_reason_clean = ("" if pd.isna(orig_reason) else str(orig_reason)).strip()
            new_reason_clean = ("" if pd.isna(new_reason) else str(new_reason)).strip()

            # If no change on both fields, skip
            if (orig_status == new_status) and (orig_reason_clean == new_reason_clean):
                continue

            # Map status_name -> status_id for update
            if new_status not in status_name_to_id:
                # Safety: skip if mapping missing
                continue

            status_id = int(status_name_to_id[new_status])

            changes.append(
                {
                    "id": int(idx),
                    "status_id": status_id,
                    "reason": new_reason_clean if new_reason_clean else None,
                }
            )

        if not changes:
            st.info("No changes detected to import.")
            return

        try:
            bulk_update_content_submissions(changes)
            st.success(f"✅ Successfully updated **{len(changes)}** submission(s) in DB.")

            # After successful import, current edited snapshot becomes the new baseline
            st.session_state.qc_original = st.session_state.qc_editable.copy()

        except Exception as e:
            st.error(f"❌ Error while importing QC changes to DB: {e}")
