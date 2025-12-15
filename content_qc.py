import pandas as pd
import streamlit as st

from db import bulk_update_content_submissions
from content_submission import fetch_content_submissions, fetch_status_map


QC_EDITOR_KEY = "qc_editor"  # key for data_editor widget state


def _reset_qc_editor_state():
    # Clears the editor's internal edit buffer so a new export starts clean
    if QC_EDITOR_KEY in st.session_state:
        del st.session_state[QC_EDITOR_KEY]


def render():
    st.title("Creator Content QC")

    # =========================
    # 0) INIT STATE
    # =========================
    if "qc_batch" not in st.session_state:
        st.session_state.qc_batch = None          # frozen snapshot (source table shown)
    if "qc_batch_original" not in st.session_state:
        st.session_state.qc_batch_original = None # baseline for diff on Import

    # =========================
    # 1) LOAD BASE DATA (DB) - keep same select logic
    # =========================
    submissions_rows = fetch_content_submissions()
    if not submissions_rows:
        st.info("No content submissions available.")
        return

    sub_df = pd.DataFrame(submissions_rows)

    # Follow your main page date logic style
    sub_df["submission_date"] = pd.to_datetime(sub_df["submission_date"], errors="coerce")
    sub_df["posting_date"] = pd.to_datetime(sub_df["posting_date"], errors="coerce").dt.date

    if "Time Submitted" not in sub_df.columns:
        sub_df["Time Submitted"] = sub_df["submission_date"]

    # guard if posting_date is empty
    if sub_df["posting_date"].isna().all():
        st.warning("posting_date is empty / invalid for all rows.")
        return

    min_posting_date = sub_df["posting_date"].min()
    max_posting_date = sub_df["posting_date"].max()

    # =========================
    # 2) FILTER + EXPORT SNAPSHOT
    # =========================
    with st.form("qc_filters"):
        c1, c2, c3 = st.columns(3)
        with c1:
            start_date = st.date_input(
                "Start posting date",
                value=min_posting_date,
                min_value=min_posting_date,
                max_value=max_posting_date,
            )
        with c2:
            end_date = st.date_input(
                "End posting date",
                value=max_posting_date,
                min_value=min_posting_date,
                max_value=max_posting_date,
            )
        with c3:
            tiktok_filter = st.text_input(
                "Filter by TikTok ID (optional)",
                value="",
                placeholder="e.g. @creator123",
            )

        export_button = st.form_submit_button("📤 Export QC Data")

    if export_button:
        mask = (sub_df["posting_date"] >= start_date) & (sub_df["posting_date"] <= end_date)

        if tiktok_filter.strip():
            mask &= sub_df["tiktok_id"].astype(str).str.contains(
                tiktok_filter.strip(), case=False, na=False
            )

        filtered_df = sub_df.loc[mask].copy()

        if filtered_df.empty:
            st.session_state.qc_batch = None
            st.session_state.qc_batch_original = None
            _reset_qc_editor_state()
            st.warning("No rows found for this filter. Try another date or TikTok ID.")
            return

        # Keep only QC columns (only those that exist)
        desired_cols = [
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
        ]
        cols = [c for c in desired_cols if c in filtered_df.columns]
        filtered_df = filtered_df[cols].copy()

        # Normalize reason
        if "reason" in filtered_df.columns:
            filtered_df["reason"] = filtered_df["reason"].fillna("")

        # IMPORTANT: reset index so editor row identity stays stable
        filtered_df = filtered_df.reset_index(drop=True)

        # Save snapshot + baseline
        st.session_state.qc_batch = filtered_df
        st.session_state.qc_batch_original = filtered_df.copy(deep=True)

        # Clear previous editor buffer so new export starts clean
        _reset_qc_editor_state()

        st.success(
            f"Exported **{len(filtered_df)}** row(s) into QC snapshot. "
            "You can edit many rows, then click Import once."
        )

    # =========================
    # 3) SHOW EDITOR (EDIT SNAPSHOT ONLY)
    # =========================
    qc_batch = st.session_state.qc_batch
    qc_original = st.session_state.qc_batch_original

    if qc_batch is None or qc_batch.empty:
        st.info("No QC batch loaded. Use filters and click **Export QC Data**.")
        return

    st.subheader("📝 QC Snapshot (Editable)")

    # status dropdown options (same as main: exclude id=1)
    status_df = fetch_status_map()
    editable_status_df = status_df[status_df["id"] != 1].copy()
    status_options = editable_status_df["status_name"].tolist()
    status_name_to_id = dict(zip(status_df["status_name"], status_df["id"]))

    edit_column_config = {}
    if "status_name" in qc_batch.columns:
        edit_column_config["status_name"] = st.column_config.SelectboxColumn(
            "Status",
            options=status_options,
            required=True,
        )
    if "reason" in qc_batch.columns:
        edit_column_config["reason"] = st.column_config.TextColumn(
            "Reason",
            max_chars=500,
        )

    disabled_cols = [c for c in [
        "id", "tiktok_id", "full_name", "agency_name", "posting_date",
        "Time Submitted", "post_type", "link_post", "category_name"
    ] if c in qc_batch.columns]

    # CRITICAL: do NOT overwrite qc_batch on every rerun.
    # Let Streamlit keep the editor edits internally via QC_EDITOR_KEY.
    edited_df = st.data_editor(
        qc_batch,
        column_config=edit_column_config,
        disabled=disabled_cols,
        use_container_width=True,
        num_rows="fixed",
        hide_index=True,
        key=QC_EDITOR_KEY,
    )

    # =========================
    # 4) IMPORT BUTTON (DIFF ONLY HERE)
    # =========================
    if st.button("📥 Import QC Changes to DB", type="primary", use_container_width=True):
        if qc_original is None or qc_original.empty:
            st.warning("No baseline snapshot found. Please Export QC Data again.")
            return

        # Use edited_df returned from data_editor (has latest edits)
        baseline = qc_original.copy()
        current = edited_df.copy()

        # Align by row position + id (since we reset_index)
        # Convert to dict by id for diff
        base_by_id = baseline.set_index("id")
        cur_by_id = current.set_index("id")

        changes = []
        for row_id in cur_by_id.index:
            base_row = base_by_id.loc[row_id]
            cur_row = cur_by_id.loc[row_id]

            # Compare status_name + reason only (extend later if you add video_quality)
            base_status = base_row.get("status_name")
            cur_status = cur_row.get("status_name")

            base_reason = ("" if pd.isna(base_row.get("reason")) else str(base_row.get("reason"))).strip()
            cur_reason = ("" if pd.isna(cur_row.get("reason")) else str(cur_row.get("reason"))).strip()

            if base_status == cur_status and base_reason == cur_reason:
                continue

            payload = {"id": int(row_id)}

            if base_status != cur_status:
                if cur_status not in status_name_to_id:
                    continue
                payload["status_id"] = int(status_name_to_id[cur_status])

            if base_reason != cur_reason:
                payload["reason"] = cur_reason if cur_reason else None

            changes.append(payload)

        if not changes:
            st.info("No changes detected to import.")
            return

        try:
            bulk_update_content_submissions(changes)
            st.success(f"✅ Updated {len(changes)} row(s) in DB.")

            # After save: update baseline + snapshot to the edited result
            st.session_state.qc_batch = current.reset_index(drop=True)
            st.session_state.qc_batch_original = st.session_state.qc_batch.copy(deep=True)

            # Reset editor buffer so it's clean after save
            _reset_qc_editor_state()

            st.rerun()

        except Exception as e:
            st.error(f"❌ Error while importing QC changes to DB: {e}")
