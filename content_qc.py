import pandas as pd
import streamlit as st

from db import bulk_update_content_submissions
from content_submission import fetch_content_submissions, fetch_status_map

QC_EDITOR_KEY = "qc_editor"


def render():
    st.title("Creator Content QC")

    # =========================
    # 0) INIT STATE
    # =========================
    if "qc_batch" not in st.session_state:
        st.session_state.qc_batch = None  # editable snapshot shown in data_editor
    if "qc_batch_original" not in st.session_state:
        st.session_state.qc_batch_original = None  # baseline snapshot for diff
    if "qc_success_msg" not in st.session_state:
        st.session_state.qc_success_msg = None  # persist success message
    if "qc_success_count" not in st.session_state:
        st.session_state.qc_success_count = None

    # Show last success message (longer, persistent)
    if st.session_state.qc_success_msg:
        st.success(st.session_state.qc_success_msg)
        col_a, col_b = st.columns([1, 5])
        with col_a:
            if st.button("Clear", key="qc_clear_success"):
                st.session_state.qc_success_msg = None
                st.session_state.qc_success_count = None
                st.rerun()
        with col_b:
            st.caption("Tip: message stays until you clear it.")

    # =========================
    # 1) LOAD BASE DATA (DB) - keep same select logic
    # =========================
    submissions_rows = fetch_content_submissions()
    if not submissions_rows:
        st.info("No content submissions available.")
        return

    sub_df = pd.DataFrame(submissions_rows)

    # Date handling (same style)
    sub_df["submission_date"] = pd.to_datetime(sub_df["submission_date"], errors="coerce")
    sub_df["posting_date"] = pd.to_datetime(sub_df["posting_date"], errors="coerce").dt.date

    if "Time Submitted" not in sub_df.columns:
        sub_df["Time Submitted"] = sub_df["submission_date"]

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
        st.session_state.qc_success_msg = None
        st.session_state.qc_success_count = None

        mask = (sub_df["posting_date"] >= start_date) & (sub_df["posting_date"] <= end_date)

        if tiktok_filter.strip():
            mask &= sub_df["tiktok_id"].astype(str).str.contains(
                tiktok_filter.strip(), case=False, na=False
            )

        filtered_df = sub_df.loc[mask].copy()

        if filtered_df.empty:
            st.session_state.qc_batch = None
            st.session_state.qc_batch_original = None

            # Reset editor internal buffer for clean start
            if QC_EDITOR_KEY in st.session_state:
                del st.session_state[QC_EDITOR_KEY]

            st.warning("No rows found for this filter. Try another date or TikTok ID.")
            return

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

        # IMPORTANT:
        # - Keep ORIGINAL with real NULLs (do NOT fillna)
        # - Make EDITABLE version with blanks for UI
        original_df = filtered_df.reset_index(drop=True)
        editable_df = original_df.copy(deep=True)

        if "reason" in editable_df.columns:
            editable_df["reason"] = editable_df["reason"].where(editable_df["reason"].notna(), "")

        st.session_state.qc_batch_original = original_df
        st.session_state.qc_batch = editable_df

        # Reset editor internal buffer so it uses the new snapshot
        if QC_EDITOR_KEY in st.session_state:
            del st.session_state[QC_EDITOR_KEY]

        st.success(
            f"Exported **{len(editable_df)}** row(s). Edit many rows, then click Import once."
        )

    # =========================
    # 3) QC EDITOR
    # =========================
    qc_batch = st.session_state.qc_batch
    qc_original = st.session_state.qc_batch_original

    if qc_batch is None or qc_batch.empty:
        st.info("No QC batch loaded. Use filters and click **Export QC Data**.")
        return

    st.subheader("📝 QC Snapshot (Editable)")

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
            help="Reason can be blank. Status still can be saved.",
        )
    if "reason" in qc_batch.columns:
        edit_column_config["reason"] = st.column_config.TextColumn(
            "Reason (optional)",
            help="Leave blank if not needed.",
            max_chars=500,
        )

    disabled_cols = [c for c in [
        "id", "tiktok_id", "full_name", "agency_name", "posting_date",
        "Time Submitted", "post_type", "link_post", "category_name"
    ] if c in qc_batch.columns]

    edited_df = st.data_editor(
        qc_batch,
        column_config=edit_column_config,
        disabled=disabled_cols,
        use_container_width=True,
        num_rows="fixed",
        hide_index=True,
        key=QC_EDITOR_KEY,
    )

    # NOTE: we intentionally do NOT write edited_df back into qc_batch every rerun.
    # Streamlit keeps the edits in the widget state via QC_EDITOR_KEY.

    # =========================
    # 4) IMPORT BUTTON (DIFF ONLY HERE)
    # =========================
    if st.button("📥 Import QC Changes to DB", type="primary", use_container_width=True):
        if qc_original is None or qc_original.empty:
            st.warning("Baseline snapshot missing. Please Export QC Data again.")
            return

        # baseline (keeps NULL info)
        base = qc_original.copy()
        cur = edited_df.copy()

        # Align by id
        base_by_id = base.set_index("id")
        cur_by_id = cur.set_index("id")

        changes = []

        for row_id in cur_by_id.index:
            base_row = base_by_id.loc[row_id]
            cur_row = cur_by_id.loc[row_id]

            base_status = base_row.get("status_name")
            cur_status = cur_row.get("status_name")

            # baseline reason may be NULL; current reason will be "" or text
            base_reason_raw = base_row.get("reason")
            base_reason_clean = "" if pd.isna(base_reason_raw) else str(base_reason_raw).strip()

            cur_reason_raw = cur_row.get("reason")
            cur_reason_clean = "" if pd.isna(cur_reason_raw) else str(cur_reason_raw).strip()

            # detect changes
            status_changed = (base_status != cur_status)
            reason_changed = (base_reason_clean != cur_reason_clean)

            if not status_changed and not reason_changed:
                continue

            payload = {"id": int(row_id)}

            # If status changed, map to status_id
            if status_changed:
                if cur_status not in status_name_to_id:
                    continue
                payload["status_id"] = int(status_name_to_id[cur_status])

            # CRITICAL FIX:
            # Always include 'reason' in payload so db function won't crash.
            # But don't overwrite existing reason unless:
            # - reason actually changed, OR
            # - status changed and user intentionally left reason blank (allowed)
            #
            # We will send:
            # - None if blank
            # - text otherwise
            if reason_changed or status_changed:
                payload["reason"] = (cur_reason_clean if cur_reason_clean else None)
            else:
                # not reachable due to condition, but safe
                payload["reason"] = (base_reason_clean if base_reason_clean else None)

            changes.append(payload)

        if not changes:
            st.info("No changes detected to import.")
            return

        try:
            bulk_update_content_submissions(changes)

            st.session_state.qc_success_count = len(changes)
            st.session_state.qc_success_msg = (
                f"✅ Successfully updated **{len(changes)}** submission(s) in DB. "
                "You can continue editing and import again."
            )

            # Update baseline to current state so you can edit the SAME ROW again
            # Keep NULLs in baseline: convert "" to None for baseline storage
            new_base = cur.copy(deep=True)
            if "reason" in new_base.columns:
                new_base["reason"] = new_base["reason"].apply(
                    lambda x: None if (pd.isna(x) or str(x).strip() == "") else str(x).strip()
                )

            st.session_state.qc_batch_original = new_base
            st.session_state.qc_batch = cur  # keep UI snapshot as-is

            # Do NOT st.rerun() immediately; let success message stay visible.
            # If you want, users can keep editing without reset.

        except Exception as e:
            st.error(f"❌ Error while importing QC changes to DB: {e}")
