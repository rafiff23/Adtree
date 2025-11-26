import datetime as dt
from datetime import date
import pandas as pd
import streamlit as st

from db import (
    insert_creator_registry_row,
    fetch_creator_registry,
    update_creator_registry_row,
    get_connection,
    bulk_update_content_submissions,
)

# ----------------- PAGE CONFIG -----------------
st.set_page_config(page_title="Creator Registry & Submissions", layout="wide")


# ----------------- CONSTANTS -----------------
AGENCY_OPTIONS = [
    "Adtree Digital Indonesia",
    "Golden Maker",
    "WH Management",
    "TB Management",
    "BTC Management",
    "HM Agency",
]

# ----------------- SIDEBAR NAV -----------------
page = st.sidebar.radio(
    "Navigation",
    ["Creator Registry", "Creator List", "Content Submissions"],
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
            cs.reason,              -- ← ADD THIS LINE
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

# def fetch_content_submissions():
#     """
#     Fetch joined view for content_submissions:
#     - joins creator_registry, agency_map, category_map, status_map
#     """
#     conn = get_connection()
#     sql = """
#         SELECT
#             cs.id,
#             cs.submission_date,
#             cs.posting_date,
#             cs.post_type,
#             cs.link_post,
#             cs.level,
#             cs.notes,
#             cs.creator_id,
#             cr.tiktok_id,
#             cr.full_name,
#             cs.management_id,
#             am.agency_name,
#             cs.category_id,
#             cat.category_name,
#             cs.status_id,
#             sm.status_name,
#             cs.created_at
#         FROM public.content_submissions cs
#         LEFT JOIN public.creator_registry cr
#             ON cs.creator_id = cr.id
#         LEFT JOIN public.agency_map am
#             ON cs.management_id = am.id
#         LEFT JOIN public.category_map cat
#             ON cs.category_id = cat.id
#         LEFT JOIN public.status_map sm
#             ON cs.status_id = sm.id
#         ORDER BY cs.created_at DESC, cs.id DESC;
#     """
#     try:
#         with conn:
#             with conn.cursor() as cur:
#                 cur.execute(sql)
#                 rows = cur.fetchall()
#         return rows
#     finally:
#         conn.close()


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
        return pd.DataFrame(rows)
    finally:
        conn.close()


def update_content_submission_row(row_id, updated_fields: dict):
    """
    Update only specified fields in content_submissions.
    Example:
        update_content_submission_row(10, {"status_id": 2, "notes": "Flagged"})
    """
    if not updated_fields:
        return

    set_clause = ", ".join([f"{col} = %({col})s" for col in updated_fields.keys()])
    sql = f"""
        UPDATE public.content_submissions
        SET {set_clause}
        WHERE id = %(id)s
    """

    params = updated_fields.copy()
    params["id"] = row_id

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
    finally:
        conn.close()


# =================================================
# PAGE 1: CREATOR REGISTRY (ADD NEW CREATOR)
# =================================================
if page == "Creator Registry":
    st.title("Creator Registry")
    st.write("Register new creators into the system.")

    today = dt.date.today()
    month_label = today.strftime("%Y-%m")
    binding_status = "Unbound"      # default, can be changed later
    onboarding_date = None          # will be set when Bound later

    with st.form("creator_registry_form", clear_on_submit=True):
        agency_name = st.selectbox("Agency Name", AGENCY_OPTIONS)

        col_tid, col_followers = st.columns([2, 1])

        with col_tid:
            tiktok_id = st.text_input(
                "TikTok ID (without @)",
                placeholder="rforramaaa",
                help="Cannot start with @ and cannot contain spaces.",
            )

        with col_followers:
            followers = st.number_input(
                "Followers (optional)",
                min_value=0,
                step=1,
                help="Exact follower count if known. Leave as 0 if unknown.",
            )

        full_name = st.text_input("Full Name")
        domicile = st.text_input("Domicile (City / Country)")
        uid = st.text_input("UID (numbers only)")

        # Phone number input (after +62)
        st.write("Phone Number")
        col_code, col_number = st.columns([1, 3])

        with col_code:
            st.text_input(
                "Code",
                value="+62",
                disabled=True,
                label_visibility="collapsed",
            )

        with col_number:
            phone_rest = st.text_input(
                "Phone Number (without +62 or 62)",
                placeholder="81234567890",
                label_visibility="collapsed",
                help="Do NOT type +62 or 62 here. It is already added.",
            )

        notes = st.text_area("Notes (optional)", height=80)

        # Binding status (display only for now)
        st.text_input(
            "Binding Status",
            value=binding_status,
            disabled=True,
            help="This will be updated when the creator is Bound.",
        )

        submitted = st.form_submit_button("Save Creator")

        if submitted:
            # ---------- VALIDATIONS ----------
            if not agency_name:
                st.error("Agency Name is required.")
                st.stop()

            if not tiktok_id:
                st.error("TikTok ID cannot be empty.")
                st.stop()

            if tiktok_id.startswith("@"):
                st.error("TikTok ID must NOT start with '@'. Please remove it.")
                st.stop()

            if " " in tiktok_id:
                st.error("TikTok ID must NOT contain spaces.")
                st.stop()

            if not full_name.strip():
                st.error("Full Name is required.")
                st.stop()

            if not domicile.strip():
                st.error("Domicile is required.")
                st.stop()

            if not uid.strip():
                st.error("UID is required.")
                st.stop()

            if not uid.isdigit():
                st.error("UID must contain numbers only.")
                st.stop()

            if not phone_rest.strip():
                st.error("Phone number cannot be empty.")
                st.stop()

            if phone_rest.startswith("+") or phone_rest.startswith("62"):
                st.error("Do NOT type +62 or 62 in the phone field. Only the remaining number, e.g. 8123xxxx.")
                st.stop()

            if not phone_rest.isdigit():
                st.error("Phone number (after +62) must contain only digits.")
                st.stop()

            phone_number = f"+62{phone_rest}"

            # Build TikTok link automatically
            tiktok_link = f"https://www.tiktok.com/@{tiktok_id}"

            # Followers: optional -> treat 0 as None if you prefer
            followers_value = int(followers) if followers > 0 else None

            # Insert into DB
            try:
                new_id = insert_creator_registry_row(
                    agency_name=agency_name,
                    tiktok_id=tiktok_id,
                    followers=followers_value,
                    full_name=full_name,
                    domicile=domicile,
                    uid=uid,
                    phone_number=phone_number,
                    tiktok_link=tiktok_link,
                    binding_status=binding_status,
                    onboarding_date=onboarding_date,
                    month_label=month_label,
                    notes=notes or None,
                )
            except Exception as e:
                st.error(f"Error saving creator: {e}")
                st.stop()

            st.success(f"Creator saved successfully with ID {new_id} ✅")
            st.info(f"TikTok Link: {tiktok_link}")
            st.info(f"Phone stored as: {phone_number}")


# =================================================
# PAGE 2: CREATOR LIST (VIEW + EDIT SPECIFIC)
# =================================================
elif page == "Creator List":
    st.title("Creator List")
    st.write("View existing creators and edit specific profiles.")

    rows = fetch_creator_registry()
    if not rows:
        st.info("No creator data available.")
        st.stop()

    df = pd.DataFrame(rows)
    df = df.sort_values("id", ascending=True).reset_index(drop=True)

    # Add WhatsApp link
    df["whatsapp_link"] = "https://wa.me/" + df["phone_number"].str.replace("+", "", n=1)

    # ---------- FILTERS ----------
    st.subheader("Filters")

    tiktok_ids = ["(Show All)"] + sorted(df["tiktok_id"].unique())
    tiktok_id_filter = st.selectbox("Filter by TikTok ID", tiktok_ids)

    # NEW: Filter by Binding Status
    binding_options = ["(Show All)"] + sorted(df["binding_status"].dropna().unique())
    binding_filter = st.selectbox("Filter by Binding Status", binding_options)

    filtered_df = df.copy()

    if tiktok_id_filter != "(Show All)":
        filtered_df = filtered_df[filtered_df["tiktok_id"] == tiktok_id_filter]

    if binding_filter != "(Show All)":
        filtered_df = filtered_df[filtered_df["binding_status"] == binding_filter]

    # ---------- VIEW-ONLY TABLE ----------
    st.subheader("Creator Data (View Only)")
    st.dataframe(
        filtered_df,
        use_container_width=True,
        height=400,
    )

    # ---------- EDIT SPECIFIC CREATOR ----------
    with st.expander("Edit Creator (Select TikTok ID)", expanded=False):

        st.write("Select a TikTok ID to edit its profile:")

        edit_tiktok_id = st.selectbox(
            "TikTok ID to Edit",
            sorted(df["tiktok_id"].unique())
        )

        row = df[df["tiktok_id"] == edit_tiktok_id].iloc[0]
        row_id = int(row["id"])

        st.write(f"Editing creator with ID: **{row_id}**")

        with st.form("edit_creator_form"):

            agency_name = st.selectbox(
                "Agency Name",
                AGENCY_OPTIONS,
                index=AGENCY_OPTIONS.index(row["agency_name"]) if row["agency_name"] in AGENCY_OPTIONS else 0,
            )

            tiktok_id_new = st.text_input(
                "TikTok ID (without @)",
                value=row["tiktok_id"],
            )

            followers_raw = row["followers"]
            if pd.isna(followers_raw) or followers_raw is None:
                followers_default = 0
            else:
                followers_default = int(followers_raw)

            followers_new = st.number_input(
                "Followers",
                min_value=0,
                step=1,
                value=followers_default,
            )

            full_name_new = st.text_input("Full Name", value=row["full_name"])
            domicile_new = st.text_input("Domicile", value=row["domicile"])
            uid_new = st.text_input("UID", value=row["uid"])

            st.write("Phone Number")
            col_code, col_num = st.columns([1, 3])
            with col_code:
                st.text_input("Code", value="+62", disabled=True, label_visibility="collapsed")
            with col_num:
                phone_value = row["phone_number"] or ""
                phone_rest = phone_value.replace("+62", "")
                phone_new = st.text_input("Phone (without +62)", value=phone_rest)

            tiktok_link_new = st.text_input("TikTok Link", value=row["tiktok_link"])

            binding_status_new = st.selectbox(
                "Binding Status",
                ["Unbound", "Bound"],
                index=0 if row["binding_status"] == "Unbound" else 1,
            )

            onboarding_raw = row["onboarding_date"]
            if onboarding_raw is None or pd.isna(onboarding_raw):
                onboarding_default = dt.date.today()
            else:
                onboarding_default = onboarding_raw

            onboarding_date_new = st.date_input(
                "Onboarding Date",
                value=onboarding_default,
            )

            month_label_new = st.text_input("Month Label (YYYY-MM)", value=row["month_label"])

            notes_new = st.text_area("Notes", value=row["notes"] if row["notes"] else "")

            submit_edit = st.form_submit_button("Apply Changes")

        if submit_edit:
            if not uid_new.isdigit():
                st.error("UID must contain numbers only.")
                st.stop()

            if phone_new.startswith("+") or phone_new.startswith("62"):
                st.error("Do NOT include +62 or 62. Only the remaining number.")
                st.stop()

            if not phone_new.isdigit():
                st.error("Phone number must contain digits only.")
                st.stop()

            phone_final = f"+62{phone_new}"

            updated_fields = {}

            def check_change(key, new_val):
                if row[key] != new_val:
                    updated_fields[key] = new_val

            check_change("agency_name", agency_name)
            check_change("tiktok_id", tiktok_id_new)
            check_change("followers", followers_new if followers_new > 0 else None)
            check_change("full_name", full_name_new)
            check_change("domicile", domicile_new)
            check_change("uid", uid_new)
            check_change("phone_number", phone_final)
            check_change("tiktok_link", tiktok_link_new)
            check_change("binding_status", binding_status_new)
            check_change("onboarding_date", onboarding_date_new)
            check_change("month_label", month_label_new)
            check_change("notes", notes_new if notes_new else None)

            if updated_fields:
                try:
                    update_creator_registry_row(row_id, updated_fields)
                    st.success("Creator data updated successfully! ✅")
                    st.info("Refresh the page to see updated values in the table above.")
                except Exception as e:
                    st.error(f"Error updating creator: {e}")
            else:
                st.info("No changes detected.")


# =================================================
# PAGE 3: CONTENT SUBMISSIONS (VIEW + EDIT)
# =================================================
elif page == "Content Submissions":
    st.title("Content Submissions")
    st.write("Monitor submissions with the read-only table below. Need to update statuses? Open the **Edit Submissions** expander.")

    # Fetch data
    submissions_rows = fetch_content_submissions()
    if not submissions_rows:
        st.info("No content submissions available.")
        st.stop()

    sub_df = pd.DataFrame(submissions_rows)
    sub_df["submission_date"] = pd.to_datetime(sub_df["submission_date"]).dt.normalize()
    sub_df["posting_date"] = pd.to_datetime(sub_df["posting_date"]).dt.date

    # Fetch status map for dropdown options (needed for editable section)
    status_df = fetch_status_map()
    status_options = status_df["status_name"].tolist()

    # ---------- FILTERS (SIDE BY SIDE) ----------
    st.subheader("Filters")
    
    filter_col1, filter_col2 = st.columns(2)
    
    with filter_col1:
        # TikTok ID filter
        tiktok_ids = ["(Show All)"] + sorted(sub_df["tiktok_id"].dropna().unique())
        tiktok_filter = st.selectbox("Filter by TikTok ID", tiktok_ids)
    
    with filter_col2:
        # Posting Date Range filter
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
    
    # Select columns for the view table
    view_columns = [
        "id", 
        "tiktok_id", 
        "agency_name", 
        "posting_date", 
        "category_name",  # Using category_name instead of category_id for readability
        "post_type", 
        "link_post", 
        "level", 
        "status_name", 
        "reason"
    ]
    
    view_df = filtered_sub_df[view_columns].copy()
    
    # Fill NaN values for prettier display
    view_df["reason"] = view_df["reason"].fillna("—")  # Em dash for empty reasons
    view_df["level"] = view_df["level"].fillna(0).astype(int)  # Convert level to integer
    view_df["category_name"] = view_df["category_name"].fillna("Uncategorized")
    
    # Configure pretty column display
    view_column_config = {
        "id": st.column_config.NumberColumn(
            "ID",
            help="Submission ID",
            width="small",
        ),
        "tiktok_id": st.column_config.TextColumn(
            "TikTok ID",
            help="Creator's TikTok handle",
            width="medium",
        ),
        "agency_name": st.column_config.TextColumn(
            "Agency",
            help="Management agency",
            width="medium",
        ),
        "posting_date": st.column_config.DateColumn(
            "Posted On",
            help="When the content was posted",
            width="small",
        ),
        "category_name": st.column_config.TextColumn(
            "Category",
            help="Industry category (Accommodation, Dining, etc.)",
            width="medium",
        ),
        "post_type": st.column_config.TextColumn(
            "Type",
            help="Content type (video, image, etc.)",
            width="small",
        ),
        "link_post": st.column_config.LinkColumn(
            "Post Link",
            help="Click to view the post",
            width="medium",
            display_text="🔗 View Post"
        ),
        "level": st.column_config.NumberColumn(
            "Level",
            help="Creator level or tier",
            width="small",
        ),
        "status_name": st.column_config.TextColumn(
            "Status",
            help="Current submission status",
            width="medium",
        ),
        "reason": st.column_config.TextColumn(
            "Reason",
            help="Why the status was set",
            width="large",
        ),
    }
    
    # Display the pretty read-only table
    st.dataframe(
        view_df,
        column_config=view_column_config,
        use_container_width=True,
        height=400,
        hide_index=True,
    )
    
    # Show record count
    st.caption(f"📌 Showing **{len(view_df)}** submission(s)")

    # ---------- EDITABLE TABLE (IN EXPANDER) ----------
    with st.expander("✏️ Edit Submissions (Status & Reason)", expanded=False):
        st.write("**Instructions:** Edit the Status or Reason columns below, then click **Apply Changes** to save.")
        
        # Select columns for editing (same as view, but we'll make some editable)
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
        
        # Fill NaN values in 'reason' column with empty strings for editing
        edit_df["reason"] = edit_df["reason"].fillna("")
        
        # Configure column settings for the editable table
        edit_column_config = {
            "id": st.column_config.NumberColumn(
                "ID", 
                disabled=True, 
                width="small"
            ),
            "tiktok_id": st.column_config.TextColumn(
                "TikTok ID", 
                disabled=True, 
                width="medium"
            ),
            "full_name": st.column_config.TextColumn(
                "Full Name", 
                disabled=True, 
                width="medium"
            ),
            "posting_date": st.column_config.DateColumn(
                "Posting Date", 
                disabled=True, 
                width="small"
            ),
            "link_post": st.column_config.LinkColumn(
                "Post Link", 
                disabled=True, 
                width="medium",
                display_text="🔗 View"
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
        
        # Display editable table
        edited_df = st.data_editor(
            edit_df,
            column_config=edit_column_config,
            use_container_width=True,
            num_rows="fixed",
            hide_index=True,
            key="content_submissions_editor"
        )

        # Apply Changes Button
        col_button, col_spacer = st.columns([1, 3])
        
        with col_button:
            apply_button = st.button("💾 Apply Changes", type="primary", use_container_width=True)
        
        if apply_button:
            # Detect changes by comparing original and edited dataframes
            changes = []
            
            for idx in edited_df.index:
                original_status = edit_df.loc[idx, "status_name"]
                edited_status = edited_df.loc[idx, "status_name"]
                
                original_reason = edit_df.loc[idx, "reason"]
                edited_reason = edited_df.loc[idx, "reason"]
                
                # Check if status or reason changed
                if original_status != edited_status or original_reason != edited_reason:
                    # Map status_name back to status_id
                    status_id = int(status_df[status_df["status_name"] == edited_status]["id"].values[0])
                    
                    changes.append({
                        "id": int(edited_df.loc[idx, "id"]),
                        "status_id": status_id,
                        "reason": edited_reason if edited_reason.strip() else None
                    })
            
            if changes:
                try:
                    bulk_update_content_submissions(changes)
                    st.success(f"✅ Successfully updated **{len(changes)}** submission(s)!")
                    st.info("🔄 Refreshing data...")
                    
                    # Clear cache to force refresh
                    st.cache_data.clear()
                    
                    # Rerun to show updated data
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Error updating submissions: {e}")
                    st.info("💡 Try refreshing the page or check your database connection.")
            else:
                st.info("ℹ️ No changes detected. Edit the Status or Reason columns to make updates.")