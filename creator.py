import datetime as dt
from datetime import date
import pandas as pd
import streamlit as st

from db import (
    insert_creator_registry_row,
    fetch_creator_registry,
    update_creator_registry_row,
    get_connection,
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
    st.write("View and edit content submissions linked to creators.")

    submissions_rows = fetch_content_submissions()
    if not submissions_rows:
        st.info("No content submissions available.")
        st.stop()

    sub_df = pd.DataFrame(submissions_rows)
    # Convert submission_date column to datetime objects for filtering
    sub_df["submission_date"] = pd.to_datetime(sub_df["submission_date"]).dt.normalize()

    # ---------- FILTERS ----------
    st.subheader("Filters")

    # Filter by TikTok ID
    tiktok_ids = ["(Show All)"] + sorted(sub_df["tiktok_id"].dropna().unique())
    tiktok_filter = st.selectbox("Filter by TikTok ID", tiktok_ids)

    # Filter by Status
    status_names = ["(Show All)"] + sorted(sub_df["status_name"].dropna().unique())
    status_filter = st.selectbox("Filter by Status", status_names)

    # --- DATE RANGE FILTER (UPDATED) ---
    st.write("Filter by Submission Date Range:")

    # Determine min/max for the date pickers
    # Min value allowed: Earliest date in DB
    min_db_date = sub_df["submission_date"].min().date() if not sub_df.empty else date.today()
    
    # Max value allowed: Today
    today_date = date.today()

    # Create two columns for side-by-side UI
    d_col1, d_col2 = st.columns(2)

    with d_col1:
        start_date = st.date_input(
            "Start Date",
            value=min_db_date,      # Default: Oldest date in data
            min_value=min_db_date,
            max_value=today_date
        )

    with d_col2:
        end_date = st.date_input(
            "End Date",
            value=today_date,       # Default: Today
            min_value=min_db_date,
            max_value=today_date
        )
    # -----------------------------------

    filtered_sub_df = sub_df.copy()

    if tiktok_filter != "(Show All)":
        filtered_sub_df = filtered_sub_df[filtered_sub_df["tiktok_id"] == tiktok_filter]

    if status_filter != "(Show All)":
        filtered_sub_df = filtered_sub_df[filtered_sub_df["status_name"] == status_filter]

    # --- APPLY DATE RANGE LOGIC (UPDATED) ---
    # Filter rows where date is >= start_date AND <= end_date
    if not filtered_sub_df.empty:
        # Ensure we compare date objects to date objects
        mask = (filtered_sub_df["submission_date"].dt.date >= start_date) & \
               (filtered_sub_df["submission_date"].dt.date <= end_date)
        filtered_sub_df = filtered_sub_df[mask]
    # ----------------------------------------

    st.subheader("Content Submissions (View Only)")
    st.dataframe(
        filtered_sub_df,
        use_container_width=True,
        height=400,
    )
    # END NEW FILTER

    # ---------- EDIT SPECIFIC SUBMISSION ----------
    with st.expander("Edit Submission", expanded=False):
        st.write("Select a submission to edit (by ID).")

        submission_ids = sorted(sub_df["id"].unique())
        selected_sub_id = st.selectbox("Submission ID", submission_ids)

        sub_row = sub_df[sub_df["id"] == selected_sub_id].iloc[0]

        st.write(f"Editing Submission **ID {selected_sub_id}**")
        st.write(f"Creator: **{sub_row['tiktok_id']} ({sub_row['full_name']})**")
        st.write(f"Agency: **{sub_row['agency_name']}**")
        st.write(f"Link: {sub_row['link_post']}")

        status_df = fetch_status_map()

        # Determine default index for status dropdown
        if not status_df.empty and sub_row["status_id"] in status_df["id"].values:
            default_status_index = status_df.index[status_df["id"] == sub_row["status_id"]][0]
        else:
            default_status_index = 0

        with st.form("edit_submission_form"):

            status_choice = st.selectbox(
                "Status",
                options=status_df.index,
                format_func=lambda i: status_df.loc[i, "status_name"],
                index=default_status_index,
            )
            new_status_id = int(status_df.loc[status_choice, "id"])

            # Level (optional integer)
            level_raw = sub_row["level"]
            if level_raw is None or pd.isna(level_raw):
                level_default = 0
            else:
                try:
                    level_default = int(level_raw)
                except Exception:
                    level_default = 0

            new_level = st.number_input(
                "Level (optional)",
                min_value=0,
                step=1,
                value=level_default,
            )

            new_notes = st.text_area(
                "Notes",
                value=sub_row["notes"] if sub_row["notes"] else "",
            )

            submit_sub_edit = st.form_submit_button("Apply Changes")

        if submit_sub_edit:
            updated_sub_fields = {}

            if sub_row["status_id"] != new_status_id:
                updated_sub_fields["status_id"] = new_status_id

            # store None if 0
            level_value = None if new_level == 0 else new_level
            if sub_row["level"] != level_value:
                updated_sub_fields["level"] = level_value

            new_notes_clean = new_notes if new_notes.strip() else None
            if sub_row["notes"] != new_notes_clean:
                updated_sub_fields["notes"] = new_notes_clean

            if updated_sub_fields:
                try:
                    update_content_submission_row(selected_sub_id, updated_sub_fields)
                    st.success("Submission updated successfully! ✅")
                    st.info("Refresh the page to see updated values in the table above.")
                except Exception as e:
                    st.error(f"Error updating submission: {e}")
            else:
                st.info("No changes detected.")