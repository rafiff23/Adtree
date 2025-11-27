# submission.py

import streamlit as st
import pandas as pd
import datetime as dt
from datetime import date
from db import (
    get_connection,
    insert_creator_registry_row,
)


# ===========================
# PAGE CONFIG
# ===========================
st.set_page_config(page_title="Content Submission & Creator Registry", layout="wide")


# ===========================
# CONSTANTS
# ===========================
AGENCY_OPTIONS = [
    "Adtree Digital Indonesia",
    "Golden Maker",
    "WH Management",
    "TB Management",
    "BTC Management",
    "HM Agency",
]


# ===========================
# SIDEBAR NAVIGATION
# ===========================
page = st.sidebar.radio(
    "Navigation",
    ["Content Submission Form", "Creator Registry"],
)


# ===========================
# DB CONNECTION WRAPPER
# ===========================
@st.cache_resource
def get_conn():
    """
    Wrap db.get_connection() with Streamlit cache.
    db.get_connection() is your single source of truth
    for host/port/user/password.
    """
    return get_connection()


# ===========================
# DATA LOADERS
# ===========================
@st.cache_data(ttl=60)
def load_creator_registry() -> pd.DataFrame:
    """
    Load creator list used in the dropdown.
    """
    conn = get_conn()

    # Safely rollback any open transaction
    try:
        conn.rollback()
    except Exception:
        pass

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 
                cr.id,
                cr.tiktok_id,
                cr.full_name,
                cr.agency_id,
                am.agency_name
            FROM public.creator_registry cr
            LEFT JOIN public.agency_map am
                ON cr.agency_id = am.id
            ORDER BY cr.tiktok_id;
            """
        )
        rows = cur.fetchall()

    df = pd.DataFrame(
        rows,
        columns=["id", "tiktok_id", "full_name", "agency_id", "agency_name"],
    )
    return df


@st.cache_data
def load_category_map() -> pd.DataFrame:
    """
    Load category map (excluding id=4).
    """
    conn = get_conn()

    try:
        conn.rollback()
    except Exception:
        pass

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, category_name
            FROM public.category_map
            WHERE id != 4
            ORDER BY id;
            """
        )
        rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=["id", "category_name"])
    return df


def is_link_already_submitted(link_post: str) -> bool:
    """
    Return True if the given TikTok link already exists in content_submissions.
    """
    if not link_post:
        return False

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM public.content_submissions
            WHERE TRIM(link_post) = TRIM(%s)
            LIMIT 1;
            """,
            (link_post,),
        )
        return cur.fetchone() is not None


# ===========================
# INSERT HELPER FOR SUBMISSIONS
# ===========================
def insert_submission(data: dict):
    """
    Insert 1 row into public.content_submissions.
    Assumes 'reason' column exists and is nullable.
    """
    conn = get_conn()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.content_submissions (
                    submission_date,
                    creator_id,
                    management_id,
                    posting_date,
                    category_id,
                    post_type,
                    link_post,
                    level,
                    status_id,
                    notes,
                    reason
                )
                VALUES (
                    %(submission_date)s,
                    %(creator_id)s,
                    %(management_id)s,
                    %(posting_date)s,
                    %(category_id)s,
                    %(post_type)s,
                    %(link_post)s,
                    %(level)s,
                    %(status_id)s,
                    %(notes)s,
                    %(reason)s
                );
                """,
                data,
            )

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise e


# =================================================
# PAGE 1: CONTENT SUBMISSION FORM
# =================================================
if page == "Content Submission Form":
    st.title("Creator Content Submission Form V3")

    creators_df = load_creator_registry()
    category_df = load_category_map()

    if creators_df.empty:
        st.error("No creators found in creator_registry. Please add creators first.")
        st.stop()

    if category_df.empty:
        st.error("No categories found in category_map. Please configure categories first.")
        st.stop()

    # ===========================
    # FORM START
    # ===========================
    with st.form("submission_form"):

        # Submission date (auto, locked)
        submission_date = st.date_input(
            "Submission Date",
            value=date.today(),
            disabled=True,
        )

        # Creator Dropdown
        creator_choice = st.selectbox(
            "Select Creator:",
            options=creators_df.index,
            format_func=lambda i: (
                f"{creators_df.loc[i, 'tiktok_id']} "
                f"({creators_df.loc[i, 'full_name']}) – "
                f"{creators_df.loc[i, 'agency_name']}"
            ),
        )

        selected = creators_df.loc[creator_choice]
        creator_id = int(selected["id"])
        raw_agency = selected["agency_id"]

        # Handle NULL agency_id safely
        if pd.isna(raw_agency) or raw_agency is None:
            management_id = None   # set to None; DB column should allow NULL
        else:
            management_id = int(raw_agency)

        st.write(f"**Full Name:** {selected['full_name']}")
        st.write(f"**Agency:** {selected['agency_name']}")

        # Posting date (auto, locked)
        posting_date = st.date_input(
            "Posting Date",
            value=date.today(),
            disabled=True,
        )

        # Category dropdown
        category_choice = st.selectbox(
            "Category:",
            options=category_df.index,
            format_func=lambda i: category_df.loc[i, "category_name"],
        )
        category_id = int(category_df.loc[category_choice, "id"])

        # Post type
        post_type = st.selectbox(
            "Jenis Postingan:",
            [
                "Foto Slide Normal Posting",
                "Video Normal Posting",
            ],
        )

        # ===========================
        # TikTok Link with Live Validation
        # ===========================
        link_post = st.text_input(
            "TikTok Post Link:",
            placeholder="https://vt.tiktok.com/xxxxxxx/",
            help="Paste the full TikTok link. Must NOT contain @ or spaces."
        )

        # Live validation warnings (shown while typing, INSIDE the form)
        if link_post:
            clean_link = link_post.strip()
            
            # Check if link contains 'tiktok'
            if "tiktok" not in clean_link.lower():
                st.warning("⚠️ TikTok link must contain 'tiktok'")
            
            # Check for @ symbol in the link
            if "@" in clean_link:
                st.warning("⚠️ TikTok link must NOT contain the @ symbol. Please remove it.")
            
            # Check for spaces in the link
            if " " in clean_link:
                st.warning("⚠️ TikTok link must NOT contain spaces. Please remove them.")
        # ===========================

        # Hidden / default fields
        status_id = None
        notes = None
        reason = None
        level = None

        # Submit button (MUST BE INSIDE THE FORM)
        submitted = st.form_submit_button("Save")

    # ===========================
    # AFTER FORM SUBMISSION
    # ===========================
    if submitted:
        clean_link = (link_post or "").strip()

        # ============ VALIDATION CHAIN ============
        
        # 1. Empty link check
        if not clean_link:
            st.error("❌ TikTok link is required.")
        
        # 2. Must contain 'tiktok'
        elif "tiktok" not in clean_link.lower():
            st.error("❌ Invalid TikTok link. Must contain 'tiktok'")
        
        # 3. Must NOT contain @ symbol
        elif "@" in clean_link:
            st.error("❌ Invalid TikTok link. Link must NOT contain the @ symbol. Please remove it.")
        
        # 4. Must NOT contain spaces
        elif " " in clean_link:
            st.error("❌ Invalid TikTok link. Link must NOT contain spaces. Please remove them.")
        
        # 5. Duplicate check
        elif is_link_already_submitted(clean_link):
            st.error("❌ This TikTok link is duplicate and has already been submitted.")
        
        # 6. All validations passed → insert
        else:
            payload = {
                "submission_date": submission_date,
                "creator_id": creator_id,
                "management_id": management_id,
                "posting_date": posting_date,
                "category_id": category_id,
                "post_type": post_type,
                "link_post": clean_link,
                "level": level,
                "status_id": status_id,
                "notes": notes,
                "reason": reason,
            }

            try:
                insert_submission(payload)
                st.success("✅ Submission saved successfully!")
                
                # Clear cache to refresh data
                st.cache_data.clear()
                
            except Exception as e:
                st.error(f"❌ Error saving submission: {e}")


# =================================================
# PAGE 2: CREATOR REGISTRY
# =================================================
elif page == "Creator Registry":
    st.title("Creator Registry")
    st.write("Register new creators into the system.")

    today = dt.date.today()
    month_label = today.strftime("%Y-%m")
    binding_status = "Unbound"
    onboarding_date = None

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
                
                st.success(f"Creator saved successfully with ID {new_id} ✅")
                st.info(f"TikTok Link: {tiktok_link}")
                st.info(f"Phone stored as: {phone_number}")
                
                # Clear cache so new creator shows up in dropdown immediately
                st.cache_data.clear()
                
            except Exception as e:
                st.error(f"Error saving creator: {e}")