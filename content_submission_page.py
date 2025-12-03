# content_submission_page.py

import streamlit as st
import pandas as pd
from datetime import date

from db import get_connection  # your existing function


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
def render_content_submission_page():
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
