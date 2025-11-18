# submission.py

import streamlit as st
import pandas as pd
from datetime import date
from db import get_connection


@st.cache_resource
def get_conn():
    return get_connection()


@st.cache_data
def load_creator_registry():
    conn = get_conn()

    try:
        conn.rollback()
    except:
        pass

    with conn.cursor() as cur:
        cur.execute("""
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
        """)
        rows = cur.fetchall()

    return pd.DataFrame(rows)


@st.cache_data
def load_category_map():
    conn = get_conn()

    try:
        conn.rollback()
    except:
        pass

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, category_name
            FROM public.category_map
            WHERE id != 4
            ORDER BY id;
        """)
        rows = cur.fetchall()

    return pd.DataFrame(rows)


def insert_submission(data: dict):
    conn = get_conn()

    try:
        with conn.cursor() as cur:
            cur.execute("""
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
                    notes
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
                    %(notes)s
                );
            """, data)

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise e


def main():

    st.title("Creator Content Submission Form")

    creators_df = load_creator_registry()
    category_df = load_category_map()

    # ===========================
    # FORM START
    # ===========================
    with st.form("submission_form"):

        # Submission date
        submission_date = st.date_input(
            "Submission Date",
            value=date.today(),
            disabled=True
        )

        # Creator Dropdown
        creator_choice = st.selectbox(
            "Select Creator:",
            options=creators_df.index,
            format_func=lambda i: (
                f"{creators_df.loc[i, 'tiktok_id']} "
                f"({creators_df.loc[i, 'full_name']}) – "
                f"{creators_df.loc[i, 'agency_name']}"
            )
        )

        selected = creators_df.loc[creator_choice]
        creator_id = int(selected["id"])
        raw_agency = selected["agency_id"]
        # Handle NULL agency_id safely
        if pd.isna(raw_agency) or raw_agency is None:
            management_id = None   # or 0 if your DB requires integer
        else:
            management_id = int(raw_agency)


        st.write(f"**Full Name:** {selected['full_name']}")
        st.write(f"**Agency:** {selected['agency_name']}")

        # Posting date
        posting_date = st.date_input("Posting Date", value= date.today(), disabled=True)

        # Category
        category_choice = st.selectbox(
            "Category:",
            options=category_df.index,
            format_func=lambda i: category_df.loc[i, "category_name"]
        )
        category_id = int(category_df.loc[category_choice, "id"])

        # Post type
        post_type = st.selectbox(
            "Jenis Postingan:",
            [
                "Foto Slide Normal Posting",
                "Video Normal Posting"
            ]
        )

        # Link field
        link_post = st.text_input("TikTok Post Link:")

        if link_post and "tiktok" not in link_post.lower():
            st.warning("⚠️ TikTok link must contain 'tiktok'")

        # Hidden fields
        status_id = 1
        notes = ""
        level = None

        # ===========================
        # SUBMIT BUTTON **INSIDE FORM**
        # ===========================
        submitted = st.form_submit_button("Save")

    # ===========================
    # AFTER FORM SUBMISSION
    # ===========================
    if submitted:
        if "tiktok" not in link_post.lower():
            st.error("❌ Invalid TikTok link. Must contain 'tiktok'")

        else:
            payload = {
                "submission_date": submission_date,
                "creator_id": creator_id,
                "management_id": management_id,
                "posting_date": posting_date,
                "category_id": category_id,
                "post_type": post_type,
                "link_post": link_post,
                "level": level,
                "status_id": status_id,
                "notes": notes,
            }

            insert_submission(payload)
            st.success("✅ Submission saved successfully!")


if __name__ == "__main__":
    main()
