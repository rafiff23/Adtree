# submission.py

import streamlit as st
import pandas as pd
from datetime import date
from db import get_connection   # we reuse your existing DB connector

# -----------------------------
# DB HELPERS (CACHED)
# -----------------------------

@st.cache_resource
def get_conn():
    """
    Keep a single DB connection alive for the app.
    """
    return get_connection()

@st.cache_data
def load_creators():
    """
    Load creators from creator_registry.
    We expect columns: id, username, full_name (or adjust if your column name differs).
    """
    conn = get_conn()
    query = """
        SELECT id, username, full_name
        FROM creator_registry
        ORDER BY username;
    """
    df = pd.read_sql(query, conn)
    return df

@st.cache_data
def load_managements():
    """
    Load management/agency options from agency_map.
    """
    conn = get_conn()
    query = """
        SELECT id, agency_name
        FROM agency_map
        ORDER BY agency_name;
    """
    df = pd.read_sql(query, conn)
    return df

@st.cache_data
def load_categories():
    """
    Load categories from category_map.
    """
    conn = get_conn()
    query = """
        SELECT id, category_name
        FROM category_map
        ORDER BY category_name;
    """
    df = pd.read_sql(query, conn)
    return df

@st.cache_data
def get_default_status_id():
    """
    Get the ID for 'normal quality' from status_map.
    If not found, returns None (we can still insert NULL).
    """
    conn = get_conn()
    query = """
        SELECT id
        FROM status_map
        WHERE status_name = 'normal quality'
        LIMIT 1;
    """
    df = pd.read_sql(query, conn)
    if df.empty:
        return None
    return int(df.iloc[0]["id"])


# -----------------------------
# INSERT HELPER
# -----------------------------

def insert_submission(
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
):
    """
    Insert a new row into content_submissions.
    """
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO content_submissions (
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
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
            ),
        )
    conn.commit()


# -----------------------------
# STREAMLIT UI
# -----------------------------

def main():
    st.title("Content Submission Form")

    st.markdown(
        """
        Use this form to submit new TikTok posts.
        All dropdown values are controlled from the database to keep data clean.
        """
    )

    # Load reference data
    creators_df = load_creators()
    mgmt_df = load_managements()
    cat_df = load_categories()
    default_status_id = get_default_status_id()

    if creators_df.empty:
        st.error("No creators found in creator_registry. Please add creators first.")
        return

    if mgmt_df.empty:
        st.error("No management data found in agency_map. Please seed agency_map.")
        return

    if cat_df.empty:
        st.error("No categories found in category_map. Please seed category_map.")
        return

    with st.form("submission_form"):
        # 1. Submission Date (default today)
        submission_date = st.date_input("Submission Date", value=date.today())

        # 2. Username dropdown (from creator_registry)
        # Build label -> id mapping, e.g. "username - full_name" : id
        creators_df["label"] = creators_df.apply(
            lambda row: f"{row['username']} - {row['full_name']}"
            if pd.notnull(row.get("full_name"))
            else str(row["username"]),
            axis=1,
        )
        username_options = creators_df["label"].tolist()
        selected_label = st.selectbox("Username", username_options)

        # Get selected creator_id and full_name
        selected_creator_row = creators_df.loc[creators_df["label"] == selected_label].iloc[0]
        creator_id = int(selected_creator_row["id"])
        full_name = selected_creator_row.get("full_name")

        # 3. Nama Lengkap (auto from username, read-only)
        st.text_input(
            "Nama Lengkap (auto)",
            value=full_name if pd.notnull(full_name) else "",
            disabled=True,
        )

        # 4. Management Name dropdown (from agency_map)
        mgmt_df["label"] = mgmt_df["agency_name"]
        mgmt_options = mgmt_df["label"].tolist()
        selected_mgmt_label = st.selectbox("Management Name", mgmt_options)
        selected_mgmt_row = mgmt_df.loc[mgmt_df["label"] == selected_mgmt_label].iloc[0]
        management_id = int(selected_mgmt_row["id"])

        # 5. Posting Date
        posting_date = st.date_input("Posting Date")

        # 6. Category dropdown (from category_map)
        cat_df["label"] = cat_df["category_name"]
        cat_options = cat_df["label"].tolist()
        selected_cat_label = st.selectbox("Category", cat_options)
        selected_cat_row = cat_df.loc[cat_df["label"] == selected_cat_label].iloc[0]
        category_id = int(selected_cat_row["id"])

        # 7. Jenis Postingan
        post_type = st.selectbox(
            "Jenis Postingan",
            [
                "Foto Slide Normal Posting",
                "Video Normal Posting",
            ],
        )

        # 8. Link Post (must contain 'tiktok')
        link_post = st.text_input("Link Post (must be a TikTok URL)")

        # Hidden / backend fields
        # For now we leave level and notes as None, and status = 'normal quality' if exists
        level = None
        status_id = default_status_id
        notes = None

        submitted = st.form_submit_button("Submit")

    # ---- Submission handling ----
    if submitted:
        # Basic validation
        errors = []

        if "tiktok" not in link_post.lower():
            errors.append("Link Post must be a TikTok URL (must contain 'tiktok').")

        if not link_post.strip():
            errors.append("Link Post cannot be empty.")

        if submission_date is None:
            errors.append("Submission Date is required.")

        if posting_date is None:
            errors.append("Posting Date is required.")

        if errors:
            for e in errors:
                st.error(e)
            return

        # Attempt insert
        try:
            insert_submission(
                submission_date=submission_date,
                creator_id=creator_id,
                management_id=management_id,
                posting_date=posting_date,
                category_id=category_id,
                post_type=post_type,
                link_post=link_post.strip(),
                level=level,
                status_id=status_id,
                notes=notes,
            )
        except Exception as e:
            st.error(f"Failed to save submission: {e}")
        else:
            st.success("Submission saved successfully.")
            st.info(
                f"Saved for creator: {selected_label} | Management: {selected_mgmt_label} | Category: {selected_cat_label}"
            )


if __name__ == "__main__":
    main()
