# submission.py

import streamlit as st
import pandas as pd
from datetime import date
from db import get_connection   # reuse your existing DB connecto

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

    We use:
    - id         -> creator_id (for DB)
    - tiktok_id  -> shown in dropdown
    - full_name  -> shown in dropdown label
    """
    conn = get_conn()
    query = """
        SELECT id, tiktok_id, full_name
        FROM creator_registry
        ORDER BY tiktok_id;
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
    If not found, returns None (so status_id will be NULL).
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

        - **Creator** options come from `creator_registry`
        - **Management** from `agency_map`
        - **Category** from `category_map`
        - Link must be a TikTok URL (must contain `tiktok`)
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

        # 2. Creator dropdown (tiktok_id + full_name), value = creator_id
        # Build label -> id mapping, e.g. "@id - Full Name" : id
        creators_df["label"] = creators_df.apply(
            lambda row: f"{row['tiktok_id']} - {row['full_name']}"
            if pd.notnull(row.get("full_name"))
            else str(row["tiktok_id"]),
            axis=1,
        )

        creator_labels = creators_df["label"].tolist()
        selected_creator_label = st.selectbox("Creator (TikTok ID)", creator_labels)

        selected_creator_row = creators_df.loc[
            creators_df["label"] == selected_creator_label
        ].iloc[0]
        creator_id = int(selected_creator_row["id"])
        full_name = selected_creator_row.get("full_name")
        tiktok_id = selected_creator_row.get("tiktok_id")

        # 3. Nama Lengkap (auto), read-only
        st.text_input(
            "Nama Lengkap (auto)",
            value=full_name if pd.notnull(full_name) else "",
            disabled=True,
        )

        # 4. Management Name dropdown (from agency_map)
        mgmt_df["label"] = mgmt_df["agency_name"]
        mgmt_labels = mgmt_df["label"].tolist()
        selected_mgmt_label = st.selectbox("Management Name", mgmt_labels)

        selected_mgmt_row = mgmt_df.loc[
            mgmt_df["label"] == selected_mgmt_label
        ].iloc[0]
        management_id = int(selected_mgmt_row["id"])

        # 5. Posting Date
        posting_date = st.date_input("Posting Date")

        # 6. Category dropdown (from category_map)
        cat_df["label"] = cat_df["category_name"]
        cat_labels = cat_df["label"].tolist()
        selected_cat_label = st.selectbox("Category", cat_labels)

        selected_cat_row = cat_df.loc[
            cat_df["label"] == selected_cat_label
        ].iloc[0]
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
        level = None               # can be filled by rule later
        status_id = default_status_id  # default "normal quality" if exists
        notes = None

        submitted = st.form_submit_button("Submit")

    # ---- Submit handler ----
    if submitted:
        errors = []

        # Basic validations
        if not link_post.strip():
            errors.append("Link Post cannot be empty.")

        if "tiktok" not in link_post.lower():
            errors.append("Link Post must be a TikTok URL (must contain 'tiktok').")

        if submission_date is None:
            errors.append("Submission Date is required.")

        if posting_date is None:
            errors.append("Posting Date is required.")

        if errors:
            for e in errors:
                st.error(e)
            return

        # Insert into DB
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
                f"Saved for creator: {tiktok_id} - {full_name} | "
                f"Management: {selected_mgmt_label} | "
                f"Category: {selected_cat_label}"
            )


if __name__ == "__main__":
    main()
