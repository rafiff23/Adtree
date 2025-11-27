import datetime as dt
import pandas as pd
import streamlit as st

from db import (
    fetch_creator_registry,
    update_creator_registry_row,
)

AGENCY_OPTIONS = [
    "Adtree Digital Indonesia",
    "Golden Maker",
    "WH Management",
    "TB Management",
    "BTC Management",
    "HM Agency",
]


def render():
    st.title("Creator List")
    st.write("View existing creators and edit specific profiles.")

    rows = fetch_creator_registry()
    if not rows:
        st.info("No creator data available.")
        return

    df = pd.DataFrame(rows)
    df = df.sort_values("id", ascending=True).reset_index(drop=True)

    # Add WhatsApp link
    df["whatsapp_link"] = "https://wa.me/" + df["phone_number"].str.replace("+", "", n=1)

    # ---------- FILTERS ----------
    st.subheader("Filters")

    tiktok_ids = ["(Show All)"] + sorted(df["tiktok_id"].unique())
    tiktok_id_filter = st.selectbox("Filter by TikTok ID", tiktok_ids)

    # Filter by Binding Status
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
            uid_new = st.text_input("UID", value=row["uid"] or "")

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
            uid_clean = uid_new or ""
            if uid_clean and not uid_clean.isdigit():
                st.error("UID must contain numbers only when filled.")
                return

            if phone_new.startswith("+") or phone_new.startswith("62"):
                st.error("Do NOT include +62 or 62. Only the remaining number.")
                return

            if not phone_new.isdigit():
                st.error("Phone number must contain digits only.")
                return

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
            updated_uid = uid_clean if uid_clean else None
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
