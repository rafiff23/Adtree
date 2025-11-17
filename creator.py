import datetime as dt

import pandas as pd
import streamlit as st

from db import insert_creator_registry_row, fetch_creator_registry, update_creator_registry_row


# ----------------- PAGE CONFIG -----------------
st.set_page_config(page_title="Creator Registry Test", layout="wide")


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
    ["Creator Registry", "Creator List"],
)


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

            # All mandatory except followers & notes
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

            # Phone: must NOT contain +62 or 62 at beginning
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

    # Fetch all rows
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

    filtered_df = df.copy()
    if tiktok_id_filter != "(Show All)":
        filtered_df = df[df["tiktok_id"] == tiktok_id_filter]

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

        # Get row for that TikTok ID
        row = df[df["tiktok_id"] == edit_tiktok_id].iloc[0]
        row_id = int(row["id"])

        st.write(f"Editing creator with ID: **{row_id}**")

        # Editable form
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

            # Followers default (handle NaN / None)
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

            # Phone
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

            # Onboarding date default (handle NaN / None)
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
            # ---------- VALIDATION ----------
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

            # Detect changed fields
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
