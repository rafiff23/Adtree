# creator_registry_page.py

import streamlit as st
import datetime as dt

from db import insert_creator_registry_row  # your existing helper


# =================================================
# PAGE 2: CREATOR REGISTRY
# =================================================
def render_creator_registry_page(AGENCY_OPTIONS):
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

        # -------- Level dropdown (0–4, show "Level X" in UI) --------
        level_options = [0, 1, 2, 3, 4]
        level = st.selectbox(
            "Level",
            options=level_options,
            format_func=lambda x: f"Level {x}",
            index=0,
            help="Creator level classification. Stored as 0–4 in the database.",
        )

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
                    level=int(level),      # 👈 NEW: send 0/1/2/3/4 to DB
                )
                
                st.success(f"Creator saved successfully with ID {new_id} ✅")
                st.info(f"TikTok Link: {tiktok_link}")
                st.info(f"Phone stored as: {phone_number}")
                st.info(f"Level stored as: Level {level}")
                
                # Clear cache so new creator shows up in dropdown immediately
                st.cache_data.clear()
                
            except Exception as e:
                st.error(f"Error saving creator: {e}")
