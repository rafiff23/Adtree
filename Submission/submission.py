import datetime as dt

import pandas as pd
import streamlit as st

# ---------- PAGE CONFIG ----------
st.set_page_config(page_title="TikTok Go - Simple Input Page", layout="centered")

st.title("TikTok Go - Content Input Form")

st.write("Fill this form to log a new TikTok Go post.")


# ---------- SESSION STATE SETUP ----------
# This keeps data in memory while the app is running
if "records" not in st.session_state:
    st.session_state["records"] = []  # list of dicts


# ---------- FORM INPUTS ----------
# 1) Today's date (auto)
today = dt.date.today()
st.caption(f"Today: **{today}**")  # just to show it on screen

# 2) Name dropdown (you can replace with your real creator list)
name_options = [
    "Select a name...",
    "Creator A",
    "Creator B",
    "Creator C",
]
selected_name = st.selectbox("Name", name_options)

# 3) Posting date picker
posting_date = st.date_input(
    "Posting Date",
    value=today,
    help="Choose the date when the content will be posted.",
)

# 4) URL input with validation rules
url = st.text_input(
    "Post URL",
    placeholder="https://vt.tiktok.com/xxxxxx",
    help="Must contain 'http' and 'vt.tiktok'.",
)

st.write("---")

# ---------- VALIDATION FUNCTION ----------
def validate_inputs(name: str, posting_date_value: dt.date, url_value: str) -> tuple[bool, str]:
    """
    Returns (is_valid, error_message).
    If valid -> (True, "").
    If invalid -> (False, "explanation").
    """

    # Name validation
    if name == "Select a name...":
        return False, "Please select a valid name from the dropdown."

    # Posting date validation (optional, you can add more rules later)
    if posting_date_value is None:
        return False, "Please choose a posting date."

    # URL validation
    if not url_value:
        return False, "URL cannot be empty."

    if "http" not in url_value:
        return False, "URL must contain 'http' or 'https'."

    if "vt.tiktok" not in url_value:
        return False, "URL must contain 'vt.tiktok'."

    # Passed all checks
    return True, ""


# ---------- SUBMIT BUTTON ----------
if st.button("Save Record"):
    is_valid, error_msg = validate_inputs(selected_name, posting_date, url)

    if not is_valid:
        st.error(error_msg)
    else:
        # Build one row (record) with 4 columns:
        # - input_date (today)
        # - name
        # - posting_date
        # - url
        record = {
            "Input Date (Today)": today,
            "Name": selected_name,
            "Posting Date": posting_date,
            "URL": url,
        }

        # Append to session_state
        st.session_state["records"].append(record)

        st.success("Record saved successfully ✅")


# ---------- SHOW SAVED DATA ----------
st.write("### Saved Records")

if st.session_state["records"]:
    df = pd.DataFrame(st.session_state["records"])
    st.dataframe(df, use_container_width=True)
else:
    st.info("No records saved yet. Fill the form and click **Save Record**.")
