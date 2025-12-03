# submission.py

import streamlit as st

from content_submission_page import render_content_submission_page
from creator_registry_page import render_creator_registry_page

# ===========================
# PAGE CONFIG
# ===========================
st.set_page_config(
    page_title="Content Submission & Creator Registry",
    layout="wide"
)

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
# ROUTING
# ===========================
if page == "Content Submission Form":
    render_content_submission_page()
elif page == "Creator Registry":
    render_creator_registry_page(AGENCY_OPTIONS)
