import streamlit as st
from creator_list import render as creator_list_page
from content_submission import render as content_submission_page

st.set_page_config(page_title="Adtree Dashboard", layout="wide")

page = st.sidebar.radio(
    "Navigation",
    ["Creator List", "Content Submissions"],
)

if page == "Creator List":
    creator_list_page()

elif page == "Content Submissions":
    content_submission_page()
