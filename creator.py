import streamlit as st
from creator_list import render as creator_list_page
from content_submission import render as content_submission_page
from content_qc import render as content_qc_page
from leaderboard import render as leaderboard_page
from leaderboard_page import render as leaderboard

st.set_page_config(page_title="Adtree Dashboard", layout="wide")

page = st.sidebar.radio(
    "Navigation",
    ["Creator List", "Content Submissions", "Content QC","Leaderboard Import"],
)

if page == "Creator List":
    creator_list_page()

elif page == "Content Submissions":
    content_submission_page()

elif page == "Content QC":
    content_qc_page()

elif page == "Leaderboard Import":
    leaderboard_page()

elif page == "Leaderboard":
    leaderboard()