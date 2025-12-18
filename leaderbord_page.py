import pandas as pd
import streamlit as st
from db import get_connection

TABLE_FULL = "leaderboard.creator_dec_leaderboard_all_level"

st.title("DEBUG: Raw DB Table Preview")

conn = get_connection()

query = f"""
SELECT *
FROM {TABLE_FULL}
LIMIT 50;
"""

df = pd.read_sql_query(query, conn)
conn.close()

st.write("Row count:", len(df))
st.write("Columns:", list(df.columns))
st.dataframe(df, use_container_width=True)
