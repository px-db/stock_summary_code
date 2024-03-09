import streamlit as st
import Summary as s
import pandas as pd
import streamlit_pandas as sp

ss = s.Summary(mode='remote', start_date='2023')

ss.dfs_days
st.line_chart(ss.dfs_days['20231229']['Close'])



