import streamlit as st
import Summary as s
import pandas as pd

ss = s.Summary(mode='remote', start_date='2023')

st.line_chart(ss.annually_cals)



