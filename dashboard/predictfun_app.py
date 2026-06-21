from __future__ import annotations

import streamlit as st

try:
    from dashboard.predictfun_view import render_predictfun_dashboard
except ModuleNotFoundError:
    from predictfun_view import render_predictfun_dashboard


st.set_page_config(
    page_title="Predict.fun Maker",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
<style>
.stApp { background-color: #0d1117 !important; }
</style>
""",
    unsafe_allow_html=True,
)

render_predictfun_dashboard(embedded=False)
