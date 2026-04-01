@echo off
setlocal
cd /d %~dp0
set POLY_SIGNER_SERVER_URL=http://100.91.159.54:8420
set SIGNER_TOKEN=Qz8yoj4Obb4nWYHj888fLgrFhIcDYVfU0nCWjv6xizY
streamlit run dashboard/app.py
