@echo off
setlocal
cd /d "C:\Users\Administrator.DESKTOP-00BT8F3\.openclaw\workspace\polymatrix"
streamlit run dashboard.py --server.headless true --browser.gatherUsageStats false
