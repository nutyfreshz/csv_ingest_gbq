import subprocess
command = ["pip", "install", "-r", "requirements.txt"]

import streamlit as st
import pandas as pd
import pandas_gbq

# Title
st.title("Upload CSV into GBQ App")

# Upload CSV file
st.sidebar.header("Upload Data")
uploaded_file = st.sidebar.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file is not None:
    @st.cache
    def load_data():
        data = pd.read_csv(uploaded_file)
        return data

    data = load_data()
    st.sidebar.markdown("### Data Sample")
    st.sidebar.write(data.head())
else:
    st.warning("Please upload a CSV file.")
