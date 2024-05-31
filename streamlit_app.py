import subprocess
command = ["pip", "install", "-r", "requirements.txt"]

import streamlit as st
import pandas as pd
import pandas_gbq
import json

# Title
st.title("Upload CSV into GBQ App")

# Upload CSV file
st.sidebar.header("Upload Data")
uploaded_file = st.sidebar.file_uploader("Upload a CSV file", type=["csv"])

st.sidebar.header("Upload JSON Credential")
uploaded_file_json = st.sidebar.file_uploader("Upload a JSON file", type=["json"])

if uploaded_file is not None:
    @st.cache_data
    def load_data():
        data = pd.read_csv(uploaded_file)
        return data

    data = load_data()
    
    # Display Data Sample in the main screen
    st.markdown("### Data Sample")
    st.write(data.head())
else:
    st.warning("Please upload a CSV file.")

