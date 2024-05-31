import subprocess
command = ["pip", "install", "-r", "requirements.txt"]

file_id = '1zolNE97MtNtclIXFMvkmjzJVpgyonpKn'
json_file_path = 'cdg-mark-cust-prd_customer_team.json'
subprocess.run(['gdown', '--id', file_id, '-O', json_file_path], check=True)

import streamlit as st
import pandas as pd
import pandas_gbq
import json

# Read the JSON file
with open(json_file_path, 'r') as file:
    json_data = json.load(file)

# Title
st.title("Upload CSV into GBQ App")

# Upload CSV file
st.sidebar.header("Upload Data")
uploaded_file = st.sidebar.file_uploader("Upload a CSV file", type=["csv"])

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

