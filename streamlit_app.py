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


##Part 2 ingest df into gbq
uploaded_file_json = 'cdg-mark-cust-prd_customer_team(1).json'

credentials = service_account.Credentials.from_service_account_file(uploaded_file_json)

project_id = 'cdg-mark-cust-prd'
table_id = 'TEMP_NUTCHAPONG.kd_temp_store_trans_analysis_nakhon_sawan_010624'

# Upload DataFrame to BigQuery
pandas_gbq.to_gbq(df_clean_fin
                  , table_id
                  , project_id=project_id
                  , if_exists='replace'
                  , credentials=credentials
                 )





