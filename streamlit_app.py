import subprocess
command = ["pip", "install", "-r", "requirements.txt"]

import streamlit as st
import pandas as pd
import pandas_gbq
import json
from google.oauth2 import service_account
import time
from datetime import date
import numpy as np

# Title
st.title("Upload CSV into GBQ WebApp")

# Caution
st.markdown(
    """
    <h1>#Caution!!</h1>
    <p>Number of columns and sequences in CSV file need to matched with table_id in GBQ.</p>
    <p>PS. group_name & commu_type[EDM,SMS,LINE] & target columns should be exists in CSV file.</p>
    """,
    unsafe_allow_html=True
)

# Instruction
st.markdown(
    """
    <h1>#Instruction</h1>
    <p>1. Browse JSON CredentiaL file from moderator in Part 1) section.</p>
    <p>2. Browse CSV file which you want to ingest in Part 2) section.</p>
    <p>3. Select function on table if add-up data > append or create new or replace table > replace in Part 3) section.</p>
    <p>4. Type table_id which came from Moderator in Part 3) section.</p>
    """,
    unsafe_allow_html=True
)


# Upload JSON credential file
st.sidebar.header("Part 1) Upload JSON Credential")
uploaded_file_json = st.sidebar.file_uploader("Upload a JSON file", type=["json"])

# Upload CSV file
st.sidebar.header("Part 2) Write data & Upload CSV Data")

#Input banner before ingest tgt/ctrl
banner_option = st.sidebar.selectbox("Select Banner", ["DS", "CDS","RBS"])

#Input campaign_name before ingest tgt/ctrl
campaign_name_input = st.sidebar.text_input("Enter Campaign name(e.g. 2024-04_RBS_CRM_SUMMER)")

#Input subgroup before ingest tgt/ctrl
subgroup_name_input = st.sidebar.text_input("Enter subgroup name(e.g. offer, commu)")

#Input start_campaign period before ingest tgt/ctrl
start_camp_input = st.sidebar.text_input("Enter start_campaign period(e.g. 2024-04-16)")

#Input end_campaign period before ingest tgt/ctrl
end_camp_input = st.sidebar.text_input("Enter end_campaign period(e.g. 2024-04-26)")

#Input send_date_sms period before ingest tgt/ctrl
send_date_sms_input = st.sidebar.text_input("Enter send_date_sms period(e.g. 2024-04-26)")

#Input send_date_sms period before ingest tgt/ctrl
send_date_edm_input = st.sidebar.text_input("Enter send_date_edm period(e.g. 2024-04-26)")

#Input send_date_sms period before ingest tgt/ctrl
send_date_app_input = st.sidebar.text_input("Enter send_date_line period(e.g. 2024-04-26)")

uploaded_file = st.sidebar.file_uploader("Upload a CSV file", type=["csv"])

# Manual input for table ID
st.sidebar.header("Part 3) BigQuery Table ID")

# Add a selection box for if_exists parameter
if_exists_option = st.sidebar.selectbox("Select function", ["append", "replace"])

#Input Bigquery table
table_id_input = st.sidebar.text_input("Enter BigQuery table ID (e.g. owner.table_name)")

# Add a button to trigger the upload process
ingest_button = st.sidebar.button("Let's ingest into GBQ")

# Load CSV file
if uploaded_file is not None:
    # Use uploaded file as cache key to invalidate the cache when a new file is uploaded
    @st.cache(allow_output_mutation=True, hash_funcs={pd.DataFrame: lambda _: None})
    def load_data(uploaded_file):
        data = pd.read_csv(uploaded_file)
        return data

    data = load_data(uploaded_file)
    ##manipulate data before ingest
    data['campaign_name'] = campaign_name_input
    data['bu'] = banner_option
    data['subgroup_name'] = subgroup_name_input
    data['create_date'] = date.today()
    data['start_campaign'] = start_camp_input
    data['end_campaign'] = end_camp_input

    data['send_sms'] = np.where(data['commu_type'].str.contains('SMS', case=False, na=False), 'Y', 'N')
    data['send_edm'] = np.where(data['commu_type'].str.contains('EDM', case=False, na=False), 'Y', 'N')
    data['send_line'] = np.where(data['commu_type'].str.contains('LINE', case=False, na=False), 'Y', 'N')
    data['send_the1app'] = np.where(data['commu_type'].str.contains('APP', case=False, na=False), 'Y', 'N')
    data['send_colapp'] = np.where(data['commu_type'].str.contains('COL', case=False, na=False), 'Y', 'N')

    data['send_date_sms'] = np.where(data['commu_type'].str.contains('SMS', case=False, na=False), send_date_sms_input, np.nan)
    data['send_date_edm'] = np.where(data['commu_type'].str.contains('EDM', case=False, na=False), send_date_edm_input, np.nan)
    data['send_date_line'] = np.where(data['commu_type'].str.contains('LINE', case=False, na=False), send_date_line_input, np.nan)
    data['send_date_t1app'] = np.where(data['commu_type'].str.contains('APP', case=False, na=False), send_date_app_input, np.nan)
    data['send_date_colapp'] = np.where(data['commu_type'].str.contains('COL', case=False, na=False), send_date_app_input, np.nan)
    ###Don't forget to convert str to datetime and convert np.nan in to null before ingest GBQ
    
    # Display Data Sample in the main screen
    st.markdown("### Data Sample")
    st.write(data.head())
    st.write("Data contain: ", data.shape[0], " rows", " and ", data.shape[1], " columns")

    # Show success message for CSV upload
    st.success("CSV file uploaded successfully.")
else:
    st.warning("Please upload a CSV file.")

# Load JSON credentials
if uploaded_file_json is not None:
    @st.cache_data
    def load_json():
        return json.load(uploaded_file_json)

    json_data = load_json()

    # Use the uploaded JSON file to create credentials
    credentials = service_account.Credentials.from_service_account_info(json_data)

    # Define BigQuery details
    project_id = 'cdg-mark-cust-prd'

    # Upload DataFrame to BigQuery if CSV is uploaded, table ID is provided, and button is clicked
    if uploaded_file is not None and table_id_input and ingest_button:
        st.markdown("### Uploading to BigQuery")
        
        # Initialize progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            total_steps = 100
            for step in range(total_steps):
                # Simulate a step in the uploading process
                time.sleep(0.1)  # Simulate work being done
                progress_bar.progress(step + 1)
                status_text.text(f"Uploading to BigQuery: {step + 1}%")

            pandas_gbq.to_gbq(data, table_id_input, project_id=project_id, if_exists=if_exists_option, credentials=credentials)
            progress_bar.progress(100)
            status_text.text("Upload Complete!")
            st.success("Data uploaded successfully to BigQuery")
        except Exception as e:
            st.error(f"An error occurred: {e}")
    elif not table_id_input:
        st.warning("Please enter a BigQuery table ID.")
else:
    st.warning("Please upload a JSON file.")
