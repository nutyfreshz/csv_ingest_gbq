import subprocess
import streamlit as st
import pandas as pd
import pandas_gbq
import json
from google.oauth2 import service_account
import time
from datetime import date
from datetime import datetime
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
    <p>1. Browse JSON Credential file from moderator in Part 1) section.</p>
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

# Input banner before ingest tgt/ctrl
banner_option = st.sidebar.selectbox("Select Banner", ["DS", "CDS", "RBS"])

# Input campaign_name before ingest tgt/ctrl
campaign_name_input = st.sidebar.text_input("Enter Campaign name(e.g. 2024-04_RBS_CRM_SUMMER)")

# Input subgroup before ingest tgt/ctrl
subgroup_name_input = st.sidebar.text_input("Enter subgroup name(e.g. offer, commu)")

# Input start_campaign period before ingest tgt/ctrl
start_camp_input = st.sidebar.text_input("Enter start_campaign period(e.g. 2024-04-16)")

# Input end_campaign period before ingest tgt/ctrl
end_camp_input = st.sidebar.text_input("Enter end_campaign period(e.g. 2024-04-26)")

# Input send_date_sms period before ingest tgt/ctrl
send_date_sms_input = st.sidebar.text_input("Enter send_date_sms period(e.g. 2024-04-26)")

# Input send_date_edm period before ingest tgt/ctrl
send_date_edm_input = st.sidebar.text_input("Enter send_date_edm period(e.g. 2024-04-26)")

# Input send_date_line period before ingest tgt/ctrl
send_date_line_input = st.sidebar.text_input("Enter send_date_line period(e.g. 2024-04-26)")

# Input send_date_t1app period before ingest tgt/ctrl
send_date_t1app_input = st.sidebar.text_input("Enter send_date_t1app period(e.g. 2024-04-26)")

# Input send_date_app period before ingest tgt/ctrl
send_date_colapp_input = st.sidebar.text_input("Enter send_date_colapp period(e.g. 2024-04-26)")

# Input send_date_martech period before ingest tgt/ctrl
send_date_martech_input = st.sidebar.text_input("Enter send_date_martech period(e.g. 2024-04-26)")

# Input send_date_facebook period before ingest tgt/ctrl
send_date_fb_input = st.sidebar.text_input("Enter send_date_facebook period(e.g. 2024-04-26)")

# Input send_date_call period before ingest tgt/ctrl
send_date_call_input = st.sidebar.text_input("Enter send_date_call period(e.g. 2024-04-26)")

# Input requester
req_option = st.sidebar.selectbox("Select requester", ["Bodee B.", "Chegita S.", "Kamontip A.", "Lalita P.", "Napas K.", "Paniti T.", "Pattamaporn V.", "Phuwanat T.", "Sypabhas T.", "Thus S.", "Tunsinee U.", "Watcharapon P."])

# Input data_owner
owner_option = st.sidebar.selectbox("Select data_owner", ["Kamontip A.", "Kittipob S.", "Nutchapong L.", "Paniti T.", "Pattamaporn V.", "Phat P.", "Pornpawit J."])

uploaded_file = st.sidebar.file_uploader("Upload a CSV file", type=["csv"])

# Manual input for table ID
st.sidebar.header("Part 3) BigQuery Table ID")

# Add a selection box for if_exists parameter
if_exists_option = st.sidebar.selectbox("Select function", ["append", "replace"])

# Input Bigquery table
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
    ## Manipulate data before ingest
    data['bu'] = banner_option
    data['campaign_name'] = campaign_name_input
    data['subgroup_name'] = subgroup_name_input
    data['create_date'] = date.today()
    data['start_campaign'] = start_camp_input
    data['end_campaign'] = end_camp_input

    data['send_sms'] = np.where(data['commu_type'].str.contains('SMS', case=False, na=False), 'Y', 'N')
    data['send_edm'] = np.where(data['commu_type'].str.contains('EDM', case=False, na=False), 'Y', 'N')
    data['send_line'] = np.where(data['commu_type'].str.contains('LINE', case=False, na=False), 'Y', 'N')
    data['send_the1app'] = np.where(data['commu_type'].str.contains('T1APP', case=False, na=False), 'Y', 'N')
    data['send_colapp'] = np.where(data['commu_type'].str.contains('COL', case=False, na=False), 'Y', 'N')
    data['send_martech'] = np.where(data['commu_type'].str.contains('MART', case=False, na=False), 'Y', 'N')
    data['send_facebook'] = np.where(data['commu_type'].str.contains('FB', case=False, na=False), 'Y', 'N')
    data['send_call'] = np.where(data['commu_type'].str.contains('CALL', case=False, na=False), 'Y', 'N')

    data['send_date_sms'] = np.where(data['commu_type'].str.contains('SMS', case=False, na=False), send_date_sms_input, np.nan)
    data['send_date_edm'] = np.where(data['commu_type'].str.contains('EDM', case=False, na=False), send_date_edm_input, np.nan)
    data['send_date_line'] = np.where(data['commu_type'].str.contains('LINE', case=False, na=False), send_date_line_input, np.nan)
    data['send_date_the1app'] = np.where(data['commu_type'].str.contains('T1APP', case=False, na=False), send_date_t1app_input, np.nan)
    data['send_date_colapp'] = np.where(data['commu_type'].str.contains('COL', case=False, na=False), send_date_colapp_input, np.nan)
    data['send_date_martech'] = np.where(data['commu_type'].str.contains('MART', case=False, na=False), send_date_martech_input, np.nan)
    data['send_date_facebook'] = np.where(data['commu_type'].str.contains('FB', case=False, na=False), send_date_fb_input, np.nan)
    data['send_date_call'] = np.where(data['commu_type'].str.contains('CALL', case=False, na=False), send_date_call_input, np.nan)

    data['requester'] = req_option
    data['data_owner'] = owner_option
    
    ### Convert str to datetime and convert np.nan to null before ingest GBQ
    date_columns = ['start_campaign', 'end_campaign', 'send_date_sms', 'send_date_edm', 'send_date_line', 'send_date_the1app', 'send_date_colapp', 'send_date_martech', 'send_date_facebook', 'send_date_call']
    for col in date_columns:
        data[col] = pd.to_datetime(data[col], errors='coerce').dt.strftime('%Y-%m-%d')

    ### Select column
    data = data[['bu', 'campaign_name', 'group_name', 'subgroup_name', 'target', 'create_date', 'start_campaign', 'end
