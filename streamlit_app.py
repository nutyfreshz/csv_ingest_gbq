# import subprocess
# command = ["pip", "install", "-r", "requirements.txt"]

# import streamlit as st
# import pandas as pd
# import pandas_gbq
# import json
# from google.oauth2 import service_account

# # Title
# st.title("Upload CSV into GBQ App")

# # Upload JSON credential file
# st.sidebar.header("Upload JSON Credential")
# uploaded_file_json = st.sidebar.file_uploader("Upload a JSON file", type=["json"])

# # Upload CSV file
# st.sidebar.header("Upload CSV Data")
# uploaded_file = st.sidebar.file_uploader("Upload a CSV file", type=["csv"])


# if uploaded_file is not None:
#     @st.cache_data
#     def load_data():
#         data = pd.read_csv(uploaded_file)
#         return data

#     data = load_data()

#     # Display Data Sample in the main screen
#     st.markdown("### Data Sample")
#     st.write(data.head())
# else:
#     st.warning("Please upload a CSV file.")

# if uploaded_file_json is not None:
#     @st.cache_data
#     def load_json():
#         return json.load(uploaded_file_json)

#     json_data = load_json()

#     # Display JSON content in the main screen
#     st.markdown("### JSON Credential Data")
#     st.json(json_data)

#     # Use the uploaded JSON file to create credentials
#     credentials = service_account.Credentials.from_service_account_info(json_data)

#     # Define BigQuery details
#     project_id = 'cdg-mark-cust-prd'
#     table_id = 'TEMP_NUTCHAPONG.kd_temp_test_csv_upload'

#     # Upload DataFrame to BigQuery if CSV is uploaded
#     if uploaded_file is not None:
#         st.markdown("### Uploading to BigQuery")
#         try:
#             pandas_gbq.to_gbq(data, table_id, project_id=project_id, if_exists='replace', credentials=credentials)
#             st.success("Data uploaded successfully to BigQuery")
#         except Exception as e:
#             st.error(f"An error occurred: {e}")
# else:
#     st.warning("Please upload a JSON file.")


#####################

import subprocess
command = ["pip", "install", "-r", "requirements.txt"]

import streamlit as st
import pandas as pd
import pandas_gbq
import json
from google.oauth2 import service_account

# Title
st.title("Upload CSV into GBQ App")

# Upload JSON credential file
st.sidebar.header("Upload JSON Credential")
uploaded_file_json = st.sidebar.file_uploader("Upload a JSON file", type=["json"])

# Upload CSV file
st.sidebar.header("Upload CSV Data")
uploaded_file = st.sidebar.file_uploader("Upload a CSV file", type=["csv"])

# Load CSV file
if uploaded_file is not None:
    @st.cache_data
    def load_data():
        data = pd.read_csv(uploaded_file)
        return data

    data = load_data()

    # Display Data Sample in the main screen
    st.markdown("### Data Sample")
    st.write(data.head())

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
    table_id = 'TEMP_NUTCHAPONG.kd_temp_test_csv_upload'

    # Upload DataFrame to BigQuery if CSV is uploaded
    if uploaded_file is not None:
        st.markdown("### Uploading to BigQuery")
        try:
            pandas_gbq.to_gbq(data, table_id, project_id=project_id, if_exists='replace', credentials=credentials)
            st.success("Data uploaded successfully to BigQuery")
        except Exception as e:
            st.error(f"An error occurred: {e}")
else:
    st.warning("Please upload a JSON file.")
