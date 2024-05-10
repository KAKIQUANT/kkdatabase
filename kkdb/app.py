# Streamlit App for KKDB
import streamlit as st
import pymongo
import pandas as pd
import hydra
#App Title
st.title('KakiQuant Database Management System😎')

@st.cache_resource
def init_connection():
    connection_string = st.secrets["mongo"]["connection_string"]
    return pymongo.MongoClient(connection_string)

client = init_connection()
db = client.crypto

@st.cache_data(ttl=600)
def get_universities():
    collection = db['universities'].find({})
    return pd.DataFrame(collection)


universities = get_universities()

#Filter by location
unique_locations = universities['location'].unique().tolist()
selected_locations = st.multiselect("Filter by Country:", unique_locations)
if selected_locations:
    universities = universities[universities['location'].isin(selected_locations)]

#disply the dataframe
st.write(universities)

@hydra.main(config_path="configs/config.yaml")
def main(cfg):
    pass


if __name__ == ""