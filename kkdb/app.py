# Streamlit App for KakiQuant Database Management System with EDA
import streamlit as st
import pymongo
import pandas as pd
from bson.json_util import loads
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
from loguru import logger
import hydra
from omegaconf import DictConfig
import numpy as np
from hydra.core.global_hydra import GlobalHydra

# Clear any existing global Hydra instance
GlobalHydra.instance().clear()

# Setup logger
now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
logger.add(
    sink="./logs/webapp.log",
    rotation="1 day",
    retention="7 days",
    enqueue=True,
    backtrace=True,
    diagnose=True,
)


# Hydra configuration setup
@hydra.main(config_path="./configs", config_name="config", version_base="1.2")
def main(cfg: DictConfig):
    # Initialize MongoDB connection using Hydra config
    client = pymongo.MongoClient(cfg.db.connection_string)
    db = client[cfg.db.default_db]

    # App Title
    st.title("KakiQuant Database Management System 😎")

    # Collection Selection
    collection_list = db.list_collection_names()
    selected_collection:str = st.selectbox("Select Collection", collection_list) # type: ignore
    collection = db[selected_collection]

    # Query Input
    query_input = st.text_area("Enter your query (JSON format)")
    query = {}
    if query_input:
        try:
            query = loads(query_input)
        except ValueError as e:
            st.error(f"Invalid JSON format: {e}")

    # Display Data
    df = pd.DataFrame()
    if st.button("Fetch Data"):
        try:
            results = collection.find(query)
            df = pd.DataFrame(list(results))
            if not df.empty:
                st.dataframe(df)
            else:
                st.write("No data found for the given query.")
        except Exception as e:
            st.error(f"Error fetching data: {e}")

    # Data Analysis Section
    if not df.empty:
        st.header("Exploratory Data Analysis")

        # Data Transformation
        if st.checkbox("Show data transformation options"):
            all_columns = df.columns.tolist()
            selected_column = st.selectbox(
                "Select Column for Transformation", all_columns
            )
            transformations = ["Log", "Square", "Square Root"]
            selected_transformation = st.selectbox(
                "Select Transformation", transformations
            )
            if st.button("Apply Transformation"):
                if selected_transformation == "Log":
                    df[selected_column] = df[selected_column].apply(lambda x: np.log(x))
                elif selected_transformation == "Square":
                    df[selected_column] = df[selected_column].apply(lambda x: x**2)
                elif selected_transformation == "Square Root":
                    df[selected_column] = df[selected_column].apply(
                        lambda x: np.sqrt(x)
                    )
                st.write("Transformed Data", df[[selected_column]])

        # Plotting
        if st.checkbox("Show plotting options"):
            plot_types = ["Histogram", "Box Plot"]
            selected_plot = st.selectbox("Select Plot Type", plot_types)
            selected_column_plot = st.selectbox("Select Column to Plot", all_columns)

            if st.button("Generate Plot"):
                plt.figure(figsize=(10, 4))
                if selected_plot == "Histogram":
                    sns.histplot(df[selected_column_plot], kde=True)  # type: ignore
                elif selected_plot == "Box Plot":
                    sns.boxplot(y=df[selected_column_plot])

                st.pyplot(plt)
                plt.clf()


if __name__ == "__main__":
    main()
