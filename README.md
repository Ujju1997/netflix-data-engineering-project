# Project Name : Netflix Data Engineering Project
  
<p align="center">
    <img src="https://community.nasscom.in/sites/default/files/media/images/HR-Analytics.jpg" alt="Logo" width="400" height="300">
</p>


## Introduction
•	Designed and implemented a scalable, event-driven ETL pipeline on Azure to process large-scale datasets from GitHub into a centralized Data Lake.
•	Orchestrated the entire end-to-end workflow using Databricks Workflows, automating the transition from raw data ingestion to cleaned, aggregated production tables.


## Methodology

The following methodology was used to accomplish the project objectives:

1. **Data Loading:** The Netflix data was loaded from the CSV file into the Bronze layer using ADF, and Titles CSV data was loaded using Autoloader. 

2. **Data Cleaning and Pre-processing:** The data cleaning process encompassed eliminating irrelevant data, addressing missing values, standardizing formats, removing duplicates, and adding new columns using Pyspark in Databricks

3. **Dumping:** The transformed data was dumped into the Gold layer in delta format to be used by management. 
