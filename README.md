
# Vehicle Insurance Claims Analytics Data Platform

## Project Overview
This project is a data engineering platform built on Databricks to process and analyze vehicle insurance data. It converts raw datasets into meaningful insights for claims analysis and reporting.

## Architecture
The platform follows Medallion Architecture:
- Bronze → Raw data  
- Silver → Cleaned data  
- Gold → Analytics-ready data  

## Data Pipeline
Data flows from Bronze to Silver to Gold where it is cleaned, validated, and transformed into KPIs.

## Key Features
- Batch and streaming processing  
- Data validation and cleaning  
- Fact and dimension modeling  
- KPI generation  
- SCD Type 2 implementation  
- Secure access using Unity Catalog  

## Data Model
- Fact table: fact_claims  
- Dimensions: customer, policy, vehicle  

## KPIs
- Total claims  
- Average claim amount  
- Settlement rate  
- High-risk customers  
```
project/
├── bronze.py   # Handles raw data ingestion (Bronze layer)
├── silver.py   # Performs data cleaning and transformations (Silver layer)
├── gold.py     # Generates analytics and KPIs (Gold layer)
```

## Execution Steps
1. Load data  
2. Run Bronze  
3. Run Silver  
4. Run Gold  

## Future Scope
- Fraud detection using ML  
- Advanced analytics  
