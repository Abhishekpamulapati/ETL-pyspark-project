# Building ETL Pipeline with PySpark

Code and sample data for the portfolio project of **"Building Real ETL Pipeline with PySpark."**

The PySpark data engineering is about building pipelines that run reliably with messy, real-world data. This project walks you through building a complete ETL pipeline from scratch.

## What is Built

A production-ready ETL pipeline that:
- Reads messy CSV data from multiple sources (online orders, store orders, mobile orders)
- Handles real data quality issues: inconsistent formats, test data, duplicates
- Cleans and standardizes customer IDs, prices, and dates
- Outputs clean CSV files ready for analysis
- Includes logging, error handling, and data quality checks

## Setup

### Requirements
- Python 3.8+
- Java 11 or 17 (recommended for Spark)

## Project Structure

```
pyspark-etl-tutorial/
├── data/
│   └── raw/                    # Sample CSV files with realistic data issues
│       ├── online_orders.csv
│       ├── store_orders.csv
│       └── mobile_orders.csv
├── main.py                     # Skeleton orchestration code
└── requirements.txt
```

## Sample Data

The CSV files in `data/raw/` contain realistic data quality issues that needs attention:

- **Inconsistent customer IDs**: Some are numbers (`12345`), some have prefixes (`CUST_12345`)
- **Messy prices**: Mix of `$12.99` and `12.99` formats
- **Test data**: Records with `TEST` in various fields that need filtering
- **Duplicates**: Same order appearing multiple times
- **Inconsistent dates**: Different date formats across files

These are the exact issues encountered in real production data.

## Overview

The complete project at: **[[LINK TO PROJECT](https://github.com/Abhishekpamulapati/ETL-pyspark-project)]**

This project walks through:
1. Setting up a professional ETL project structure
2. Building Extract, Transform, and Load functions
3. Handling data quality issues defensively
4. Adding logging, error handling, and validation
5. Running the complete pipeline with `spark-submit`

## Quick Start

After following the tutorial, run your completed pipeline:

```bash
spark-submit main.py
```

Successful output will be in `data/processed/orders/orders.csv` with a summary report in `logs/`.

## What I Learned during this project

- **Professional ETL patterns**: Separate extract, transform, load functions
- **Defensive data reading**: Explicit schemas and error handling
- **Real-world transformations**: Cleaning messy strings, standardizing formats
- **Data quality**: Flagging suspicious values, removing test data
- **Production practices**: Logging, validation checks, summary reports
