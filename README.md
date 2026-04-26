# Retail Data Pipeline

This project builds a data engineering pipeline that processes retail sales data, combining multiple datasets and preparing them for analysis. The final output includes cleaned data and monthly aggregated sales metrics.

---

## What this project does

* Merges retail sales data with additional features
* Cleans missing values in key numerical columns
* Converts date values into proper format
* Extracts month information from date
* Filters out low sales records
* Applies basic data transformations
* Aggregates data to compute monthly average sales
* Exports cleaned and aggregated datasets as CSV files

---

## Project structure

* `data/` → input datasets (CSV and parquet files)
* `src/` → main data pipeline logic
* `output/` → generated output files (cleaned and aggregated data)

---

## How to run

Install requirements:

```bash
pip install -r requirements.txt
```

Run the pipeline:

```bash
python src/pipeline.py
```

---

## Notes

* This project was built as part of a practical Data Engineering task
* It demonstrates data cleaning, transformation, and aggregation
* The pipeline is implemented using Python and pandas
* Output files are generated in the `output/` directory
