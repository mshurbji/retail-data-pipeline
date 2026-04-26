import pandas as pd
import os


# Merge sales data with extra dataset
def extract(store_data, extra_data):
    extra_df = pd.read_parquet(extra_data)
    merged_df = store_data.merge(extra_df, on="index")
    return merged_df


# Extract
merged_df = extract(grocery_sales, "extra_data.parquet")


# Clean and prepare the data
def transform(raw_data):

    # fill missing values using mean
    raw_data.fillna(
        {
            'CPI': raw_data['CPI'].mean(),
            'Weekly_Sales': raw_data['Weekly_Sales'].mean(),
            'Unemployment': raw_data['Unemployment'].mean(),
        },
        inplace=True
    )

    # convert date and extract month
    raw_data["Date"] = pd.to_datetime(raw_data["Date"])
    raw_data["Month"] = raw_data["Date"].dt.month

    # keep only rows with sufficient sales
    raw_data = raw_data.loc[raw_data["Weekly_Sales"] > 10000, :]

    # drop unnecessary columns
    raw_data = raw_data.drop(
        ["index", "Temperature", "Fuel_Price", "MarkDown1", "MarkDown2",
         "MarkDown3", "MarkDown4", "MarkDown5", "Type", "Size", "Date"],
        axis=1
    )

    return raw_data


# Apply transform
clean_data = transform(merged_df)


# Calculate average monthly sales
def avg_weekly_sales_per_month(clean_data):

    result = clean_data[["Month", "Weekly_Sales"]]

    result = (
        result.groupby("Month")
        .agg(Avg_Sales=("Weekly_Sales", "mean"))
        .reset_index()
        .round(2)
    )

    return result


# Run aggregation
agg_data = avg_weekly_sales_per_month(clean_data)


# Save results to CSV files
def load(full_data, full_data_file_path, agg_data, agg_data_file_path):
    full_data.to_csv(full_data_file_path, index=False)
    agg_data.to_csv(agg_data_file_path, index=False)


# Run load step
load(clean_data, "clean_data.csv", agg_data, "agg_data.csv")


# Validate output files
def validation(file_path):
    if not os.path.exists(file_path):
        raise Exception(f"Missing file: {file_path}")


# Run validation
validation("clean_data.csv")
validation("agg_data.csv")
