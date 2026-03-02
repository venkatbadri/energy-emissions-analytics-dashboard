#Import necessary packages
import pandas as pd

# -----------------------------
# Load the csv created from the data_retrieval.py file
# -----------------------------

csv_path = "nem_facility_power_emissions_oct2025.csv"

try:
    df = pd.read_csv(csv_path)
    print(f"Loaded {df.shape[0]} rows, {df.shape[1]} columns from {csv_path}")
except FileNotFoundError:
    raise SystemExit(f"File not found: {csv_path}. Make sure data_retrieval.py ran successfully.")

# -----------------------------
# Clean and process the file
# -----------------------------

# Remove duplicates
df.drop_duplicates(inplace=True)

# Fill missing values
df.fillna(0, inplace=True)

# Make sure timestamp in datetime format
if "timestamp" in df.columns:
    df["timestamp"] = pd.to_datetime(df["timestamp"])

# -----------------------------
# Restructure for analysis
# -----------------------------

# Pivot the metrics (so 'power' and 'emissions' become columns)
if {"facility_code", "metric", "value"}.issubset(df.columns):
    df_pivot = df.pivot_table(
        index=["facility_code", "timestamp"],
        columns="metric",
        values="value",
        aggfunc="first"
    ).reset_index()
else:
    df_pivot = df.copy()

# -----------------------------
# Save the file in csv format
# -----------------------------

output_path = "consolidated_data.csv"
df_pivot.to_csv(output_path, index=False)

print(f"Consolidated data saved to {output_path}")
print(f"Rows: {df_pivot.shape[0]}, Columns: {df_pivot.shape[1]}")
