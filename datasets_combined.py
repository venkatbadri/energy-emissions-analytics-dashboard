#Import necessary package
import pandas as pd

# -----------------------------
# File paths for the three csv files and "merged_cleaned_file" is from the Assignment 1
# -----------------------------

MERGED_CLEANED_FILE = "merged_cleaned_data.csv"
CONSOLIDATED_FILE = "consolidated_data.csv"
NEM_FILE = "nem_facility_power_emissions_oct2025.csv"

# -----------------------------
# Loading the data
# -----------------------------

# Merged the cleaned data
merged_df = pd.read_csv(MERGED_CLEANED_FILE, low_memory=False)
print("Merged cleaned data columns:", merged_df.columns.tolist())

# Consolidated data
consolidated_df = pd.read_csv(CONSOLIDATED_FILE, parse_dates=["timestamp"])
print("Consolidated data columns:", consolidated_df.columns.tolist())

# NEM data
nem_df = pd.read_csv(NEM_FILE, parse_dates=["timestamp"])
print("NEM data columns:", nem_df.columns.tolist())

# -----------------------------
# Prepare "NEM data"
# -----------------------------
# Aggregate to latest per facility_code & metric
nem_latest = (
    nem_df.sort_values("timestamp")
    .groupby(["facility_code", "metric"])
    .tail(1)
)

# Pivot so each facility has columns for each metric
nem_pivot = (
    nem_latest.pivot(index="facility_code", columns="metric", values="value")
    .rename(columns={"power": "power_nem", "emissions": "emissions_nem"})
    .reset_index()
)

# -----------------------------
# Prepare "Consolidate data"
# -----------------------------

# Aggregate latest per facility_code
consolidated_latest = (
    consolidated_df.sort_values("timestamp")
    .groupby("facility_code")
    .tail(1)
    .rename(columns={"power": "power_consolidated", "emissions": "emissions_consolidated"})
)

# -----------------------------
# Merge all the datasets
# -----------------------------

# Merge merged_cleaned_data with consolidated data on facility_code
#-----> First, ensure facility codes exist in merged_df
if "Facility name" in merged_df.columns:
    #-> Use "Facility name" as key if "facility_code" is missing
    merged_df["facility_code"] = merged_df["Facility name"].str.upper().str.replace(" ", "")
else:
    merged_df["facility_code"] = merged_df["Reporting entity"].str.upper().str.replace(" ", "")

full_df = merged_df.merge(consolidated_latest[["facility_code", "power_consolidated", "emissions_consolidated"]],
                          on="facility_code", how="left")

#-----> Merge "NEM data"
full_df = full_df.merge(nem_pivot, on="facility_code", how="left")

# -----------------------------
# Calculate the new insights
# -----------------------------

# Ensure numeric
full_df["MW Capacity"] = pd.to_numeric(full_df.get("MW Capacity", 0), errors="coerce").fillna(0)
full_df["Total emissions t CO2 e"] = pd.to_numeric(full_df.get("Total emissions t CO2 e", 0), errors="coerce").fillna(0)

# Emission intensity per MW
full_df["emission_intensity"] = full_df["Total emissions t CO2 e"] / full_df["MW Capacity"].replace(0, 1)

# -----------------------------
# Save in csv format
# -----------------------------

full_df.to_csv("full_merged_dataset.csv", index=False)
print("Merged dataset shape:", full_df.shape)
print(full_df.head())
