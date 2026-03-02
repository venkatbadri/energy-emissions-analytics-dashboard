#Import necessary packages
import os
from datetime import datetime, timedelta
import pandas as pd
from openelectricity import OEClient #Importing openelectricity package
from openelectricity.types import DataMetric, UnitStatusType, UnitFueltechType

# Setting up API with the key
api_key = "oe_3ZJkPQEkujnPedTdFNPAYy8V"  #use your own api key

# Initialising the client
client = OEClient(api_key=api_key)

# Configuring the api
network_code = "NEM"
interval = "5m"  # 5-minute interval
date_start = datetime(2025, 10, 1)
date_end = date_start + timedelta(days=7)  # one week

# Step 1: Getting subset of facilities(Solar+wind)
facilities_response = client.get_facilities(
    network_id=[network_code],
    status_id=[UnitStatusType.OPERATING],
    fueltech_id=[UnitFueltechType.SOLAR_UTILITY, UnitFueltechType.WIND]
)

facility_ids = [f.code for f in facilities_response.data]
print(f"Retrieved {len(facility_ids)} facilities.")
print("Example facilities:", facility_ids[:10])

# Step 2: Get "power" and "emissions" from those facilities
metrics = [DataMetric.POWER, DataMetric.EMISSIONS]

all_records = []

for fid in facility_ids:
    print(f"Fetching data for facility: {fid}")
    current_start = date_start

    while current_start < date_end:
        current_end = min(current_start + timedelta(days=1), date_end)
        print(f"  → {current_start.date()} to {current_end.date()}")

        try:
            resp = client.get_facility_data(
                network_code=network_code,
                facility_code=fid,
                metrics=metrics,
                interval=interval,
                date_start=current_start,
                date_end=current_end
            )

            for series in resp.data:
                for result in series.results:
                    for pt in result.data:
                        all_records.append({
                            "facility_code": fid,
                            "metric": series.metric,
                            "timestamp": pt.timestamp,
                            "value": pt.value,
                        })

        except Exception as e:
            print(f"Skipped {fid} {current_start}–{current_end}: {e}")

        current_start = current_end


# Step 3: Save the file in csv format
df = pd.DataFrame(all_records)
print(df.head())
df.to_csv("nem_facility_power_emissions_oct2025.csv", index=False)
print("Data saved to nem_facility_power_emissions_oct2025.csv")
