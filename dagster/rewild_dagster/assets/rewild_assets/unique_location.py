from dagster import (asset, asset_check, AssetExecutionContext, AssetCheckResult)
import pandas as pd
from datetime import datetime
import hashlib
from rewild_dagster.util import add_table_metadata

def generate_location_id(lat, long):
    location_str = f"{lat}{long}"
    return hashlib.md5(location_str.encode()).hexdigest()

@asset(compute_kind="python", io_manager_key="unique_location_io_manager")
def unique_location(context: AssetExecutionContext, location: pd.DataFrame) -> pd.DataFrame:
  
  df = location.drop_duplicates(subset=['lat', 'long']).drop('suburb', axis=1)

  # Apply the function to create the 'location_id' column
  df['location_id'] = df.apply(lambda row: generate_location_id(row['lat'], row['long']), axis=1)
  df = df[['location_id'] + [col for col in df.columns if col != 'location_id']]

  df = add_table_metadata(context, df)

  return df

@asset_check(asset=unique_location, name="unique_location_has_no_duplicate_lat_long_combinations")
def check_unique_location_has_no_duplicate_lat_long_combinations(unique_location: pd.DataFrame) -> AssetCheckResult:

  df_temp = unique_location[['lat', 'long']].astype(str)
  duplicates = df_temp.duplicated()
  duplicate_count = duplicates.sum()

  return AssetCheckResult(
      passed=bool(duplicate_count == 0),
      metadata={"num_rows": len(unique_location),
                "dupliate lat long pairs": str(duplicate_count)})
