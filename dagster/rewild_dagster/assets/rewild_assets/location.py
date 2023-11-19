from dagster import (asset, asset_check, AssetExecutionContext, AssetCheckResult)
import pandas as pd
from datetime import datetime
import os
import pyarrow as pa
import pyarrow.csv as csv
from rewild_dagster.util import add_table_metadata

def empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=['postcode', 'suburb','state','lat','long'])

@asset(deps=["location_csv"], compute_kind="python", io_manager_key="location_io_manager")
def location(context: AssetExecutionContext) -> pd.DataFrame:
  cwd = os.getcwd()
  location_csv = os.path.join(cwd, "location.csv")

  convert_options = csv.ConvertOptions(
  column_types={
      'postcode': pa.int64(),
      'suburb': pa.string(),
      'state': pa.string(),
      'lat': pa.decimal128(10,3),
      'long': pa.decimal128(10,3)
  })

  table = csv.read_csv(
      input_file=f"{location_csv}", convert_options=convert_options)

  df = empty_df()

  try:
     df = csv.read_csv(input_file=f"{location_csv}",
                              convert_options= convert_options).to_pandas()
  except FileNotFoundError:
     context.log.warning(f"File not found {location_csv}.")

  df = add_table_metadata(context, df)

  df_total = len(df)

  context.log.info(f"Processed {df_total} locations.")

  return df

@asset_check(asset=location, name="location_postcode_has_no_nulls")
def check_location_postcode_has_no_nulls(location: pd.DataFrame) -> AssetCheckResult:
  num_null = location["postcode"].isna().sum()
  return AssetCheckResult(
      passed=bool(num_null == 0),
      metadata={"num_rows": len(location),
                "num_empty": len(location[location["postcode"].isnull()])})

@asset_check(asset=location, name="location_latitutde_has_no_nulls")
def check_location_latitude_has_no_nulls(location: pd.DataFrame) -> AssetCheckResult:
  num_null = location["lat"].isna().sum()
  return AssetCheckResult(
      passed=bool(num_null == 0),
      metadata={"num_rows": len(location),
                "num_empty": len(location[location["lat"].isnull()])})

@asset_check(asset=location, name="location_longitude_has_no_nulls")
def check_location_longitude_has_no_nulls(location: pd.DataFrame) -> AssetCheckResult:
  num_null = location["long"].isna().sum()
  return AssetCheckResult(
      passed=bool(num_null == 0),
      metadata={"num_rows": len(location),
                "num_empty": len(location[location["long"].isnull()])})