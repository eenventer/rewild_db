from dagster import (asset, asset_check, AssetExecutionContext, AssetCheckResult)
import pandas as pd
from datetime import datetime
import os
import pyarrow as pa
import pyarrow.csv as csv
import json
from rewild_dagster.util import convert_yes_no_string_to_boolean, convert_to_json, column_array_split, add_table_metadata

def empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=['guid', 'plant_type','plant_origin','light_requirement','wind_tolerance','growth_rate','frost_resistant',
                                 'is_evergreen', 'is_native','plant_height','plant_width','flower_color','flowering_month','climate_zone'])

@asset(deps=["gardensonline_csv"], compute_kind="python", io_manager_key="gardensonline_io_manager")
def gardensonline(context: AssetExecutionContext) -> pd.DataFrame:
  cwd = os.getcwd()
  gardensonline_csv = os.path.join(cwd, "gardensonline.csv")

  go_convert_options = csv.ConvertOptions(
          column_types={
              'guid': pa.string(),
              'plant_type': pa.string(),
              'plant_origin': pa.string(),
              'light_requirement': pa.string(),
              'wind_tolerance': pa.string(),
              'growth_rate': pa.string(),
              'frost_resistant': pa.string(),
              'is_evergreen': pa.string(),
              'is_native': pa.string(),
              'plant_height': pa.string(),
              'plant_width': pa.string(),
              'flower_color': pa.string(),
              'flowering_month': pa.string(),
              'climate_zone': pa.string()
          })

  df = empty_df()
  
  try:
     df = csv.read_csv(input_file=f"{gardensonline_csv}",
                              convert_options= go_convert_options).to_pandas()
  except FileNotFoundError:
     context.log.warning(f"File not found {gardensonline_csv}.")

  df['flower_color'] = df['flower_color'].apply(column_array_split)
  df['flowering_month'] = df['flowering_month'].apply(column_array_split)

  df['is_evergreen'] = df['is_evergreen'].apply(convert_yes_no_string_to_boolean)
  df['is_native'] = df['is_native'].apply(convert_yes_no_string_to_boolean)    

  df['climate_zone'] = df['climate_zone'].apply(convert_to_json)

  df = add_table_metadata(context, df)

  df_total = len(df)

  context.log.info(f"Processed {df_total} locations.")

  return df
