from dagster import (asset, asset_check, AssetExecutionContext, AssetCheckResult)
import pandas as pd
from datetime import datetime
import os
import pyarrow as pa
import pyarrow.csv as csv
from rewild_dagster.util import convert_yes_no_string_to_boolean, convert_to_json, column_array_split, add_table_metadata

def empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=['guid', 'occurrence_by_state','flower_color','flowering_season','plant_size'])

@asset(deps=["anpsa_csv"], compute_kind="python", io_manager_key="anpsa_io_manager")
def anpsa(context: AssetExecutionContext) -> pd.DataFrame:
  cwd = os.getcwd()

  anpsa_csv = os.path.join(cwd, "anpsa.csv")

  anpsa_convert_options = csv.ConvertOptions(
          column_types={
              'guid': pa.string(),
              'occurrence_by_state': pa.string(),
              'flower_color': pa.string(),
              'flowering_season': pa.string(),
              'plant_size': pa.string()
          })


  df = empty_df()
  
  try:
     df = csv.read_csv(input_file=f"{anpsa_csv}",
                              convert_options= anpsa_convert_options).to_pandas()
  except FileNotFoundError:
     context.log.warning(f"File not found {anpsa_csv}.")
     

  df['occurrence_by_state'] = df['occurrence_by_state'].apply(column_array_split)
  df['flower_color'] = df['flower_color'].apply(column_array_split)
  df['flowering_season'] = df['flowering_season'].apply(column_array_split)

  df = add_table_metadata(context, df)  

  df_total = len(df)

  context.log.info(f"Processed {df_total} locations.")

  return df