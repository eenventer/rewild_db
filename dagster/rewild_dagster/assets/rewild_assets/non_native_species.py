from dagster import (asset, asset_check, AssetExecutionContext, AssetCheckResult)
import pandas as pd
from datetime import datetime
import os
import pyarrow as pa
import pyarrow.csv as csv
from rewild_dagster.util import add_table_metadata

@asset(deps=["non_native_species_csv"], compute_kind="python", io_manager_key="non_native_species_io_manager")
def non_native_species(context: AssetExecutionContext) -> pd.DataFrame:
  cwd = os.getcwd()
  non_native_species_csv = os.path.join(cwd, "non_native_species.csv")

  convert_options = csv.ConvertOptions(
    column_types={
                'guid': pa.string(),
                'act': pa.int64(),
                'tas': pa.int64(),
                'wa': pa.int64(),
                'vic': pa.int64(),
                'qld': pa.int64(),
                'nsw': pa.int64(),
                'sa': pa.int64(),
                'nt': pa.int64()
            })

  table = csv.read_csv(
      input_file=f"{non_native_species_csv}", convert_options=convert_options)

  df = table.to_pandas()

  df = df.groupby('guid').agg({'act':'max','tas':'max','wa':'max','vic':'max','qld':'max','nsw':'max','sa':'max','nt':'max',}).reset_index()

  columns_to_transform = ['act', 'tas', 'wa', 'vic', 'qld', 'nsw', 'sa', 'nt']

  for column in columns_to_transform:
    df[column] = df[column].replace({0: False, 1: True})
  
  new_columns = {'act': 'is_introduced_act', 'tas': 'is_introduced_tas', 'wa': 'is_introduced_wa', 'vic': 'is_introduced_vic', 'qld': 'is_introduced_qld', 'nsw': 'is_introduced_nsw', 'sa': 'is_introduced_sa', 'nt': 'is_introduced_nt'}
  df.rename(columns=new_columns, inplace=True)

  df = add_table_metadata(context, df)

  return df