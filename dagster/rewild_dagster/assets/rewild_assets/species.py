from dagster import (asset, asset_check, AssetExecutionContext, AssetCheckResult)
import pandas as pd
from datetime import datetime
from rewild_dagster.util import add_table_metadata

@asset(compute_kind="python", io_manager_key="species_io_manager")
def species(context: AssetExecutionContext, species_occurrence: pd.DataFrame) -> pd.DataFrame:

  df = species_occurrence.groupby('guid').agg({'scientific_name': 'min',
                                                'common_name': 'min',
                                                'family': 'min',
                                                'kingdom': 'min'}).reset_index()
  
  df['species_id'] = range(1, len(df) + 1)

  df = df[['species_id', 'guid', 'scientific_name', 'common_name', 'family', 'kingdom']]

  df = add_table_metadata(context, df)

  return df

@asset_check(asset=species, name="species_guid_is_unique")
def check_species_guid_is_unique(species: pd.DataFrame) -> AssetCheckResult:
  
  non_unique_guids = species['guid'][species['guid'].duplicated(keep=False)]
  non_unique_guid_count = non_unique_guids.value_counts()

  return AssetCheckResult(
      passed=bool(non_unique_guid_count.empty),
      metadata={"num_rows": len(species),
                "num_non_unique_guid": str(non_unique_guid_count)})
