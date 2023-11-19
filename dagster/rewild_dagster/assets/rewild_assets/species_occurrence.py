from dagster import (asset, asset_check, AssetExecutionContext, AssetCheckResult)
import pandas as pd
from datetime import datetime
from rewild_dagster.util import add_table_metadata

@asset(compute_kind="python", io_manager_key="species_occurrence_io_manager")
def species_occurrence(context: AssetExecutionContext, stage_species_occurrence: pd.DataFrame) -> pd.DataFrame:

  location_id_list = []
  guid_list = []
  scientific_name_list = []
  common_name_list = []
  family_list = []
  kingdom_list = []
  count_list = []

  total = len(stage_species_occurrence)
  counter = 1

  for index, row in stage_species_occurrence.iterrows():
      
      location_id = row['location_id']
      state = row['state']
      occurrence_json = row['occurrence']

      context.log.info(f"Processing location_id {location_id} in state {state}, # {counter} of {total}")
      
      for item in occurrence_json:
          common_name = item['commonName']
          count = item['count']
          family = item['family']
          guid = item['guid']
          kingdom = item['kingdom']
          scientific_name = item['name']

          location_id_list.append(location_id)
          guid_list.append(guid)
          scientific_name_list.append(scientific_name)
          common_name_list.append(common_name)
          family_list.append(family)
          kingdom_list.append(kingdom)
          count_list.append(count)

      counter+=1

  data = {'location_id': location_id_list, 'guid':guid_list, 'scientific_name': scientific_name_list, 'common_name': common_name_list, 'family': family_list,'kingdom':kingdom_list, 'count': count_list}
  df = pd.DataFrame(data)

  df = add_table_metadata(context, df)

  return df