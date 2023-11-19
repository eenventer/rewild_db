from dagster import (asset, asset_check, AssetExecutionContext, AssetCheckResult)
from rewild_dagster.partitions import state_partitions
import pandas as pd
from datetime import datetime
import os
import requests
import json
from rewild_dagster.util import add_table_metadata

@asset(deps=["ala_occurrence_api"], compute_kind="python", io_manager_key="stage_species_occurrence_io_manager")
def stage_species_occurrence(context: AssetExecutionContext, unique_location: pd.DataFrame) -> pd.DataFrame:

  #state = context.asset_partition_key_for_output()
  #context.log.info(f"Partition {state}")

  flimit = 10000
  radius = 10

  location_id_list = []
  lat_list = []
  long_list = []
  state_list = []
  occurrence_list = []

  #partitioned_df = unique_location.loc[unique_location['state'] == state]
  #partitioned_df_len = len(partitioned_df)
  unqiue_location_total = len(unique_location)
  counter = 1

  for index, row in unique_location.iterrows():

      location_id = row['location_id']
      lat = row['lat']
      long = row['long']
      state = row['state']

      context.log.info(f"Processing location_id {location_id} in state {state}, # {counter} of {unqiue_location_total}")
      
      #call ala occurrence API
      occurrence_json = get_ala_occurrence(context, radius, lat, long, flimit)

      location_id_list.append(location_id)
      lat_list.append(lat)
      long_list.append(long)
      state_list.append(state)
      occurrence_list.append(occurrence_json)

      counter+=1

  data = {'location_id': location_id_list, 'lat': lat_list, 'long': long_list, 'state': state_list,'occurrence':occurrence_list}
  df = pd.DataFrame(data)

  bad_occurrence_json = '{"message":"Endpoint request timed out"}'

  bad_occurrence_df = df[df['occurrence'] == bad_occurrence_json]

  df = add_table_metadata(context, df)

  context.log.info(f"{len(bad_occurrence_df)} bad occurrence data rows. {bad_occurrence_df.to_string()}")

  return df

def get_ala_occurrence(context, radius, lat, lon, flimit):
  ala_occurrences_api_endpoint_env = os.getenv("ELT_ALA_OCCURRENCES_API_ENDPOINT")
  ala_occurrences_api_endpoint_query_params = os.getenv("ELT_ALA_OCCURRENCES_API_ENDPOINT_QUERY_PARAMS")

  ala_occurrences_api_endpoint = ala_occurrences_api_endpoint_env + ala_occurrences_api_endpoint_query_params.format(radius=radius, lat=lat, lon=lon, flimit=flimit)

  try:
     request = requests.Request('GET', url=ala_occurrences_api_endpoint)
     prepared = request.prepare()
     request_session = requests.Session()
     response = request_session.send(prepared)
     occurrence_json = json.loads(response.content)
     return occurrence_json
  except Exception as ex:
     context.log.error(f"Error calling API {ala_occurrences_api_endpoint} error {ex}")
     return None
  
@asset_check(asset=stage_species_occurrence, name="stage_species_occurrence_contains_valid_occurrence_json")
def check_stage_species_occurrence_contains_valid_occurrence_json(stage_species_occurrence: pd.DataFrame) -> AssetCheckResult:

  error_count = 0
  for entry in stage_species_occurrence['occurrence']:
    if isinstance(entry, dict) and entry.get('message') == 'Endpoint request timed out':
        error_count += 1

  return AssetCheckResult(
      passed=bool(error_count == 0),
      metadata={"num_rows": len(stage_species_occurrence),
                "rows with bad occurrence json ": str(error_count)})  