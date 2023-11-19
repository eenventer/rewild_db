import requests
import json
from dagster import (asset, asset_check, AssetExecutionContext, AssetCheckResult)
import pandas as pd
from datetime import datetime
import os
import pyarrow as pa
import pyarrow.csv as csv
from rewild_dagster.util import add_table_metadata

def default_cultivatable_plant() -> dict:
    return  {
                "species_id": None,
                "guid": None,
                "scientific_name": None,
                "common_name": None,
                "family": None,
                "plant_type": None,
                "plant_origin": None,
                "light_requirement": None,
                "wind_tolerance": None,
                "growth_rate": None,
                "frost_resistant": None,
                "is_evergreen": None,
                "is_native": None,
                "plant_height": None,
                "plant_width": None,
                "plant_size": None,
                "occurrence_by_state":None,
                "flower_color": None,
                "flowering_month": None,
                "climate_zone": None,
                "is_introduced_act": None,
                "is_introduced_tas": None,
                "is_introduced_wa": None,
                "is_introduced_vic": None,
                "is_introduced_qld": None,
                "is_introduced_nsw": None,
                "is_introduced_sa": None,
                "is_introduced_nt": None,
                "image": None}

def get_image(context: AssetExecutionContext, scientific_name:str):

    ala_image_search_api_endpoint_env = os.getenv("ELT_ALA_IMAGE_SEARCH_API_ENDPOINT")
    ala_image_search_api_endpoint = ala_image_search_api_endpoint_env.format(scientific_name=scientific_name)

    ala_image_detail_api_endpoint_env = os.getenv("ELT_ALA_IMAGE_DETAIL_API_ENDPOINT")

    result = []

    try:

        image_search_request = requests.Request('GET', url=ala_image_search_api_endpoint)
        prepared = image_search_request.prepare()

        image_search_session = requests.Session()
        image_search_response = image_search_session.send(prepared)
        image_search_response_json = json.loads(image_search_response.content)

        image_search_results_json = {"images": image_search_response_json["images"]}
        image_result_list = image_search_results_json['images']

        image_detail_session = requests.Session()

        for image_result in image_result_list:

            image_id = image_result.get('imageIdentifier')
            ala_image_detail_api_endpoint = ala_image_detail_api_endpoint_env.format(image_id=image_id)

            try:
                image_detail_request = requests.Request('GET', url=ala_image_detail_api_endpoint)
                prepared = image_detail_request.prepare()
                
                image_detail_response = image_detail_session.send(prepared)
                image_detail_response_json = json.loads(image_detail_response.content)
                image_url = image_detail_response_json.get('imageUrl')
                size_in_bytes = image_detail_response_json.get('sizeInBytes')
                mime_type = image_detail_response_json.get('mimeType')

                image_detail = {"imageIdentifier":image_id, "imageUrl": image_url}

                result.append(image_detail)

            except Exception as ex:
                context.log.error(f"Error calling API {ala_image_detail_api_endpoint} error {ex}")
            
    except Exception as ex:
        context.log.error(f"Error calling API {ala_image_search_api_endpoint} error {ex}")

    return result

def create_cultivatable_plant(context: AssetExecutionContext, row: pd.Series, go_df : pd.DataFrame, anpsa_df : pd.DataFrame, nns_df: pd.DataFrame) -> dict:

    cultivatable_plant = None
    species_id = row['species_id']
    scientific_name = row['scientific_name']
    common_name = row['common_name']
    family = row['family']
    guid = row['guid']

    
    if not go_df.empty and not anpsa_df.empty:
        context.log.info(f"Cultivatable plant match guid {guid} scientific_name {scientific_name}")
        go_plant_type = go_df['plant_type'].values[0]
        go_plant_origin = go_df['plant_origin'].values[0]
        go_light_requirement = go_df['light_requirement'].values[0]
        go_wind_tolerance = go_df['wind_tolerance'].values[0]
        go_growth_rate = go_df['growth_rate'].values[0]
        go_frost_resistant = go_df['frost_resistant'].values[0]
        go_is_evergreen = go_df['is_evergreen'].values[0]
        go_is_native = go_df['is_native'].values[0]
        go_plant_height = go_df['plant_height'].values[0]
        go_plant_width = go_df['plant_width'].values[0]
        go_flower_color = go_df['flower_color'].values[0]
        go_flowering_month = go_df['flowering_month'].values[0]
        go_climate_zone = go_df['climate_zone'].values[0]
        is_introduced_act = nns_df['is_introduced_act'].values[0] if not nns_df.empty else False
        is_introduced_tas = nns_df['is_introduced_tas'].values[0] if not nns_df.empty else False
        is_introduced_wa = nns_df['is_introduced_wa'].values[0] if not nns_df.empty else False
        is_introduced_vic = nns_df['is_introduced_vic'].values[0] if not nns_df.empty else False
        is_introduced_qld = nns_df['is_introduced_qld'].values[0] if not nns_df.empty else False
        is_introduced_nsw = nns_df['is_introduced_nsw'].values[0] if not nns_df.empty else False
        is_introduced_sa = nns_df['is_introduced_sa'].values[0] if not nns_df.empty else False
        is_introduced_nt = nns_df['is_introduced_nt'].values[0] if not nns_df.empty else False
        image = get_image(context, scientific_name)

        anpsa_occurrence_by_state = anpsa_df['occurrence_by_state'].values[0]
        anpsa_flower_color = anpsa_df['flower_color'].values[0]
        anpsa_flowering_season = anpsa_df['flowering_season'].values[0]
        anpsa_plant_size = anpsa_df['plant_size'].values[0]

        cultivatable_plant = default_cultivatable_plant()
        cultivatable_plant.update({"species_id": species_id})
        cultivatable_plant.update({"guid": guid})
        cultivatable_plant.update({"scientific_name": scientific_name})
        cultivatable_plant.update({"common_name": common_name})
        cultivatable_plant.update({"family": family})
        cultivatable_plant.update({"plant_type": go_plant_type})
        cultivatable_plant.update({"plant_origin": go_plant_origin})
        cultivatable_plant.update({"light_requirement": go_light_requirement})
        cultivatable_plant.update({"wind_tolerance": go_wind_tolerance})
        cultivatable_plant.update({"growth_rate": go_growth_rate})
        cultivatable_plant.update({"frost_resistant": go_frost_resistant})
        cultivatable_plant.update({"is_evergreen": go_is_evergreen})
        cultivatable_plant.update({"is_native": go_is_native})
        cultivatable_plant.update({"plant_height": go_plant_height})
        cultivatable_plant.update({"plant_width": go_plant_width})
        cultivatable_plant.update({"plant_size": anpsa_plant_size})
        cultivatable_plant.update({"occurrence_by_state":anpsa_occurrence_by_state})
        cultivatable_plant.update({"flower_color": go_flower_color})
        cultivatable_plant.update({"flowering_month": go_flowering_month})
        cultivatable_plant.update({"climate_zone": go_climate_zone})
        cultivatable_plant.update({"is_introduced_act": is_introduced_act})
        cultivatable_plant.update({"is_introduced_tas": is_introduced_tas})
        cultivatable_plant.update({"is_introduced_wa": is_introduced_wa})
        cultivatable_plant.update({"is_introduced_vic": is_introduced_vic})
        cultivatable_plant.update({"is_introduced_qld": is_introduced_qld})
        cultivatable_plant.update({"is_introduced_nsw": is_introduced_nsw})
        cultivatable_plant.update({"is_introduced_sa": is_introduced_sa})
        cultivatable_plant.update({"is_introduced_nt": is_introduced_nt})
        cultivatable_plant.update({"image": image})        
            

    elif not go_df.empty:
        context.log.info(f"Cultivatable plant match guid {guid} scientific_name {scientific_name}")
        go_plant_type = go_df['plant_type'].values[0]
        go_plant_origin = go_df['plant_origin'].values[0]
        go_light_requirement = go_df['light_requirement'].values[0]
        go_wind_tolerance = go_df['wind_tolerance'].values[0]
        go_growth_rate = go_df['growth_rate'].values[0]
        go_frost_resistant = go_df['frost_resistant'].values[0]
        go_is_evergreen = go_df['is_evergreen'].values[0]
        go_is_native = go_df['is_native'].values[0]
        go_plant_height = go_df['plant_height'].values[0]
        go_plant_width = go_df['plant_width'].values[0]
        go_flower_color = go_df['flower_color'].values[0]
        go_flowering_month = go_df['flowering_month'].values[0]
        go_climate_zone = go_df['climate_zone'].values[0]
        is_introduced_act = nns_df['is_introduced_act'].values[0] if not nns_df.empty else False
        is_introduced_tas = nns_df['is_introduced_tas'].values[0] if not nns_df.empty else False
        is_introduced_wa = nns_df['is_introduced_wa'].values[0] if not nns_df.empty else False
        is_introduced_vic = nns_df['is_introduced_vic'].values[0] if not nns_df.empty else False
        is_introduced_qld = nns_df['is_introduced_qld'].values[0] if not nns_df.empty else False
        is_introduced_nsw = nns_df['is_introduced_nsw'].values[0] if not nns_df.empty else False
        is_introduced_sa = nns_df['is_introduced_sa'].values[0] if not nns_df.empty else False
        is_introduced_nt = nns_df['is_introduced_nt'].values[0] if not nns_df.empty else False
        
        image = get_image(context, scientific_name)
    
        cultivatable_plant = default_cultivatable_plant()
        cultivatable_plant.update({"species_id": species_id})
        cultivatable_plant.update({"guid": guid})
        cultivatable_plant.update({"scientific_name": scientific_name})
        cultivatable_plant.update({"common_name": common_name})
        cultivatable_plant.update({"family": family})
        cultivatable_plant.update({"plant_type": go_plant_type})
        cultivatable_plant.update({"plant_origin": go_plant_origin})
        cultivatable_plant.update({"light_requirement": go_light_requirement})
        cultivatable_plant.update({"wind_tolerance": go_wind_tolerance})
        cultivatable_plant.update({"growth_rate": go_growth_rate})
        cultivatable_plant.update({"frost_resistant": go_frost_resistant})
        cultivatable_plant.update({"is_evergreen": go_is_evergreen})
        cultivatable_plant.update({"is_native": go_is_native})
        cultivatable_plant.update({"plant_height": go_plant_height})
        cultivatable_plant.update({"plant_width": go_plant_width})
        cultivatable_plant.update({"plant_size": None})
        cultivatable_plant.update({"occurrence_by_state":None})
        cultivatable_plant.update({"flower_color": go_flower_color})
        cultivatable_plant.update({"flowering_month": go_flowering_month})
        cultivatable_plant.update({"climate_zone": go_climate_zone})
        cultivatable_plant.update({"is_introduced_act": is_introduced_act})
        cultivatable_plant.update({"is_introduced_tas": is_introduced_tas})
        cultivatable_plant.update({"is_introduced_wa": is_introduced_wa})
        cultivatable_plant.update({"is_introduced_vic": is_introduced_vic})
        cultivatable_plant.update({"is_introduced_qld": is_introduced_qld})
        cultivatable_plant.update({"is_introduced_nsw": is_introduced_nsw})
        cultivatable_plant.update({"is_introduced_sa": is_introduced_sa})
        cultivatable_plant.update({"is_introduced_nt": is_introduced_nt})
        cultivatable_plant.update({"image": image})

    elif not anpsa_df.empty:
        context.log.info(f"Cultivatable plant match guid {guid} scientific_name {scientific_name}")
        anpsa_occurrence_by_state = anpsa_df['occurrence_by_state'].values[0]
        anpsa_flower_color = anpsa_df['flower_color'].values[0]
        anpsa_flowering_season = anpsa_df['flowering_season'].values[0]
        anpsa_plant_size = anpsa_df['plant_size'].values[0]
        is_introduced_act = nns_df['is_introduced_act'].values[0] if not nns_df.empty else False
        is_introduced_tas = nns_df['is_introduced_tas'].values[0] if not nns_df.empty else False
        is_introduced_wa = nns_df['is_introduced_wa'].values[0] if not nns_df.empty else False
        is_introduced_vic = nns_df['is_introduced_vic'].values[0] if not nns_df.empty else False
        is_introduced_qld = nns_df['is_introduced_qld'].values[0] if not nns_df.empty else False
        is_introduced_nsw = nns_df['is_introduced_nsw'].values[0] if not nns_df.empty else False
        is_introduced_sa = nns_df['is_introduced_sa'].values[0] if not nns_df.empty else False
        is_introduced_nt = nns_df['is_introduced_nt'].values[0] if not nns_df.empty else False
        image = get_image(context, scientific_name)
     
        cultivatable_plant = default_cultivatable_plant()
        cultivatable_plant.update({"species_id": species_id})
        cultivatable_plant.update({"guid": guid})
        cultivatable_plant.update({"scientific_name": scientific_name})
        cultivatable_plant.update({"common_name": common_name})
        cultivatable_plant.update({"family": family})
        cultivatable_plant.update({"plant_type": None})
        cultivatable_plant.update({"plant_origin": None})
        cultivatable_plant.update({"light_requirement": None})
        cultivatable_plant.update({"wind_tolerance": None})
        cultivatable_plant.update({"growth_rate": None})
        cultivatable_plant.update({"frost_resistant": None})
        cultivatable_plant.update({"is_evergreen": None})
        cultivatable_plant.update({"is_native": None})
        cultivatable_plant.update({"plant_height": None})
        cultivatable_plant.update({"plant_width": None})
        cultivatable_plant.update({"plant_size": anpsa_plant_size})
        cultivatable_plant.update({"occurrence_by_state":anpsa_occurrence_by_state})
        cultivatable_plant.update({"flower_color": None})
        cultivatable_plant.update({"flowering_month": None})
        cultivatable_plant.update({"climate_zone": None})
        cultivatable_plant.update({"is_introduced_act": is_introduced_act})
        cultivatable_plant.update({"is_introduced_tas": is_introduced_tas})
        cultivatable_plant.update({"is_introduced_wa": is_introduced_wa})
        cultivatable_plant.update({"is_introduced_vic": is_introduced_vic})
        cultivatable_plant.update({"is_introduced_qld": is_introduced_qld})
        cultivatable_plant.update({"is_introduced_nsw": is_introduced_nsw})
        cultivatable_plant.update({"is_introduced_sa": is_introduced_sa})
        cultivatable_plant.update({"is_introduced_nt": is_introduced_nt})
        cultivatable_plant.update({"image": image})

    else:
        cultivatable_plant = None

    return cultivatable_plant


@asset(deps=["ala_image_search_api"], compute_kind="python", io_manager_key="cultivatable_plant_io_manager")
def cultivatable_plant(context: AssetExecutionContext, species: pd.DataFrame, gardensonline: pd.DataFrame, anpsa: pd.DataFrame, non_native_species: pd.DataFrame) -> pd.DataFrame:
  
    guid_list = gardensonline['guid'].tolist() + anpsa['guid'].tolist()

    # Filter down the 'species' dataframe down to the list of these 'guid' values   
    filtered_species = species[species['guid'].isin(guid_list)]
    species_total = len(filtered_species)

    cultivatable_plant_list = []
    counter = 1
    for index, row in filtered_species.iterrows():
                
        scientific_name = row['scientific_name']
        common_name = row['common_name']
        family = row['family']
        kingdom = row['kingdom']
        guid = row['guid']

        context.log.info(f"Processing species guid {guid} # {counter} of {species_total}")

        go_filtered_df = gardensonline.loc[gardensonline['guid'] == guid]
        anpsa_filtered_df = anpsa.loc[anpsa['guid'] == guid]
        non_native_species_filtered_df = non_native_species.loc[non_native_species['guid'] == guid]

        cultivatable_plant = create_cultivatable_plant(context, row, go_filtered_df, anpsa_filtered_df, non_native_species_filtered_df)

        if cultivatable_plant is not None:
            cultivatable_plant_list.append(cultivatable_plant)

        counter+=1

    df = pd.DataFrame(cultivatable_plant_list)
    df = add_table_metadata(context, df)

    return df