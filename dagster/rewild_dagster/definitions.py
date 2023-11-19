import os

from rewild_dagster.assets import rewild_assets
from rewild_dagster.assets.rewild_assets.location import check_location_postcode_has_no_nulls, check_location_latitude_has_no_nulls, check_location_longitude_has_no_nulls
from rewild_dagster.assets.rewild_assets.unique_location import check_unique_location_has_no_duplicate_lat_long_combinations
from rewild_dagster.assets.rewild_assets.stage_species_occurrence import check_stage_species_occurrence_contains_valid_occurrence_json
from rewild_dagster.assets.rewild_assets.species import check_species_guid_is_unique
from rewild_dagster.resources.resources import (resources)
from dagster import Definitions

all_assets = [*rewild_assets]

asset_checks = [check_location_postcode_has_no_nulls, check_location_latitude_has_no_nulls, check_location_longitude_has_no_nulls,
                check_unique_location_has_no_duplicate_lat_long_combinations,
                check_stage_species_occurrence_contains_valid_occurrence_json,
                check_species_guid_is_unique]

defs = Definitions(assets=all_assets, 
                   resources=resources,
                   asset_checks=asset_checks)
