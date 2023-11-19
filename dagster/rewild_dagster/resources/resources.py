from rewild_dagster.iomanagers import (location_io_manager, unqiue_location_io_manager, 
                                       stage_species_occurence_io_manager, species_occurence_io_manager, 
                                       species_io_manager, non_native_species_io_manager, cultivatable_plant_io_manager,
                                       anpsa_io_manager, gardensonline_io_manager)


resources = {
    "location_io_manager": location_io_manager.configured({}),
    "unique_location_io_manager": unqiue_location_io_manager.configured({}),
    "stage_species_occurrence_io_manager": stage_species_occurence_io_manager.configured({}),
    "species_occurrence_io_manager": species_occurence_io_manager.configured({}),
    "species_io_manager": species_io_manager.configured({}),
    "non_native_species_io_manager": non_native_species_io_manager.configured({}),
    "cultivatable_plant_io_manager": cultivatable_plant_io_manager.configured({}),
    "anpsa_io_manager": anpsa_io_manager.configured({}),
    "gardensonline_io_manager": gardensonline_io_manager.configured({})
}