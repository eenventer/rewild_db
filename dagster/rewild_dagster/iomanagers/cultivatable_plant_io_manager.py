import pandas as pd
import os

from dagster import IOManager, io_manager, InputContext, OutputContext
from sqlalchemy import create_engine
from sqlalchemy.types import String, Integer, DateTime, Boolean
from sqlalchemy.dialects.postgresql import JSONB

class Cultivatable_Plant_IOManager(IOManager):
    def __init__(self):
        pass

    def handle_output(self, context: OutputContext, df: pd.DataFrame) -> None:
        context.log.info(f"Materializing {len(df)} rows.")
        engine = create_engine(os.getenv("ELT_DATABASE_CONN_STRING"))

        dtype = {'species_id':Integer(),'guid':String(), 'scientific_name':String(), 'common_name': String(), 'family':String(), 'plant_type': String(),
                 'plant_origin': String(), 'light_requirement': String(), 'wind_tolerance': String(), 'growth_rate': String(), 'frost_resistant': String(),
                 'is_evergreen': Boolean(),  'is_native': Boolean, 'plant_height': String(), 'plant_width': String(), 'plant_size': String(), 'occurrence_by_state': JSONB(),
                 'flower_color': JSONB(), 'flowering_month': JSONB(), 'climate_zone': JSONB(), 'is_introduced_act': Boolean(), 'is_introduced_tas': Boolean(),
                 'is_introduced_wa': Boolean(), 'is_introduced_vic': Boolean(), 'is_introduced_qld': Boolean(), 'is_introduced_nsw': Boolean(),
                 'is_introduced_sa': Boolean(), 'is_introduced_nt': Boolean(), "image":JSONB(), '_run_id':String(), '_dwh_processed_change_dtm':DateTime()}

        table_name = context.asset_key.path[0]
        df.to_sql(name=table_name ,schema='public', con=engine, if_exists='replace', index=False, dtype=dtype)

    def load_input(self, context: "InputContext") -> pd.DataFrame:
        table_name = context.upstream_output.asset_key.path[-1]
        df = pd.read_sql(f"SELECT * FROM {table_name}", con=os.getenv("ELT_DATABASE_CONN_STRING"))
        context.log.info(f"Read {len(df)} rows.")
        return df
    
@io_manager()
def cultivatable_plant_io_manager(context):
    return Cultivatable_Plant_IOManager()
