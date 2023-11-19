import pandas as pd
import os

from dagster import IOManager, io_manager, InputContext, OutputContext
from sqlalchemy import create_engine
from sqlalchemy.types import String, Integer, DateTime,Boolean
from dotenv import load_dotenv

load_dotenv()

class Non_Native_Species_IOManager(IOManager):
    def __init__(self):
        pass

    def handle_output(self, context: OutputContext, df: pd.DataFrame) -> None:
        context.log.info(f"Materializing {len(df)} rows.")
        engine = create_engine(os.getenv("ELT_DATABASE_CONN_STRING"))

        dtype = {'guid':String(),'is_introduced_act':Boolean(), 'is_introduced_tas':Boolean(), 'is_introduced_wa':Boolean(), 'is_introduced_vic':Boolean(), 
                 'is_introduced_qld':Boolean(), 'is_introduced_nsw':Boolean(), 'is_introduced_sa':Boolean(), 'is_introduced_nt':Boolean(),
                 '_run_id':String(), '_dwh_processed_change_dtm':DateTime()}

        table_name = context.asset_key.path[0]
        df.to_sql(name=table_name ,schema='public', con=engine, if_exists='replace', index=False, dtype=dtype)

    def load_input(self, context: "InputContext") -> pd.DataFrame:
        table_name = context.upstream_output.asset_key.path[-1]
        df = pd.read_sql(f"SELECT * FROM {table_name}", con=os.getenv("ELT_DATABASE_CONN_STRING"))
        context.log.info(f"Read {len(df)} rows.")
        return df
    
@io_manager()
def non_native_species_io_manager(context):
    return Non_Native_Species_IOManager()
