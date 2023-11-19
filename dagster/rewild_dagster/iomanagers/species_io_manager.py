import pandas as pd
import os

from dagster import IOManager, io_manager, InputContext, OutputContext
from sqlalchemy import create_engine
from sqlalchemy.types import String, DateTime,Integer
from dotenv import load_dotenv

load_dotenv()

class Species_IOManager(IOManager):
    def __init__(self):
        pass

    def handle_output(self, context: OutputContext, df: pd.DataFrame) -> None:
        context.log.info(f"Materializing {len(df)} rows.")
        engine = create_engine(os.getenv("ELT_DATABASE_CONN_STRING"))

        dtype = {'species_id':Integer(), 'guid':String(), 'scientific_name':String(), 'common_name':String(), 'family':String(), 'kingdom':String(), '_run_id':String(), '_dwh_processed_change_dtm':DateTime()}

        table_name = context.asset_key.path[0]
        df.to_sql(name=table_name ,schema='public', con=engine, if_exists='replace', index=False, dtype=dtype)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        table_name = context.upstream_output.asset_key.path[-1]
        df = pd.read_sql(f"SELECT * FROM {table_name}", con=os.getenv("ELT_DATABASE_CONN_STRING"))
        context.log.info(f"Read {len(df)} rows.")
        return df
    
@io_manager()
def species_io_manager(context):
    return Species_IOManager()
