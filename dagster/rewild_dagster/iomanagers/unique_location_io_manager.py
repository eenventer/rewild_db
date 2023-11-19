import pandas as pd
import os

from dagster import IOManager, io_manager, InputContext, OutputContext
from sqlalchemy import create_engine
from sqlalchemy.types import String, Integer, DateTime,DECIMAL
from dotenv import load_dotenv

load_dotenv()

class Unique_Location_IOManager(IOManager):
    def __init__(self):
        pass

    def handle_output(self, context: OutputContext, df: pd.DataFrame) -> None:
        context.log.info(f"Materializing {len(df)} rows.")
        engine = create_engine(os.getenv("ELT_DATABASE_CONN_STRING"))

        dtype = {'location_id':String(), 'postcode':Integer(),'suburb':String(), 'state':String(), 'lat':DECIMAL(10,3), 'long':DECIMAL(10,3), '_run_id':String(), '_dwh_processed_change_dtm':DateTime()}

        table_name = context.asset_key.path[0]
        df.to_sql(name=table_name ,schema='public', con=engine, if_exists='replace', index=False, dtype=dtype)

    def load_input(self, context: "InputContext") -> pd.DataFrame:
        table_name = context.upstream_output.asset_key.path[-1]
        df = pd.read_sql(f"SELECT * FROM {table_name}", con=os.getenv("ELT_DATABASE_CONN_STRING"))
        context.log.info(f"Read {len(df)} rows.")
        return df

class Mock_Unique_Location_IOManager(Unique_Location_IOManager):
    def __init__(self):
        super().__init__()

    def load_input(self, context: "InputContext") -> pd.DataFrame:
        data = {"location_id":["6a3fc99f8d564735de2fdaecea482b8b"],
                "postcode": [200],
                "state": ["ACT"],
                "lat":[-35.280],
                "long":[149.120]}
        
        df = pd.DataFrame(data)
        context.log.info(f"Read {len(df)} rows.")
        return df


@io_manager()
def unqiue_location_io_manager(context):
    env = os.getenv("ELT_ENV").lower()
    if env == "dev":
        return Mock_Unique_Location_IOManager()
    else:
        return Unique_Location_IOManager()
