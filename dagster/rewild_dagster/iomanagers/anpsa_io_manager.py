import pandas as pd
import os

from dagster import IOManager, io_manager, InputContext, OutputContext
from sqlalchemy import create_engine
from sqlalchemy.types import String, Integer, DateTime,DECIMAL
from sqlalchemy.dialects.postgresql import JSONB
from rewild_dagster.util import column_array_split

class Anpsa_IOManager(IOManager):
    def __init__(self):
        pass

    def handle_output(self, context: OutputContext, df: pd.DataFrame) -> None:
        context.log.info(f"Materializing {len(df)} rows.")
        engine = create_engine(os.getenv("ELT_DATABASE_CONN_STRING"))

        dtype = {'guid':String(),'occurrence_by_state':JSONB(), 'flower_color':JSONB(), 'flowering_season':JSONB(), 'plant_size':String(),'_run_id':String(), '_dwh_processed_change_dtm':DateTime()}

        table_name = context.asset_key.path[0]
        df.to_sql(name=table_name ,schema='public', con=engine, if_exists='replace', index=False, dtype=dtype)

    def load_input(self, context: "InputContext") -> pd.DataFrame:
        table_name = context.upstream_output.asset_key.path[-1]
        df = pd.read_sql(f"SELECT * FROM {table_name}", con=os.getenv("ELT_DATABASE_CONN_STRING"))
        context.log.info(f"Read {len(df)} rows.")
        return df
    
@io_manager()
def anpsa_io_manager(context):
    env = os.getenv("ELT_ENV").lower()
    if env == "dev":
        return Mock_Anpsa_IOManager()
    else:
        return Anpsa_IOManager()


class Mock_Anpsa_IOManager(Anpsa_IOManager):
    def __init__(self):
        super().__init__()

    def load_input(self, context: "InputContext") -> pd.DataFrame:
        data = {"guid":["https://id.biodiversity.org.au/node/apni/2886064","https://id.biodiversity.org.au/node/apni/2886197","https://id.biodiversity.org.au/node/apni/2886206"],
                "occurrence_by_state": ["New South Wales, Queensland", "New South Wales, Queensland", "Western Australia"],
                "flower_color": ["Green", "Black, Green, Pink, Purple, Red, White, Yellow", "Orange, Red"],
                "flowering_season":["Autumn, Summer", "Autumn, Spring, Summer, Winter", ""],
                "plant_size":["Tree", "Medium", "Small"]}
        
        df = pd.DataFrame(data)

        df['occurrence_by_state'] = df['occurrence_by_state'].apply(column_array_split)
        df['flower_color'] = df['flower_color'].apply(column_array_split)
        df['flowering_season'] = df['flowering_season'].apply(column_array_split)

        context.log.info(f"Read {len(df)} rows.")
        return df



