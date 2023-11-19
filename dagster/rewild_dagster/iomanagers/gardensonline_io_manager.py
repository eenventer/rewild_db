import pandas as pd
import os

from dagster import IOManager, io_manager, InputContext, OutputContext
from sqlalchemy import create_engine
from sqlalchemy.types import String, Integer, DateTime,DECIMAL, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from rewild_dagster.util import convert_to_json, column_array_split

class Gardensonline_IOManager(IOManager):
    def __init__(self):
        pass

    def handle_output(self, context: OutputContext, df: pd.DataFrame) -> None:
        context.log.info(f"Materializing {len(df)} rows.")
        engine = create_engine(os.getenv("ELT_DATABASE_CONN_STRING"))

        dtype = {'guid':String(),'plant_type':String(), 'plant_origin':String(), 'light_requirement':String(), 'wind_tolerance':String(),'growth_rate':String(),
                 'frost_resistant': String(), 'is_evergreen': Boolean(), 'is_native': Boolean(), 'plant_height': String(), 'plant_width': String(), 
                 'flower_color': JSONB(), 'flowering_month':JSONB(), 'climate_zone': JSONB(), '_run_id':String(), '_dwh_processed_change_dtm':DateTime()}

        table_name = context.asset_key.path[0]
        df.to_sql(name=table_name, schema='public', con=engine, if_exists='replace', index=False, dtype=dtype)

    def load_input(self, context: "InputContext") -> pd.DataFrame:
        table_name = context.upstream_output.asset_key.path[-1]
        df = pd.read_sql(f"SELECT * FROM {table_name}", con=os.getenv("ELT_DATABASE_CONN_STRING"))
        context.log.info(f"Read {len(df)} rows.")
        return df
    
@io_manager()
def gardensonline_io_manager(context):
    env = os.getenv("ELT_ENV").lower()
    if env == "dev":
        return Mock_Gardensonline_IOManager()
    else:
        return Gardensonline_IOManager()


class Mock_Gardensonline_IOManager(Gardensonline_IOManager):
    def __init__(self):
        super().__init__()

    def load_input(self, context: "InputContext") -> pd.DataFrame:
        data = {"guid":["https://id.biodiversity.org.au/node/apni/2900020"],
                "plant_type": ["Shrub"],
                "plant_origin": ["Act"],
                "light_requirement":["Full Sun"],
                "wind_tolerance":["Medium"],
                "growth_rate": ["Medium"],
                "frost_resistant": ["Hardy"],
                "is_evergreen":[True],
                "is_native" : [True],
                "plant_height": ["2"],
                "plant_width": ["1"],
                "flower_color": ["Pink Red White Yellow"],
                "flowering_month":["January, February, June, July, August, September, October, November, December"],
                "climate_zone":['[{"Zone": 7}, {"Zone": 8}, {"Zone": 9}]']
                }
        
        
        df = pd.DataFrame(data)
        df['flower_color'] = df['flower_color'].apply(column_array_split)
        df['flowering_month'] = df['flowering_month'].apply(column_array_split)
        df['climate_zone'] = df['climate_zone'].apply(convert_to_json)
        context.log.info(f"Read {len(df)} rows.")
        return df
