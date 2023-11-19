import json
import pandas as pd
from dagster import AssetExecutionContext
from datetime import datetime

def add_table_metadata(context: AssetExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
    df['_run_id'] = context.run_id
    df['_dwh_processed_change_dtm'] = datetime.now()

    return df

def convert_yes_no_string_to_boolean(string):
    if string.lower() == "yes":
        return True
    elif string.lower() == "no":
        return False
    else:
        raise ValueError(f"Invalid string: {string}")

def convert_to_json(value):
    if pd.notna(value) and value.strip():  # Check if not null and not blank
        return json.loads(value)
    else:
        return None
    
def column_array_split(value):
    if pd.notna(value) and value.strip():  # Check if not null and not blank
        return value.split(",")
    else:
        return None
