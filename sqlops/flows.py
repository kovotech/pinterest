import sys
import os
from dotenv import load_dotenv
load_dotenv()
sys.path.append(os.environ['PROJECT_PATH_SERVER'])
from sqlops.validator import get_data_type, validate_table_columns
from sqlops.schema import LoadTableSchema, DDLObject
from sqlops.engine import Engine

def add_missing_column(serialized_dict:dict,tablename:str,sql_engine:Engine,service_type:str):
    """This function created the missing column in mentioned tablename parameter with sql_engine(sqlalchemy engine)"""
    print(f"================================================================================",flush=True)
    conn = sql_engine.get_engine()
    table_obj = LoadTableSchema(conn).load_table(tablename)
    missing_columns = validate_table_columns(serialized_dict,table_obj)
    try:
        for column in missing_columns:
            stmt = DDLObject.add_column(tablename,column,get_data_type(serialized_dict[column],service_type))
            sql_engine.execute_query(stmt,ddl=True)
            print("----------------------------------------------------------------------------------",flush=True)
        return None
    except:
        pass