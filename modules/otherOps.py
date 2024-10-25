import dotenv
from typing import List, Dict
from functools import reduce

def update_env(data:dict,env_file_path:str):
    for key,value in data.items():
        dotenv.set_key(env_file_path,key,value)
    return None

def convert_list_to_string(list_:list):
    delim = ","
    string = reduce(lambda x, y: str(x) + delim + str(y), list_)
    return string