from sqlalchemy import Table, Integer, DECIMAL, DateTime, Boolean, Text
from dateutil.parser import parse

def validate_table_columns(serialized_dict:dict,table_obj:Table):
    existing_columns = list(table_obj.columns.keys())
    missing_columns = []
    new_columns = list(serialized_dict.keys())
    for column in new_columns:
        if column not in existing_columns:
            missing_columns.append(column)
    if len(missing_columns) > 0:
        print(f"These are the missing columns:\n{missing_columns}",flush=True)
        return missing_columns
    else:
        print(f"These are no missing columns",flush=True)
        return False

class ValidateDataType:
    python_data_types:list=[int,str,float]

    @staticmethod
    def validate_date_type(input):
        try:
            parse(input)
            return True
        except:
            return False
        
    @staticmethod
    def validate_bool_type(input):
        validation_list = [True,False,"true","false","True","False","TRUE","FALSE"]
        if input not in [0,1] and input in validation_list:
            return True
        else:
            return False
        
    @classmethod
    def validate_data_type(cls,input):
        result = None
        while result is None:
            date_validation = cls.validate_date_type(input)
            bool_validation = cls.validate_bool_type(input)
            if input is not None:
                if date_validation is False:
                    if bool_validation is False:
                        for data_type in ValidateDataType.python_data_types:
                            data_check = isinstance(input,data_type)
                            if data_check is True:
                                result = str(data_type.__name__)
                            else:
                                pass
                    else:
                        result = "bool"
                else:
                    result = 'datetime'
            else:
                result = 'null'
        
        return result

class DataTypes:
    template:dict={
            "sqlalchemy":None,
            "snowflake":None
        }

    @classmethod
    def int(cls):
        cls.template.update(
            {
            "sqlalchemy":Integer,
            "snowflake":"INTEGER",
            "mysql":"INTEGER"
        }
        )
        return cls.template
    
    @classmethod
    def float(cls):
        cls.template.update(
            {
            "sqlalchemy":DECIMAL(20,2),
            "snowflake":"DECIMAL(20,2)",
            "mysql":"DECIMAL(20,2)"
        }
        )
        return cls.template
    
    @classmethod
    def datetime(cls):
        cls.template.update(
            {
            "sqlalchemy":DateTime,
            "snowflake":"DATETIME",
            "mysql":"DATETIME"
        }
        )
        return cls.template
    
    @classmethod
    def bool(cls):
        cls.template.update(
            {
            "sqlalchemy":Boolean,
            "snowflake":"BOOLEAN",
            "mysql":"BOOLEAN"
        }
        )
        return cls.template
    
    @classmethod
    def str(cls):
        cls.template.update(
            {
            "sqlalchemy":Text,
            "snowflake":"TEXT",
            "mysql":"TEXT"
        }
        )
        return cls.template
    
    @classmethod
    def null(cls):
        cls.template.update(
             {
            "sqlalchemy":Text,
            "snowflake":"TEXT",
            "mysql":"TEXT"
        }
        )
        return cls.template

def get_data_type(input_value,service_type:str):
    data_type_str = ValidateDataType.validate_data_type(input_value)
    data_type = getattr(DataTypes,data_type_str)()
    return data_type[service_type]