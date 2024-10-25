from sqlalchemy import Table, Integer, Text, DECIMAL, and_, or_, true, false
from sqlalchemy.sql.expression import literal_column
from typing import Dict, List
import pandas as pd
from sqlops.engine import Engine

class SQLConditions:
    
    @staticmethod
    def and_nest_conditions(conditions:list,include:bool):
        if include is True:
            return and_(true(),*conditions)
        else:
            return and_(false(),*conditions)
    
    @staticmethod
    def or_nest_conditions(conditions:list,include:bool):
        if include is True:
            return or_(true(),*conditions)
        else:
            return or_(false(),*conditions)

    @staticmethod
    def equals_to(field:str,value):
        return literal_column(field)==value
    
    @staticmethod
    def not_equals_to(field:str,value):
        return literal_column(field)!=value
    
    @staticmethod
    def greater_than(field:str,value):
        return literal_column(field)>value
    
    @staticmethod
    def greater_than_eqauls_to(field:str,value):
        return literal_column(field)>=value
    
    @staticmethod
    def less_than(field:str,value):
        return literal_column(field)<value
    
    @staticmethod
    def less_than_eqauls_to(field:str,value):
        return literal_column(field)<=value

class SelectQuery:
    def __init__(self,table:Table) -> None:
        self.table=table
    
    def select(self,limit:int=None):
        if limit is None:
            query = self.table.select()
            return query
        else:    
            query = self.table.select().fetch(limit)
            return query

    def select_by_daterange(self,start_date:str,end_date:str,start_date_column:str,end_date_column:str):
        filter = SQLConditions()
        conditions = filter.and_nest_conditions(
                                                [filter.greater_than_eqauls_to(field=start_date_column,value=start_date),
                                                 filter.less_than_eqauls_to(field=end_date_column,value=end_date)],
                                                 True
                                            )
        query = self.table.select().where(conditions)                                            
        return query

class InsertQuery:
    def __init__(self,table:Table) -> None:
        self.table=table

    def insert_all(self,serialized_data:List[Dict]):
        query = self.table.insert().values(serialized_data)
        return query

    def insert(self,serialized_dict:dict):
        query = self.table.insert().values(serialized_dict)
        return query

class DeleteQuery:
    def __init__(self,table:Table) -> None:
        self.table=table

    def delete_by_id(self,id_column_name:str,id_value):
        query = self.table.delete().where(SQLConditions.equals_to(id_column_name,id_value))
        return query
    
class DataFrameCrudOps:
    
    @staticmethod
    def insert(df:pd.DataFrame,conn:Engine,tablename:str,if_exists:str):
        df.to_sql(tablename,con=conn.get_engine(),if_exists=if_exists,index=False)
        records_count = len(df.axes[0])
        return f"{records_count} rows imported in {tablename} table"
    
    @staticmethod
    def get_data_by_query(sqlalchemy_query,conn:Engine):
        df = pd.read_sql_query(sql=sqlalchemy_query,con=conn.get_engine())
        return df
    
    # @staticmethod
    # def get_data_by_tablename(conn:Engine,tablename:str):
