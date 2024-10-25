from sqlalchemy import create_engine, Engine

class SQLEngineException(Exception):
    pass

class Engine:
    def __init__(self,host:str,db:str,user:str,pswd:str) -> None:
        self.host=host
        self.db=db
        self.user=user
        self.pswd=pswd

    def get_engine(self) -> Engine:
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.pswd}@{self.host}/{self.db}")
        print("SQLalchemy Engine Initiated...",flush=True)
        return engine
    
    def execute_query(self,statement,select:bool=False,insert:bool=False,update:bool=False,delete:bool=False,ddl=False):
        engine = self.get_engine()
        print(f"Executing Query: {statement}...",flush=True)
        if insert is True or update is True or delete is True or ddl is True:
            with engine.begin() as conn:
                conn.execute(statement)
            print("Query Execution Done...",flush=True)
            return None
        elif select is True:
            with engine.begin() as conn:
                response = conn.execute(statement).fetchall()
                print("Query Execution Done...",flush=True)
                return response
        else:
            raise SQLEngineException("Please set anyone of these parameteres as True:\n-select\n-insert\n-update")