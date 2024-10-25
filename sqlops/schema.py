from sqlalchemy import MetaData, Engine, text

class LoadTableSchema:
    def __init__(self,engine:Engine) -> None:
        self.engine=engine

    def load_metadata(self):
        metadata = MetaData()
        metadata.reflect(self.engine)
        print("Metadata Loaded...",flush=True)
        return metadata

    def load_table(self,tablename):
        metadata = self.load_metadata()
        table = metadata.tables[tablename]
        print(f"{tablename} Table loaded...",flush=True)
        return table
    
    def load_all_tables(self):
        metadata = self.load_metadata()
        print(f"All Tables loaded...",flush=True)
        return metadata.tables
    
    def create_table(self,tablename):
        metadata = self.load_metadata()
        table = metadata.tables[tablename]
        table.create(self.engine)
        print(f"{tablename} created in database...",flush=True)



class DDLObject:

    @staticmethod
    def add_column(tablename:str,new_column_name:str,data_type:str):
        stmt = text(f"""ALTER TABLE {tablename} ADD COLUMN {new_column_name} {data_type}""")
        print(f"Add Columns statement is created:\n{stmt}",flush=True)
        return stmt