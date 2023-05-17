from dataclasses import dataclass
import yaml
import sqlalchemy

@dataclass
class DatabaseConnector:
    def read_db_creds(self, file_path):
        with open(file_path, 'r') as f:
            creds = yaml.safe_load(f)
        return creds
    
    def init_db_engine(self, creds):
        # Init connection to RDS server
        db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = sqlalchemy.create_engine(db_url)
        return engine
    
    def list_db_tables(self, engine):
        inspector = sqlalchemy.inspect(engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, df, table_name):
        # Init connection to LOCAL server
        creds = self.read_db_creds('./db_creds.yaml')
        db_url = f"postgresql://{creds['LOCAL_USER']}:{creds['LOCAL_PASSWORD']}@{creds['LOCAL_HOST']}:{creds['LOCAL_PORT']}/{creds['LOCAL_DATABASE']}"
        engine = sqlalchemy.create_engine(db_url)
        connection = engine.connect()

        # Upload df as table_name
        try:
            df.to_sql(table_name, connection, if_exists='replace', index=False)
            print(f"Data uploaded to '{table_name}' table in the local database.")
        except sqlalchemy.exc.SQLAlchemyError as e:
            print(f"Error uploading data to database: {e}")
        finally:
            connection.close()

connector = DatabaseConnector()

# List tables in db
# creds = connector.read_db_creds('./db_creds.yaml')
# tables = connector.list_db_tables(connector.init_db_engine(creds))
# print(tables)