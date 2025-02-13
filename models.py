import os
import traceback
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from urllib.parse import quote_plus
from time import perf_counter
from sqlalchemy.exc import SQLAlchemyError
import numpy as np
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String, DateTime, text, Boolean, ForeignKey,func, TIMESTAMP,BigInteger, JSON
import json
from sqlalchemy.orm import sessionmaker

load_dotenv()

Base = declarative_base()

db = SQLAlchemy()
    
class RawDatabase:
    """
    This class have the methods to connect and comunicate with DB. 
    """
    def __init__(self,database_uris=None, db_name = None, db_conn = None):
        """
        Connecting with DB based on input.
        """
        if database_uris:
            self.database_uris = database_uris
        
        elif db_name:
            self.database_uris = self.create_database_uri(os.getenv(f'{name.upper()}_USER'),
                                                                    os.getenv(f'{name.upper()}_PASSWORD'),
                                                                    os.getenv(f'{name.upper()}_HOST'),
                                                                    os.getenv(f'{name.upper()}_port'),
                                                                    os.getenv(f'{name.upper()}_DB'))
        elif db_conn:
            self.database_uris = self.create_database_uri(os.getenv(f'_USER'),
                                                                    os.getenv(f'_PASSWORD'),
                                                                    os.getenv(f'_HOST'),
                                                                    os.getenv(f'_port'),
                                                                    os.getenv(f'{db_conn.upper()}_DB'))

        else:
            self.database_uris = self.create_database_uri(os.getenv(f'_USER'),
                                                                    os.getenv(f'_PASSWORD'),
                                                                    os.getenv(f'_HOST'),
                                                                    os.getenv(f'_port'),
                                                                    os.getenv(f'_DB'))
            
            
    def create_database_uri(self, user, password, host, port, database):
        return f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}"

    def get_db_engine(self, source=None):
        try:
            database_uri = self.database_uris
            engine = create_engine(database_uri)
            return engine
        except Exception as e:
            print(traceback.format_exc(), str(e))
    

    # used for running base query (insert,update) 
    def execute_query(self, query, **kwargs):
        try:
            params = kwargs.get("params", {})
            source = kwargs.get("source", None)

            query = query.replace("%(", ":").replace(")s", "")

            engine = self.get_db_engine(source)

            with engine.connect() as connection:
                result = connection.execute(text(query), params)
            return result, 200
        except Exception as e:
            print(traceback.format_exc(), str(e))
            return dict(), 400


    # used for getting data from database (return: dataframe)
    def extract_data(self, query, **kwargs):
        try:
            result, status = self.execute_query(query, **kwargs)
            df = pd.DataFrame(result)
            if len(df) == 0:
                return pd.DataFrame(columns=result.keys()), status
        
            df.columns = result.keys()
            return df, status
        except Exception as e:
            print(traceback.format_exc(), str(e))
            return pd.DataFrame(), status

    def _clean_data(self, data):
        cleaned_data = data.where(pd.notnull(data), None)
        cleaned_data = cleaned_data.replace({np.nan: None})
        cleaned_data = cleaned_data.replace({'NaN': None})

        normalised_cloumns = [''.join(e for e in i if e.isalnum()) for i in cleaned_data.columns.to_list()]
        cleaned_data.columns = normalised_cloumns
        return cleaned_data
    
    def insert_data(self, data=None, table_name=None, primary_key=None, batch_size=1000):
        try:
            engine = self.get_db_engine()
            all_columns = data.columns.tolist()
            normalised_cloumns = [''.join(e for e in i if e.isalnum()) for i in all_columns]

            # Preparing the SQL query with named parameters for bulk insert
            columns_placeholder = ", ".join([f":{col}" for col in normalised_cloumns])
            updateString = ",".join([f"`{i}` = values(`{i}`)" for i in all_columns])
            updateString = updateString+ ",`record_updated` = current_timestamp()"

            sql_query = f"""
                INSERT INTO {table_name} ({', '.join(["`"+col+"`" for col in all_columns])})
                VALUES ({columns_placeholder})
                ON DUPLICATE KEY UPDATE {updateString};
            """

            # cleaning null datas and renaming headers
            cleaned_data = self._clean_data(data)

            def chunker(seq, size):
                return (seq[pos:pos + size] for pos in range(0, len(seq), size))
        
            # Convert DataFrame to a list of dictionaries
            data_dicts = cleaned_data.to_dict('records')

            t1_start = perf_counter()
            with engine.begin() as conn:
                # Process data in batches
                for batch in chunker(data_dicts, batch_size):
                    # SQLAlchemy executes queries using transactions, so this is already optimized
                    result = conn.execute(text(sql_query), batch)
            duration = perf_counter() - t1_start
            return [result.rowcount, duration]
        except SQLAlchemyError as se:
            print(str(se))
            error_code = se.orig.args[0]
            error_message = se.orig.args[1]

            if error_code == 1146 and "doesn't exist" in error_message:
                data['record_inserted'] = datetime.now()
                data['record_updated'] = datetime.now()
                data.to_sql(table_name, con=engine, if_exists='append', index=False)
            else:
                pass
        except Exception as e:
            print(traceback.format_exc(), str(e))
            return [0,0]

    def insert_df_table(self, df, table_name, primary_key=None):
        try:
            engine = self.get_db_engine()
        
            # Replace NaN with None to handle NULL values in the database
            df = df.where(pd.notnull(df), None)
            df = df.applymap(lambda x: None if pd.isna(x) else x)

            for col in df.select_dtypes(include=['datetime64[ns]']).columns:
                df[col] = df[col].apply(lambda x: None if pd.isna(x) or x == pd.Timestamp.min or x == pd.Timestamp('1970-01-01 00:00:00')  else x)

            column_names = df.columns.tolist()

            data_dict = df.to_dict(orient='records')

            for i in range(len(data_dict)):
                for j in data_dict[i].keys():
                    if pd.isna(data_dict[i][j]):

                        data_dict[i][j] = None
        
            # Prepare the insert statement with parameter placeholders
            insert_stmt = f"INSERT INTO {table_name} ("
            insert_stmt += ", ".join([f"`{col}`" for col in column_names]) + ") VALUES "
            insert_stmt += "(" + ", ".join([":{}".format(col) for col in column_names]) + ")"
            
            if primary_key:
                insert_stmt += """ ON DUPLICATE KEY UPDATE """
                update_stmt = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in column_names if col != primary_key])
                update_stmt += ", `record_updated` = current_timestamp()"
                insert_stmt += update_stmt
            
            insert_stmt += ';'
            

            # print('insert_stmt: ', insert_stmt)
            with engine.connect() as conn:
                conn.execute(text(insert_stmt), data_dict)
                conn.commit() 
                
        except Exception as e:
            print(f"Error during upsert operation: {e}")
            raise