#!/usr/bin/env python
# coding: utf-8

# In[6]:


from dagster import job, op,DagsterInstance,reconstructable
from dagster import resource
from dagster_pandas import DataFrame

import pandas as pd
from sqlalchemy import create_engine

# Define PostgreSQL connection string
POSTGRES_DB = 'postgres'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'system'
POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5432'

POSTGRES_URI = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'

# Extract: Define a op to extract data from PostgreSQL
@op(required_resource_keys={'postgres'})
def extract_from_postgres(context) -> pd.DataFrame:
    engine = create_engine(POSTGRES_URI)
    query = "SELECT * FROM whole_data;"
    return pd.read_sql(query, engine)

# Transform: Define a op to transform the data
@op
def transform_data(context, dataframe: pd.DataFrame) -> pd.DataFrame:
    # Drop columns 'column1' and 'column2'
    dataframe = dataframe.drop(columns=['belongs_to_collection', 'genres','homepage','overview','spoken_languages','tagline','Keywords','cast','crew'])
    
    # Perform other transformations if needed
    # dataframe['new_column'] = some_calculation(dataframe['other_column'])

    return dataframe

# Load: Define a solid to load the data back to PostgreSQL
@op(required_resource_keys={'postgres'})
def load_to_postgres(context, dataframe: pd.DataFrame):
    engine = create_engine(POSTGRES_URI)
    dataframe.to_sql('transformed_data', engine, if_exists='replace', index=False)

# Define the PostgreSQL resource
#postgres_resource = resource(resource_fn=lambda _: create_engine(POSTGRES_URI))
@resource
def postgres_resource(_):
    return create_engine(POSTGRES_URI)

# Define the Dagster pipeline
#@job(mode_defs=[resource(resource_defs={'postgres': postgres_resource})])
#def etl_pipeline():
#    data = extract_from_postgres()
#    transformed_data = transform_data(data)
#    load_to_postgres(transformed_data)
#@job(mode_defs=[ModeDefinition(resource_defs={'postgres': postgres_resource})])
#def etl_pipeline():
#    data = extract_from_postgres()
#    transformed_data = transform_data(data)
#    load_to_postgres(transformed_data)
    
@job(resource_defs={'postgres': postgres_resource})
def etl_pipeline():
    data = extract_from_postgres()
    transformed_data = transform_data(data)
    load_to_postgres(transformed_data)
    
# Run the pipeline using dagit or programmatically
if __name__ == "__main__":
    from dagster import execute_job
    instance = DagsterInstance.get()  
    recon_job = reconstructable(etl_pipeline)  # Create a ReconstructableJob
    rresult = execute_job(recon_job, instance=instance)


# In[ ]:




