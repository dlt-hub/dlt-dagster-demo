from dagster import asset, get_dagster_logger
from ..mongodb import mongodb
import dlt
import os

url = os.getenv('SOURCES__MONGODB__CONNECTION__URL')
db = os.getenv('SOURCES__MONGODB__DATABASE')

@asset
def load_entire_database():
    """Use the mongo source to completely load all collection in a database"""
    pipeline = dlt.pipeline(
        pipeline_name="local_mongo",
        destination='bigquery',
        dataset_name="mongo_database",
    )

    # By default the mongo source reflects all collections in the database
    source = mongodb(connection_url=url, database=db)
    logger = get_dagster_logger()

    # Run the pipeline. For a large db this may take a while
    load_info = pipeline.run(source, write_disposition="replace")
    logger.info(load_info)

    return load_info