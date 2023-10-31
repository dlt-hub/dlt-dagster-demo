from dagster import multi_asset, AssetOut, get_dagster_logger, AssetExecutionContext, MetadataValue, define_asset_job, AssetSelection, OpExecutionContext
from ..mongodb import mongodb
import dlt
import os

url = os.getenv('SOURCES__MONGODB__CONNECTION__URL')
database_collection ={
    "sample_mflix":[
        "comments",
        "embedded_movies"
    ]
}

def dlt_asset_factory(collection_list):
    multi_assets = []
    jobs = []

    for db, collection_name in database_collection.items():
        @multi_asset(
            name=db,
            group_name=db,
            outs={
            stream: AssetOut(key_prefix=[f'raw_{collection_name}'])
            for stream in collection_name}

        )
        def load_select_collection(context: OpExecutionContext):
            """Use the dlt mongodb source to reflect an entire database schema and load select tables from it.

            This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
            """

            pipeline = dlt.pipeline(
                pipeline_name="local_mongo",
                destination='bigquery',
                dataset_name="mongo_select_hint",
            )

            logger = get_dagster_logger()
            data = mongodb(url, db).with_resources(*collection_name)

            load_info = pipeline.run(data, write_disposition="replace")

            # md_content=""
            # for package in results.load_packages:
            #     for table_name, table in package.schema_update.items():
            #         for column_name, column in table["columns"].items():
            #             md_content= f"\tTable updated: {table_name}: Column changed: {column_name}: {column['data_type']}"
            return tuple([None for _ in context.selected_output_names])


        multi_assets.append(load_select_collection)

        asset_job = define_asset_job(f"{db}_assets", AssetSelection.groups(db))

        jobs.append(asset_job)

        return multi_assets, jobs

dlt_assets, jobs = dlt_asset_factory(database_collection)

# @multi_asset(
#     outs={"asset1": AssetOut(is_required=False),
#           "asset2": AssetOut(is_required=False)}
# )
# def load_collection(pipeline: DltPipeline):
#     """Use the dlt mongodb source to reflect an entire database schema and load select tables from it.

#     This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
#     """

#     database_name = 'sample_mflix'
#     collections = ['comments', 'embedded_movies']
#     pipeline_name = "local_mongo"
#     destination = 'bigquery'
#     dataset_name = "mongo_database"

#     logger = get_dagster_logger()

#     results = pipeline.load_select_collection(
#         url,
#         database_name,
#         collections,
#         pipeline_name,
#         destination,
#         dataset_name
#     )

#     logger.info(results)

#     return results
