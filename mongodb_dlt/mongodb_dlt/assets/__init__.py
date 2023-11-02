from dagster import multi_asset, AssetOut, get_dagster_logger, AssetExecutionContext, MetadataValue, define_asset_job, AssetSelection, OpExecutionContext
from ..mongodb import mongodb
from ..resources import DltResource

import dlt
import os

URL = os.getenv('SOURCES__MONGODB__CONNECTION__URL')

DATABASE_COLLECTIONS = {
    "sample_mflix": [
        "comments",
        "embedded_movies",
    ],
}


def dlt_asset_factory(collection_list):
    multi_assets = []

    for db, collection_name in collection_list.items():
        @multi_asset(
            name=db,
            group_name=db,
            outs={
                stream: AssetOut(key_prefix=[f'raw_{db}'])
                for stream in collection_name}

        )
        def collections_asset(context: OpExecutionContext, pipeline: DltResource):

            # Getting Data From MongoDB    
            data = mongodb(URL, db).with_resources(*collection_name)

            logger = get_dagster_logger()
            results = pipeline.load_collection(data, db)
            logger.info(results)

            return tuple([None for _ in context.selected_output_names])

        multi_assets.append(collections_asset)

    return multi_assets


dlt_assets = dlt_asset_factory(DATABASE_COLLECTIONS)
