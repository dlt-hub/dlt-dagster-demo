from dagster import Definitions, load_assets_from_modules, define_asset_job

from .assets import load_mongo_database

all_assets = load_assets_from_modules([assets])
mongo_pipeline = define_asset_job(name="mongo_pipeline", selection= ['load_mongo_database'])

defs = Definitions(
    assets=all_assets,
    jobs=[mongo_pipeline],
)
