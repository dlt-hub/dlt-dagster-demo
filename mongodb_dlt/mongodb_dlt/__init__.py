from dagster import Definitions, load_assets_from_modules, define_asset_job

from .assets import dlt_assets, jobs


defs = Definitions(
    assets=dlt_assets,
    jobs=jobs,
)
