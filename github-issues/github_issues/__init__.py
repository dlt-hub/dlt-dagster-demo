from dagster import Definitions, load_assets_from_modules, define_asset_job

from .assets import issues_pipeline
from .resources import DltPipeline

all_assets = load_assets_from_modules([assets])
simple_pipeline = define_asset_job(name="simple_pipeline", selection= ['issues_pipeline'])

defs = Definitions(
    assets=all_assets,
    jobs=[simple_pipeline],
    resources={
        "pipeline": DltPipeline(),
    }
)
