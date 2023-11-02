from dagster import Definitions, load_assets_from_modules, define_asset_job

from .assets import issues_pipeline
from .resources import DltResource

all_assets = load_assets_from_modules([assets])
simple_pipeline = define_asset_job(name="simple_pipeline", selection= ['issues_pipeline'])

defs = Definitions(
    assets=all_assets,
    jobs=[simple_pipeline],
    resources={
        "pipeline": DltResource(
            pipeline_name = "github_issues",
            dataset_name = "dagster_github_issues",
            destination = "bigquery",
            table_name= "github_issues"
        ),
    }
)
