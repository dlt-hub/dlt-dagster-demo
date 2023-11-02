from dagster import Definitions, EnvVar

from .assets import dlt_assets
from .resources import DltResource

defs = Definitions(
    assets=dlt_assets,
    resources={
        "pipeline": DltResource(
            pipeline_name = "mongo",
            dataset_name = "dagster_mongo",
            destination = "bigquery"
        ),
    }
)
