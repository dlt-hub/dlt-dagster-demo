from dagster import asset, get_dagster_logger, AssetExecutionContext, MetadataValue
from ..resources import DltResource
from ..dlt import github_issues_resource

@asset
def issues_pipeline(context: AssetExecutionContext, pipeline: DltResource):
    logger = get_dagster_logger()
    results = pipeline.create_pipeline(github_issues_resource, table_name='github_issues')
    logger.info(results)

    md_content=""
    for package in results.load_packages:
        for table_name, table in package.schema_update.items():
            for column_name, column in table["columns"].items():
                md_content= f"\tTable updated: {table_name}: Column changed: {column_name}: {column['data_type']}"

    # Attach the Markdown content as metadata to the asset
    context.add_output_metadata(metadata={"Updates": MetadataValue.md(md_content)})