from dagster import ConfigurableResource 

import dlt

class DltResource(ConfigurableResource):
    pipeline_name: str
    dataset_name: str
    destination: str
    table_name: str


    def create_pipeline(self, dlt_resource):

        # configure the pipeline with your destination details
        pipeline = dlt.pipeline(
        pipeline_name=self.pipeline_name, destination=self.destination, dataset_name=self.dataset_name
        )

        # run the pipeline with your parameters
        load_info = pipeline.run(dlt_resource, table_name=self.table_name)

        return load_info



