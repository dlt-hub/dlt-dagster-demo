from dagster import ConfigurableResource 

import dlt

class DltPipeline(ConfigurableResource):

    def create_pipeline(self, pipeline_name ,dataset_name, destination, dlt_resource, table_name):

        # configure the pipeline with your destination details
        pipeline = dlt.pipeline(
        pipeline_name=pipeline_name, destination=destination, dataset_name=dataset_name
        )

        # run the pipeline with your parameters
        load_info = pipeline.run(dlt_resource, table_name=table_name)

        return load_info



