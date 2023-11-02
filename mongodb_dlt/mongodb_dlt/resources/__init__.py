from dagster import ConfigurableResource 

import dlt

class DltResource(ConfigurableResource):
    pipeline_name: str
    dataset_name: str
    destination: str

    def load_collection(self, resource_data, database):

        # configure the pipeline with your destination details
        pipeline = dlt.pipeline(
        pipeline_name=f"{database}_{self.pipeline_name}", destination=self.destination, dataset_name=f"{self.dataset_name}_{database}"
        )

        load_info = pipeline.run(resource_data, write_disposition="replace")

        return load_info