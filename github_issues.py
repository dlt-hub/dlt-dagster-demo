import dlt
import time
from dlt.sources.helpers import requests

@dlt.resource(write_disposition="append")
def github_issues_resource(api_secret_key=dlt.secrets.value):
    owner = 'dlt-hub'
    repo = 'dlt'
    
    url = f"https://api.github.com/repos/{owner}/{repo}/issues"
    headers = {"Accept": "application/vnd.github.raw+json"}

    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # raise exception if invalid response
        issues = response.json()
        yield issues

        if 'link' in response.headers:
            if 'rel="next"' not in response.headers['link']:
                break

            url = response.links['next']['url']  # fetch next page of stargazers
        else:
            break
        time.sleep(2)  # sleep for 2 seconds to respect rate limits


if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='github_issues', destination='bigquery', dataset_name='github_issues_data'
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(github_issues_resource())

    # pretty print the information on data that was loaded
    print(load_info)
