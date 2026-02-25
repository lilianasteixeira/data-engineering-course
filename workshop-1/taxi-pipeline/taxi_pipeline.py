"""
DLT REST source for NYC taxi data.

This source paginates the API at:
https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api

Pagination: requests pages of `page_size` (default 1000) and stops when an empty
page (empty list) is returned.

See the homework reference: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/workshops/dlt/dlt_homework.md
"""
from typing import Iterator, Dict, Any

import requests
import dlt

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


@dlt.resource(name="nyc_taxi")
def nyc_taxi_resource(base_url: str = BASE_URL, page_size: int = 1000) -> Iterator[Dict[str, Any]]:
    """Yield NYC taxi records from the paginated REST API.

    Requests are performed with query params `page` (1-based) and `page_size`.
    Stops when the API returns an empty list for a page.
    """
    page = 1
    while True:
        resp = requests.get(base_url, params={"page": page, "page_size": page_size})
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        for record in data:
            yield record
        page += 1


@dlt.source
def taxi_source() -> Dict[str, Iterator[Dict[str, Any]]]:
    """DLT source exposing the `nyc_taxi` resource."""
    # Return the resource function (not its generator) so DLT can infer the name
    # DLT also accepts a list of resource functions; return the list to avoid
    # ambiguities when creating DltResource instances from mappings.
    return [nyc_taxi_resource]


# Named pipeline as requested
taxi_pipeline = dlt.pipeline(pipeline_name="taxi_pipeline")


if __name__ == "__main__":
    # Run the pipeline locally: requires `dlt` and `requests` installed.
    # This will execute the `nyc_taxi` resource and store data according to your
    # DLT destinations (default: local).
    result = taxi_pipeline.run(taxi_source())
    print(result)
