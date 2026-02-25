"""REST API pipeline for Open Library API."""

import sys
import dlt
import requests
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


def search_harry_potter_isbns() -> str:
    """
    Search for Harry Potter books on Open Library and extract ISBNs or OCLC numbers.
    
    Returns:
        Comma-separated string of ISBNs or OCLC identifiers
    """
    search_url = "https://openlibrary.org/search.json"
    params = {
        "title": "Harry Potter",
        "author": "Rowling",
        "limit": 15
    }
    
    response = requests.get(search_url, params=params)
    response.raise_for_status()
    
    data = response.json()
    identifiers = []
    
    for doc in data.get("docs", []):
        # Try ISBN first
        isbn_list = doc.get("isbn", [])
        if isbn_list:
            identifiers.append(f"ISBN:{isbn_list[0]}")
        # Fall back to OCLC if no ISBN
        elif doc.get("oclc"):
            oclc_list = doc.get("oclc", [])
            if oclc_list:
                identifiers.append(f"OCLC:{oclc_list[0]}")
        # Fall back to OpenLibrary key
        else:
            key = doc.get("key", "")
            if key:
                # Extract the book ID from the key (e.g., "/works/OL45883W" -> "OL45883W")
                book_id = key.split("/")[-1]
                identifiers.append(f"OLID:{book_id}")
    
    if not identifiers:
        raise ValueError("No Harry Potter books found")
    
    bibkeys = ",".join(identifiers[:7])  # Limit to 7 books
    print(f"Found Harry Potter books: {bibkeys}\n")
    return bibkeys


@dlt.source
def open_library_rest_api_source(bibkeys: str):
    """
    Ingest data from Open Library API.
    
    Args:
        bibkeys: Comma-separated list of ISBNs to fetch.
    
    Open Library API documentation: https://openlibrary.org/dev/docs/api/books
    """
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://openlibrary.org",
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    "path": "api/books",
                    "params": {
                        "bibkeys": bibkeys,
                        "jscmd": "details",
                        "format": "json",
                    },
                    # Open Library returns the data directly in the response
                    # Set data_selector to "$" to use the entire response
                    "data_selector": "$",
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    # Search for Harry Potter books dynamically
    bibkeys = search_harry_potter_isbns()
    
    load_info = pipeline.run(open_library_rest_api_source(bibkeys))
    print(load_info)  # noqa: T201
