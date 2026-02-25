# Taxi Pipeline

This folder contains a DLT REST source for NYC taxi data (`taxi_pipeline.py`).

Homework reference: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/workshops/dlt/dlt_homework.md

Quick run (from this folder):

```bash
python -m pip install --user dlt requests
python taxi_pipeline.py
```

Notes:
- The script defines `taxi_pipeline` and a `taxi_source` DLT source that paginates
  the API at `https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api`.
- Pagination uses `page` (1-based) and `page_size=1000` and stops on an empty page.
