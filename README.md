<p align="center">
  <a href="https://www.airflow.apache.org">
    <img alt="Airflow" src="https://cwiki.apache.org/confluence/download/attachments/145723561/airflow_transparent.png?api=v2" width="60" />
  </a>
  <a href="https://cloud.google.com/bigquery/docs">
    <img alt="BigQuery Pricing" src="https://storage.googleapis.com/data-fullstack-utils-public/logo_bq_pricing_bg.png" width="80" />
  </a>
</p>
<h1 align="center">
  Airflow BigQuery Reservation Provider
</h1>
<br/>

[![PyPI version](https://badge.fury.io/py/airflow-provider-bigquery-reservation.svg)](https://badge.fury.io/py/airflow-provider-bigquery-reservation)
[![codecov](https://codecov.io/gh/PierreC1024/airflow-provider-bigquery-reservation/branch/main/graph/badge.svg?token=VQ18VBAGNO)](https://codecov.io/gh/PierreC1024/airflow-provider-bigquery-reservation)
![Github Action Test](https://github.com/PierreC1024/airflow-provider-bigquery-reservation/actions/workflows/test.yaml/badge.svg)

This repository provides an Airflow provider based on BigQuery reservation API.

Operators available:
* `BigQueryReservationCreateOperator`: Buy BigQuery slots (commitment) and assign them to a GCP project (reserve and assign).
* `BigQueryReservationDeleteOperator`: Delete BigQuery reservation and remove associated ressources.
* `BigQueryBiEngineReservationCreateOperator`: Create or Update BI engine reservation.
* `BigQueryBiEngineReservationDeleteOperator`: Delete or Update BI engine reservation.

You could find DAG sample [here](https://github.com/PierreC1024/airflow-provider-bigquery-reservation/tree/main/airflow_provider_bigquery_reservation/example_dags).

## How to install

```bash
pip install --user airflow-provider-bigquery-reservation
```
