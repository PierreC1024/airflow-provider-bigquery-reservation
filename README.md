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

> **Warning**
> This package is a pre-released of the official apache-airflow-providers-google package. All of these operators will be integrated to the official package, soon.

This repository provides an Apache Airflow provider based on [BigQuery Reservation API](https://cloud.google.com/python/docs/reference/bigqueryreservation/latest).

## Airflow Operators
* `BigQueryReservationCreateOperator`: Buy BigQuery slots (commitments) and assign them to a GCP project (reserve and assign).
* `BigQueryReservationDeleteOperator`: Delete BigQuery commitments and remove associated ressources (rservation and assignment).
* `BigQueryBiEngineReservationCreateOperator`: Create or Update a BI engine reservation.
* `BigQueryBiEngineReservationDeleteOperator`: Delete or Update a BI engine reservation.

You could find DAG samples [here](https://github.com/PierreC1024/airflow-provider-bigquery-reservation/tree/main/airflow_provider_bigquery_reservation/example_dags).

### Requirements

* A [Google Cloud connection](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html) has to be defined.
By default, all hooks and operators use `google_cloud_default`.
* This connection requires the following roles on the Google Cloud project(s) used in these operators:
  * [BigQuery Resource Admin](https://cloud.google.com/iam/docs/understanding-roles#bigquery.resourceAdmin)
  * [BigQuery Job User](https://cloud.google.com/iam/docs/understanding-roles#bigquery.jobUser) - *Required for `BigQueryReservationCreateOperator` because of the reservation attachment check.*

*Defining a new dedicated connection and custom GCP role could be good practices to respect the principle of least privilege.*

## How to install

```bash
pip install --user airflow-provider-bigquery-reservation
```
