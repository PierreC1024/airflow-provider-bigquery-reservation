from importlib.metadata import version


def get_provider_info():
    """Get provider information for Airflow."""
    return {
        "package-name": "airflow-provider-bigquery-reservation",
        "name": "Apache Airflow BigQuery Reservation Provider",
        "description": "Airflow Provider to buy reservation in BigQuery",
        "connection-types": [
            {
                "connection-type": "gcp_bigquery_reservation",
                "hook-class-name": "airflow_provider_bigquery_reservation.hooks.bigquery_reservation.BigQueryReservationServiceHook",
            }
        ],
        "extra-links": [
            "airflow_provider_bigquery_reservation.operators.bigquery_reservation.BigQueryReservationCreateOperator",
            "airflow_provider_bigquery_reservation.operators.bigquery_reservation.BigQueryReservationDeleteOperator",
            "airflow_provider_bigquery_reservation.operators.bigquery_reservation.BigQueryBiEngineReservationCreateOperator",
            "airflow_provider_bigquery_reservation.operators.bigquery_reservation.BigQueryBiEngineReservationDeleteOperator",
        ],
        "versions": [version("airflow_provider_bigquery_reservation")],
    }  # pragma: no cover
