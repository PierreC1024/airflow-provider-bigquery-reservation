from importlib.metadata import version

__name__ = "airflow-provider-bigquery-reservation"
__version__ = version(__name__)


def get_provider_info():
    return {
        "package-name": __name__,
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
        "versions": [__version__],
    }
