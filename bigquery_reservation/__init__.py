from importlib.metadata import version

__name__ = "airflow-provider-bigquery-commitment"
__version__ = version(__name__)

def get_provider_info():
    return {
        "package-name": __name__,  # Required
        "name": "Apache Airflow BigQuery Commitment Provider",  # Required
        "description": "Airflow Provider to buy commitment in BigQuery",  # Required
        "connection-types": [
            {
                "connection-type": "google_cloud_platform",
                "hook-class-name": "bigquery_commitment.bigquery_reservation.sample.BiqQueryReservationServiceHook",
            }
        ],
        "extra-links": [
            "bigquery_commitment.operators.bigquery_reservation.BigQueryCommitmentSlotReservationOperator",
            "bigquery_commitment.operators.bigquery_reservation.BigQueryCommitmentSlotDeletionOperator",
            ],
        "versions": [__version__],  # Required
    }
