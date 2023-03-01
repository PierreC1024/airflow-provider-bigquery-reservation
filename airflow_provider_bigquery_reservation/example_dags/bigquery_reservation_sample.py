import os

from pendulum import datetime

from airflow.decorators import dag

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from airflow_provider_bigquery_reservation.operators.bigquery_reservation import (
    BigQueryReservationCreateOperator,
    BigQueryReservationDeleteOperator,
)


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    default_args={
        "retries": 2,
        "location": os.getenv("GCP_PROJECT_LOCATION", "US"),
        "project_id": os.getenv("GCP_PROJECT_ID"),
        "slots_provisioning": 100,
    },
    tags=["example", "bigquery", "commitment"],
)
def bigquery_reservation_sample():
    """
    Sample workflow to show how the bigquery reservation could work.

    1. Buy flex slot commitment and assign it to the corresponding project_id
    2. Run a query
    3. Delete commitment
    """

    create_reservation = BigQueryReservationCreateOperator(
        task_id="create_reservation",
    )

    run_sample_query = BigQueryInsertJobOperator(
        task_id="run_sample_query",
        configuration={
            "query": {
                "query": """
                    SELECT COUNT(*)
                    FROM `bigquery-public-data.wikipedia.pageviews_2023`
                    WHERE DATE(datehour) >= "2023-01-15" AND
                    DATE(datehour) <= "2023-01-30" AND
                    SEARCH(title, '`paul`')
                """,
                "useLegacySql": False,
            }
        },
    )

    delete_reservation = BigQueryReservationDeleteOperator(
        task_id="delete_reservation",
        commitment_name=create_reservation.output["commitment_name"],
        reservation_name=create_reservation.output["reservation_name"],
        assignment_name=create_reservation.output["assignment_name"],
    )

    create_reservation >> run_sample_query >> delete_reservation


bigquery_reservation_sample()
