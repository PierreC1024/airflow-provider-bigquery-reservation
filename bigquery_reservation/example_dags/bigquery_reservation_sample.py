import os

from pendulum import datetime

from airflow.decorators import dag

from bigquery_commitment.operators.bigquery_reservation import (
    BigQueryReservationCreateOperator,
    BigQueryReservationDeletionOperator,
)

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


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
def sample_workflow():
    """
    Sample workflow to show how the bigquery reservation could work.

    1. Buy flex slot commitment and assign it to the corresponding project_id
    2. Run a query
    3. Delete commitment
    """

    buy_flex_slot_and_assign = BigQueryReservationCreateOperator(
        task_id="buy_flex_slot_and_assign",
    )

    run_sample_query = BigQueryInsertJobOperator(
        task_id="run_sample_query",
        configuration={
            "query": {
                "query": """
                    SELECT COUNT(*)
                    FROM `bigquery-test-374822.public.wikipedia_pageviews_2023`
                    WHERE DATE(datehour) >= "2023-01-15" AND
                    DATE(datehour) <= "2023-01-30" AND
                    SEARCH(title, '`paul`')
                """,
                "useLegacySql": False,
            }
        },
        location=location,
    )

    delete_slots_commitment = BigQueryReservationDeletionOperator(
        task_id="delete_slots_commitment",
        commitment_name=buy_flex_slot_and_assign.output["commitment_name"],
        reservation_name=buy_flex_slot_and_assign.output["reservation_name"],
        assignment_name=buy_flex_slot_and_assign.output["assignment_name"],
    )

    buy_flex_slot_and_assign >> run_sample_query >> delete_slots_commitment


sample_workflow()
