"""A sample DAG to show how the BigQuery BI reservation could work.

In this DAG, you reserve 100GB in BI engine between from 8am ato 7pm each working days.
"""
import os

from airflow.decorators import dag
from airflow.sensors.time_sensor import TimeSensor
from airflow_provider_bigquery_reservation.operators.bigquery_reservation import (
    BigQueryBiEngineReservationCreateOperator,
    BigQueryBiEngineReservationDeleteOperator,
)
from pendulum import Time, datetime


start_time = Time(7, 0, 0)
end_time = Time(19, 0, 0)
bi_engine_reservation_size = 100


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=f"{start_time.minute} {start_time.hour} * * 1-5",
    default_args={
        "retries": 2,
        "location": os.getenv("GCP_PROJECT_LOCATION", "US"),
        "project_id": os.getenv("GCP_PROJECT_ID"),
        "size": bi_engine_reservation_size,
    },
    tags=["example", "bigquery", "commitment"],
    catchup=False,
)
def bi_engine_reservation_sample():
    """
    Sample workflow to show how the bigquery BI engine reservation could work.

    1. Create BI reservation
    2. Wait the precise date
    3. Delete BI reservation
    """
    create_bi_engine_reservation = BigQueryBiEngineReservationCreateOperator(
        task_id="create_bi_engine_reservation",
    )

    wait = TimeSensor(task_id="wait", target_time=end_time)

    delete_bi_engine_reservation = BigQueryBiEngineReservationDeleteOperator(
        task_id="delete_bi_engine_reservation",
    )

    create_bi_engine_reservation >> wait
    wait >> delete_bi_engine_reservation


bi_engine_reservation_sample()
