import datetime
from unittest import mock

from airflow_provider_bigquery_reservation.operators.bigquery_reservation import (
    BigQueryBiEngineReservationCreateOperator,
    BigQueryBiEngineReservationDeleteOperator,
    BigQueryReservationCreateOperator,
    BigQueryReservationDeleteOperator,
    BigQueryReservationServiceHook,
)
from google.cloud.bigquery_reservation_v1 import (
    Assignment,
    CapacityCommitment,
    Reservation,
)


# ToDo: Put all in a config file
PROJECT_ID = "test"
TASK_ID = "task"
LOCATION = "US"
SIZE = 100
SLOTS = 100
JOB_TYPE = "QUERY"
DAG = "adhoc_airflow"
COMMITMENTS_DURATION = "FLEX"
COMMITMENT = CapacityCommitment(name="commitment_test")
RESERVATION = Reservation(name="reservation_test")
ASSIGNMENT = Assignment(name="assignment_test")
RESOURCE_ID = "resource_test"
LOGICAL_DATE = datetime.datetime(2023, 1, 1)


class TestBigQueryReservationCreateOperator:
    def setup_method(self):
        self.operator = BigQueryReservationCreateOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            location=LOCATION,
            slots_provisioning=SLOTS,
        )

    @mock.patch("airflow.models.connection.Connection.get_connection_from_secrets")
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "create_commitment_reservation_and_assignment",
    )
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "_get_commitment",
        return_value=COMMITMENT,
    )
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "_get_reservation",
        return_value=RESERVATION,
    )
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "_get_assignment",
        return_value=ASSIGNMENT,
    )
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook"
    )
    def test_execute(
        self,
        hook_mock,
        assignment_mock,
        reservation_mock,
        commitment_mock,
        create_commitment_reservation_and_assignment_mock,
        get_conn_mock,
    ):
        ti = mock.MagicMock()
        self.operator.execute({"ti": ti, "logical_date": LOGICAL_DATE})

        create_commitment_reservation_and_assignment_mock.assert_called_once_with(
            slots=SLOTS,
            assignment_job_type=JOB_TYPE,
            commitments_duration=COMMITMENTS_DURATION,
            project_id=PROJECT_ID,
            reservation_project_id=None,
        )

        ti.xcom_push.assert_has_calls(
            [
                mock.call(key="commitment_name", value=COMMITMENT.name),
                mock.call(key="reservation_name", value=RESERVATION.name),
                mock.call(key="assignment_name", value=ASSIGNMENT.name),
            ]
        )

    @mock.patch("airflow.models.baseoperator.BaseOperator.on_kill")
    def test_on_kill_hook_none(self, on_kill_mock):
        assert self.operator.on_kill() is None

    @mock.patch("airflow.models.baseoperator.BaseOperator.on_kill")
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook.delete_commitment_reservation_and_assignment"
    )
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook"
    )
    def test_on_kill_hook(
        self,
        hook_mock,
        delete_commitment_reservation_and_assignment_mock,
        on_kill_mock,
    ):
        hook_mock.commitment = COMMITMENT
        hook_mock.reservation = RESERVATION
        hook_mock.assignment = ASSIGNMENT

        operator = self.operator
        operator.hook = hook_mock

        operator.on_kill()

        delete_commitment_reservation_and_assignment_mock.assert_called_once_with(
            commitment_name=COMMITMENT.name,
            reservation_name=RESERVATION.name,
            assignment_name=ASSIGNMENT.name,
            slots=SLOTS,
        )


class TestBigQueryReservationDeleteOperator:
    @mock.patch("airflow.models.connection.Connection.get_connection_from_secrets")
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook"
    )
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook.delete_commitment_reservation_and_assignment"
    )
    def test_execute_with_resources_name(
        self,
        delete_commitment_reservation_and_assignment_mock,
        client_mock,
        get_conn_mock,
    ):
        operator = BigQueryReservationDeleteOperator(
            task_id=TASK_ID,
            location=LOCATION,
            slots_provisioning=SLOTS,
            commitment_name=COMMITMENT.name,
            reservation_name=RESERVATION.name,
            assignment_name=ASSIGNMENT.name,
        )

        operator.execute(None)

        delete_commitment_reservation_and_assignment_mock.assert_called_once_with(
            commitment_name=COMMITMENT.name,
            reservation_name=RESERVATION.name,
            assignment_name=ASSIGNMENT.name,
            slots=SLOTS,
        )

    @mock.patch("airflow.models.connection.Connection.get_connection_from_secrets")
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook"
    )
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook.delete_commitments_assignment_associated"
    )
    def test_execute_with_project(
        self,
        delete_commitments_assignment_associated_mock,
        client_mock,
        get_conn_mock,
    ):
        operator = BigQueryReservationDeleteOperator(
            task_id=TASK_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
        )

        operator.execute(None)

        delete_commitments_assignment_associated_mock.assert_called_once_with(
            project_id=PROJECT_ID,
            location=LOCATION,
            reservation_project_id=PROJECT_ID,
        )


class TestBigQueryBiEngineReservationCreateOperator:
    @mock.patch(
        "airflow_provider_bigquery_reservation.operators.bigquery_reservation.BigQueryReservationServiceHook"
    )
    def test_execute(self, hook_mock):
        operator = BigQueryBiEngineReservationCreateOperator(
            project_id=PROJECT_ID,
            task_id=TASK_ID,
            location=LOCATION,
            size=SIZE,
        )

        operator.execute(None)

        hook_mock.return_value.create_bi_reservation.assert_called_once_with(
            project_id=PROJECT_ID,
            size=SIZE,
        )


class TestBigQueryBiEngineReservationDeleteOperator:
    @mock.patch(
        "airflow_provider_bigquery_reservation.operators.bigquery_reservation.BigQueryReservationServiceHook"
    )
    def test_execute(self, hook_mock):
        operator = BigQueryBiEngineReservationDeleteOperator(
            project_id=PROJECT_ID,
            task_id=TASK_ID,
            location=LOCATION,
            size=SIZE,
        )

        operator.execute(None)

        hook_mock.return_value.delete_bi_reservation.assert_called_once_with(
            project_id=PROJECT_ID,
            size=SIZE,
        )
