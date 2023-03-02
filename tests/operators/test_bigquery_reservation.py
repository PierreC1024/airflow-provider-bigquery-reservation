import datetime
from unittest import mock


from airflow_provider_bigquery_reservation.operators.bigquery_reservation import (
    BigQueryReservationCreateOperator,
    BigQueryReservationDeleteOperator,
    BigQueryBiEngineReservationCreateOperator,
    BigQueryBiEngineReservationDeleteOperator,
    BigQueryReservationServiceHook,
)

from google.cloud.bigquery_reservation_v1 import (
    CapacityCommitment,
    Reservation,
    Assignment,
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
        "generate_resource_id",
        return_value=RESOURCE_ID,
    )
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
        generate_resource_id_mock,
        get_conn_mock,
    ):
        ti = mock.MagicMock()
        self.operator.execute({"ti": ti, "logical_date": LOGICAL_DATE})

        generate_resource_id_mock.assert_called_once_with(
            dag_id=DAG,
            task_id=TASK_ID,
            logical_date=LOGICAL_DATE,
        )

        create_commitment_reservation_and_assignment_mock.assert_called_once_with(
            resource_id=RESOURCE_ID,
            slots=SLOTS,
            assignment_job_type=JOB_TYPE,
            commitments_duration=COMMITMENTS_DURATION,
            project_id=PROJECT_ID,
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
        + "bigquery_reservation.BigQueryReservationServiceHook.delete_all_commitments"
    )
    def test_execute_with_project(
        self,
        delete_all_commitments_mock,
        client_mock,
        get_conn_mock,
    ):
        operator = BigQueryReservationDeleteOperator(
            task_id=TASK_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
        )

        operator.execute(None)

        delete_all_commitments_mock.assert_called_once_with(
            project_id=PROJECT_ID,
            location=LOCATION,
        )


class TestBigQueryBiEngineReservationCreateOperator:
    @mock.patch("airflow.models.connection.Connection.get_connection_from_secrets")
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook"
    )
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook.create_bi_reservation"
    )
    def test_execute(self, create_bi_reservation_mock, hook_mock, get_conn_mock):
        operator = BigQueryBiEngineReservationCreateOperator(
            project_id=PROJECT_ID,
            task_id=TASK_ID,
            location=LOCATION,
            size=SIZE,
        )

        operator.execute(None)

        create_bi_reservation_mock.assert_called_once_with(
            parent=f"projects/{PROJECT_ID}/locations/{LOCATION}/biReservation",
            size=SIZE,
        )


class TestBigQueryBiEngineReservationDeleteOperatorTestCase:
    @mock.patch("airflow.models.connection.Connection.get_connection_from_secrets")
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook"
    )
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook.delete_bi_reservation"
    )
    def test_execute(self, delete_bi_reservation_mock, hook_mock, get_conn_mock):
        operator = BigQueryBiEngineReservationDeleteOperator(
            project_id=PROJECT_ID,
            task_id=TASK_ID,
            location=LOCATION,
            size=SIZE,
        )

        operator.execute(None)

        delete_bi_reservation_mock.assert_called_once_with(
            parent=f"projects/{PROJECT_ID}/locations/{LOCATION}/biReservation",
            size=SIZE,
        )
