import datetime
import logging
import random
import uuid
from unittest import mock

from tests.utils import QueryJob, mock_base_gcp_hook_no_default_project_id

import pytest
from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow_provider_bigquery_reservation.hooks.bigquery_reservation import (
    BigQueryReservationServiceHook,
)
from google.cloud.bigquery_reservation_v1 import (
    Assignment,
    BiReservation,
    CapacityCommitment,
    Reservation,
    ReservationServiceClient,
)
from google.protobuf import field_mask_pb2


LOGGER = logging.getLogger(__name__)
CREDENTIALS = "test-creds"
PROJECT_ID = "test-project"
LOCATION = "US"
SLOTS = 100
SLOTS_ALL = SLOTS + 100
SIZE = 100
COMMITMENT_DURATION = "FLEX"
PARENT = f"projects/{PROJECT_ID}/locations/{LOCATION}"
RESOURCE_NAME = "test"
RESOURCE_ID = "test"
JOB_TYPE = "QUERY"
STATE = "ACTIVE"
DAG_ID = "dag"
TASK_ID = "task"
LOGICAL_DATE = datetime.datetime.strptime("2023-01-01", "%Y-%m-%d")
CONSTANT_GO_TO_KO = 1073741824
SIZE_KO = SIZE * CONSTANT_GO_TO_KO
PARENT_BI_RESERVATION = f"{PARENT}/biReservation"


@pytest.fixture()
def logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARN)
    return logger


class TestBigQueryReservationHook:
    def setup_method(self):
        with mock.patch(
            "airflow_provider_bigquery_reservation.hooks."
            + "bigquery_reservation.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = BigQueryReservationServiceHook(location=LOCATION)
            self.hook.get_credentials = mock.MagicMock(return_value=CREDENTIALS)
            self.location = LOCATION

    @mock.patch("google.cloud.bigquery_reservation_v1.ReservationServiceClient")
    def test_get_client_already_exist(self, reservation_client_mock):
        expected = reservation_client_mock(
            credentials=self.hook.get_credentials(), client_info=CLIENT_INFO
        )
        self.hook._client = expected
        assert self.hook.get_client() == expected

    def test_verify_slots_conditions(self):
        valid_slots = 100 * random.randint(1, 1000)
        unvalid_slots = random.randint(0, 10) * 100 + random.randint(1, 99)

        assert self.hook._verify_slots_conditions(valid_slots) is None
        with pytest.raises(AirflowException):
            self.hook._verify_slots_conditions(unvalid_slots)

    def test_convert_gb_to_kb(self):
        value = random.randint(1, 1000)
        assert self.hook._convert_gb_to_kb(value) == value * 1073741824

    @mock.patch.object(
        uuid, "uuid4", return_value="e39dfcb7-dc5f-498d-8a89-5a871e9c4363"
    )
    def test_generate_resource_id(self, uuid_mock):
        expected = f"airflow--{DAG_ID}-{TASK_ID}--2023-01-01t00-00-00-3bfe"
        assert (
            self.hook.generate_resource_id(
                DAG_ID,
                TASK_ID,
                LOGICAL_DATE,
            )
            == expected
        )

    # Create Capacity Commitment
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook.get_client"
    )
    def test_create_capacity_commitment_success(self, client_mock):
        self.hook.create_capacity_commitment(PARENT, SLOTS, COMMITMENT_DURATION)
        client_mock.return_value.create_capacity_commitment.assert_called_once_with(
            parent=PARENT,
            capacity_commitment=CapacityCommitment(
                plan=COMMITMENT_DURATION, slot_count=SLOTS
            ),
        )

    @mock.patch.object(
        ReservationServiceClient,
        "create_capacity_commitment",
        side_effect=Exception("Test"),
    )
    def test_create_capacity_commitment_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.create_capacity_commitment(PARENT, SLOTS, COMMITMENT_DURATION)

    # List Capacity Commitments
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook.get_client"
    )
    def test_list_capacity_commitments_success(self, client_mock):
        self.hook.list_capacity_commitments(PARENT)
        client_mock.return_value.list_capacity_commitments.assert_called_once_with(
            parent=PARENT
        )

    @mock.patch.object(
        ReservationServiceClient,
        "list_capacity_commitments",
        side_effect=Exception("Test"),
    )
    def test_list_capacity_commitments_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.list_capacity_commitments(PARENT)

    # Delete Capacity Commitment
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks.bigquery_reservation."
        + "BigQueryReservationServiceHook.get_client"
    )
    @mock.patch(
        "google.api_core.retry.Retry",
    )
    def test_delete_capacity_commitment_success(self, retry_mock, client_mock):
        self.hook.delete_capacity_commitment(RESOURCE_NAME)
        client_mock.return_value.delete_capacity_commitment.assert_called_once_with(
            name=RESOURCE_NAME,
            retry=retry_mock(),
        )

    @mock.patch.object(
        ReservationServiceClient,
        "delete_capacity_commitment",
        side_effect=Exception("Test"),
    )
    def test_delete_capacity_commitment_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.delete_capacity_commitment(RESOURCE_NAME)

    # Create Reservation
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook.get_client"
    )
    def test_create_reservation_success(self, client_mock):
        self.hook.create_reservation(
            PARENT,
            RESOURCE_ID,
            SLOTS,
        )
        client_mock.return_value.create_reservation.assert_called_once_with(
            parent=PARENT,
            reservation_id=RESOURCE_ID,
            reservation=Reservation(slot_capacity=SLOTS, ignore_idle_slots=True),
        ),

    @mock.patch.object(
        ReservationServiceClient,
        "create_reservation",
        side_effect=Exception("Test"),
    )
    def test_create_reservation_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.create_reservation(
                PARENT,
                RESOURCE_ID,
                SLOTS,
            )

    # Get Reservation
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook.get_client"
    )
    def test_get_reservation_success(self, client_mock):
        expected = Reservation(name=RESOURCE_NAME)
        client_mock.return_value.get_reservation.return_value = expected

        result = self.hook.get_reservation(
            RESOURCE_NAME,
        )

        client_mock.return_value.get_reservation.assert_called_once_with(
            name=RESOURCE_NAME
        ),

        assert result == expected

    @mock.patch.object(
        ReservationServiceClient,
        "get_reservation",
        side_effect=Exception("Test"),
    )
    def test_get_reservation_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.get_reservation(RESOURCE_NAME)

    # List Reservations
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook.get_client"
    )
    def test_list_reservations_success(self, client_mock):
        self.hook.list_reservations(PARENT)
        client_mock.return_value.list_reservations.assert_called_once_with(
            parent=PARENT
        )

    @mock.patch.object(
        ReservationServiceClient,
        "list_reservations",
        side_effect=Exception("Test"),
    )
    def test_list_reservations_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.list_reservations(PARENT)

    # Update Reservation
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook.get_client"
    )
    def test_update_reservation_success(self, client_mock):
        new_reservation = Reservation(name=RESOURCE_NAME, slot_capacity=SLOTS)
        field_mask = field_mask_pb2.FieldMask(paths=["slot_capacity"])
        self.hook.update_reservation(
            RESOURCE_NAME,
            SLOTS,
        )
        client_mock.return_value.update_reservation.assert_called_once_with(
            reservation=new_reservation, update_mask=field_mask
        )

    @mock.patch.object(
        ReservationServiceClient,
        "update_reservation",
        side_effect=Exception("Test"),
    )
    def test_update_reservation_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.update_reservation(RESOURCE_NAME, SLOTS)

    # Delete Reservation
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook.get_client"
    )
    def test_delete_reservation_success(self, client_mock):
        self.hook.delete_reservation(RESOURCE_NAME)
        client_mock.return_value.delete_reservation.assert_called_once_with(
            name=RESOURCE_NAME
        )

    @mock.patch.object(
        ReservationServiceClient,
        "delete_reservation",
        side_effect=Exception("Test"),
    )
    def test_delete_reservation_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.delete_reservation(RESOURCE_NAME)

    # Create Assignment
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook.get_client"
    )
    def test_create_assignment_success(self, client_mock):
        self.hook.create_assignment(PARENT, PROJECT_ID, JOB_TYPE)
        client_mock.return_value.create_assignment.assert_called_once_with(
            parent=PARENT,
            assignment=Assignment(job_type=JOB_TYPE, assignee=f"projects/{PROJECT_ID}"),
        )

    @mock.patch.object(
        ReservationServiceClient,
        "create_assignment",
        side_effect=Exception("Test"),
    )
    def test_create_assignment_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.create_assignment(PARENT, PROJECT_ID, JOB_TYPE)

    # List Assignments
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook.get_client"
    )
    def test_list_assignments_success(self, client_mock):
        self.hook.list_assignments(PARENT)
        client_mock.return_value.list_assignments.assert_called_once_with(
            parent=PARENT,
        )

    @mock.patch.object(
        ReservationServiceClient,
        "list_assignments",
        side_effect=Exception("Test"),
    )
    def test_list_assignments_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.list_assignments(PARENT)

    # Search Assignment
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook.get_client"
    )
    def test_search_assignment_success_none_assignments(self, client_mock):
        self.hook.search_assignment(PARENT, PROJECT_ID, JOB_TYPE)
        client_mock.return_value.search_all_assignments.assert_called_once_with(
            parent=PARENT, query=f"assignee=projects/{PROJECT_ID}"
        )

    @mock.patch.object(
        ReservationServiceClient,
        "search_all_assignments",
        return_value=[
            Assignment(
                name=RESOURCE_NAME,
                assignee=PROJECT_ID,
                job_type="PIPELINE",
                state=STATE,
            ),
            Assignment(
                name=RESOURCE_NAME,
                assignee=PROJECT_ID,
                job_type=JOB_TYPE,
                state="PENDING",
            ),
            Assignment(
                name=RESOURCE_NAME,
                assignee=PROJECT_ID,
                job_type=JOB_TYPE,
                state=STATE,
            ),
        ],
    )
    def test_search_assignment_success_have_assignments(self, search_all_mock):
        expected = Assignment(
            name=RESOURCE_NAME,
            assignee=PROJECT_ID,
            job_type=JOB_TYPE,
            state=STATE,
        )
        result = self.hook.search_assignment(PARENT, PROJECT_ID, JOB_TYPE)

        assert result == expected

    @mock.patch.object(
        ReservationServiceClient,
        "search_all_assignments",
        return_value=[
            Assignment(
                name=RESOURCE_NAME,
                assignee=PROJECT_ID,
                job_type=JOB_TYPE,
                state="PENDING",
            )
        ],
    )
    def test_search_assignment_success_no_assignment(self, search_all_mock):
        result = self.hook.search_assignment(PARENT, PROJECT_ID, JOB_TYPE)
        assert result is None

    @mock.patch.object(
        ReservationServiceClient,
        "search_all_assignments",
        side_effect=Exception("Test"),
    )
    def test_search_assignment_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.search_assignment(PARENT, PROJECT_ID, JOB_TYPE)

    # Delete Assignment
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks."
        + "bigquery_reservation.BigQueryReservationServiceHook.get_client"
    )
    def test_delete_assignment_success(self, client_mock):
        self.hook.delete_assignment(RESOURCE_NAME)
        client_mock.return_value.delete_assignment.assert_called_once_with(
            name=RESOURCE_NAME,
        )

    @mock.patch.object(
        ReservationServiceClient,
        "delete_assignment",
        side_effect=Exception("Test"),
    )
    def test_delete_assignment_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.delete_assignment(RESOURCE_NAME)

    # Get BQ client
    @mock.patch("google.cloud.bigquery.Client")
    def test_get_bq_client(self, bq_client_mock):
        assert self.hook.get_bq_client() == bq_client_mock(
            credentials=self.hook.get_credentials(), client_info=CLIENT_INFO
        )

    # Is Assignment attached
    @mock.patch("google.cloud.bigquery.QueryJobConfig")
    def test_is_assignment_attached_true(self, query_job_config_mock):
        bq_client = mock.MagicMock()
        bq_client.query.return_value = QueryJob(reservation_id=True)

        dummy_query = """
            SELECT dummy
            FROM UNNEST([STRUCT(true as dummy)])
        """
        rslt = self.hook._is_assignment_attached_in_query(
            bq_client,
            PROJECT_ID,
            LOCATION,
        )
        bq_client.query.assert_called_with(
            dummy_query,
            project=PROJECT_ID,
            location=LOCATION,
            job_id_prefix="test_assignment_reservation",
            job_config=query_job_config_mock(),
        )

        assert rslt is True

    @mock.patch("google.cloud.bigquery.QueryJobConfig")
    def test_is_assignment_attached_false(self, query_job_config_mock):
        bq_client = mock.MagicMock()
        bq_client.query.return_value = QueryJob(reservation_id=False)

        dummy_query = """
            SELECT dummy
            FROM UNNEST([STRUCT(true as dummy)])
        """
        rslt = self.hook._is_assignment_attached_in_query(
            bq_client,
            PROJECT_ID,
            LOCATION,
        )
        bq_client.query.assert_called_with(
            dummy_query,
            project=PROJECT_ID,
            location=LOCATION,
            job_id_prefix="test_assignment_reservation",
            job_config=query_job_config_mock(),
        )

        assert rslt is False

    # Create BI Reservation
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks.bigquery_reservation.BigQueryReservationServiceHook.get_client"
    )
    def test_create_bi_reservation_existing_reservation_success(self, client_mock):
        requested_size_gb = SIZE
        initial_size = SIZE_KO
        expected_size = initial_size + requested_size_gb * CONSTANT_GO_TO_KO

        client_mock.return_value.get_bi_reservation.return_value = BiReservation(
            name=PARENT_BI_RESERVATION, size=initial_size
        )
        self.hook.create_bi_reservation(project_id=PROJECT_ID, size=requested_size_gb)
        client_mock.return_value.get_bi_reservation.assert_called_once_with(
            name=PARENT_BI_RESERVATION
        )
        client_mock.return_value.update_bi_reservation.assert_called_once_with(
            bi_reservation=BiReservation(name=PARENT_BI_RESERVATION, size=expected_size)
        )

    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks.bigquery_reservation.BigQueryReservationServiceHook.get_client"
    )
    def test_create_bi_reservation_none_existing_reservation_success(self, client_mock):
        requested_size_gb = SIZE
        initial_size = 0
        expected_size = initial_size + requested_size_gb * CONSTANT_GO_TO_KO

        client_mock.return_value.get_bi_reservation.return_value = BiReservation(
            name=PARENT_BI_RESERVATION, size=initial_size
        )
        self.hook.create_bi_reservation(project_id=PROJECT_ID, size=requested_size_gb)
        client_mock.return_value.get_bi_reservation.assert_called_once_with(
            name=PARENT_BI_RESERVATION
        )

        client_mock.return_value.update_bi_reservation.assert_called_once_with(
            bi_reservation=BiReservation(name=PARENT_BI_RESERVATION, size=expected_size)
        )

    @mock.patch.object(
        ReservationServiceClient, "get_bi_reservation", side_effect=Exception("Test")
    )
    def test_create_bi_reservation_get_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.create_bi_reservation(project_id=PROJECT_ID, size=SIZE)

    @mock.patch.object(ReservationServiceClient, "get_bi_reservation")
    @mock.patch.object(
        ReservationServiceClient, "update_bi_reservation", side_effect=Exception("Test")
    )
    def test_create_bi_reservation_update_failure(
        self, call_failure, get_bi_reservation_mock
    ):
        with pytest.raises(AirflowException):
            self.hook.create_bi_reservation(project_id=PROJECT_ID, size=SIZE)

    # Delete BI Reservation
    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks.bigquery_reservation.BigQueryReservationServiceHook.get_client"
    )
    def test_delete_bi_reservation_size_none_success(self, client_mock):
        initial_size = SIZE_KO
        expected_size = 0
        client_mock.return_value.get_bi_reservation.return_value = BiReservation(
            name=PARENT_BI_RESERVATION, size=initial_size
        )
        self.hook.delete_bi_reservation(project_id=PROJECT_ID)
        client_mock.return_value.get_bi_reservation.assert_called_once_with(
            name=PARENT_BI_RESERVATION
        )
        client_mock.return_value.update_bi_reservation.assert_called_once_with(
            bi_reservation=BiReservation(name=PARENT_BI_RESERVATION, size=expected_size)
        )

    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks.bigquery_reservation.BigQueryReservationServiceHook.get_client"
    )
    def test_delete_bi_reservation_size_filled_update_non_negative_success(
        self, client_mock
    ):
        initial_size = 100 * CONSTANT_GO_TO_KO
        requeted_deleted_size_gb = 50
        expected_size = initial_size - requeted_deleted_size_gb * CONSTANT_GO_TO_KO
        client_mock.return_value.get_bi_reservation.return_value = BiReservation(
            name=PARENT_BI_RESERVATION, size=initial_size
        )
        self.hook.delete_bi_reservation(
            project_id=PROJECT_ID, size=requeted_deleted_size_gb
        )
        client_mock.return_value.get_bi_reservation.assert_called_once_with(
            name=PARENT_BI_RESERVATION
        )
        client_mock.return_value.update_bi_reservation.assert_called_once_with(
            bi_reservation=BiReservation(name=PARENT_BI_RESERVATION, size=expected_size)
        )

    @mock.patch(
        "airflow_provider_bigquery_reservation.hooks.bigquery_reservation.BigQueryReservationServiceHook.get_client"
    )
    def test_delete_bi_reservation_size_filled_update_negative_success(
        self, client_mock
    ):
        initial_size = 100 * CONSTANT_GO_TO_KO
        requeted_deleted_size_gb = 200
        expected_size = 0
        client_mock.return_value.get_bi_reservation.return_value = BiReservation(
            name=PARENT_BI_RESERVATION, size=initial_size
        )
        self.hook.delete_bi_reservation(
            project_id=PROJECT_ID, size=requeted_deleted_size_gb
        )
        client_mock.return_value.get_bi_reservation.assert_called_once_with(
            name=PARENT_BI_RESERVATION
        )
        client_mock.return_value.update_bi_reservation.assert_called_once_with(
            bi_reservation=BiReservation(name=PARENT_BI_RESERVATION, size=expected_size)
        )

    @mock.patch.object(
        ReservationServiceClient, "get_bi_reservation", side_effect=Exception("Test")
    )
    def test_delete_bi_reservation_get_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.delete_bi_reservation(project_id=PROJECT_ID, size=SIZE)

    @mock.patch.object(ReservationServiceClient, "get_bi_reservation")
    @mock.patch.object(
        ReservationServiceClient, "update_bi_reservation", side_effect=Exception("Test")
    )
    def test_delete_bi_reservation_update_failure(
        self, call_failure, get_bi_reservation_mock
    ):
        with pytest.raises(AirflowException):
            self.hook.delete_bi_reservation(project_id=PROJECT_ID, size=SIZE)

    # Create Commitment Reservation And Assignment
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "create_capacity_commitment",
    )
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "search_assignment",
        return_value=Assignment(
            name=RESOURCE_NAME,
            assignee=PROJECT_ID,
            job_type=JOB_TYPE,
            state=STATE,
        ),
    )
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "get_reservation",
        return_value=Reservation(name=RESOURCE_NAME, slot_capacity=SLOTS),
    )
    @mock.patch.object(BigQueryReservationServiceHook, "update_reservation")
    def test_create_commitment_reservation_and_assignment_success_existing_assignment(
        self,
        update_reservation_mock,
        get_reservation_mock,
        search_assignment_mock,
        create_capacity_commitment_mock,
    ):
        parent = f"projects/{PROJECT_ID}/locations/{self.location}"
        new_slots = get_reservation_mock.return_value.slot_capacity + SLOTS

        self.hook.create_commitment_reservation_and_assignment(
            resource_id=RESOURCE_ID,
            slots=SLOTS,
            assignment_job_type=JOB_TYPE,
            commitments_duration=COMMITMENT_DURATION,
            project_id=PROJECT_ID,
        )

        create_capacity_commitment_mock.assert_called_once_with(
            parent=parent,
            slots=SLOTS,
            commitments_duration=COMMITMENT_DURATION,
        )

        get_reservation_mock.assert_called_once_with(name=RESOURCE_NAME)

        update_reservation_mock.assert_called_once_with(
            name=RESOURCE_NAME, slots=new_slots
        )

    @mock.patch.object(
        BigQueryReservationServiceHook,
        "create_capacity_commitment",
    )
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "search_assignment",
        return_value=None,
    )
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "create_reservation",
        return_value=Reservation(name=RESOURCE_NAME),
    )
    @mock.patch.object(BigQueryReservationServiceHook, "create_assignment")
    @mock.patch.object(BigQueryReservationServiceHook, "get_bq_client")
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "_is_assignment_attached_in_query",
        return_value=True,
    )
    def test_create_commitment_reservation_and_assignment_success_not_existing_assignment(
        self,
        _is_assignment_attached_in_query_mock,
        bq_client_mock,
        create_assignment_mock,
        create_reservation_mock,
        search_assignment_mock,
        create_capacity_commitment_mock,
    ):
        parent = f"projects/{PROJECT_ID}/locations/{self.location}"
        self.hook.reservation = Reservation(name=RESOURCE_NAME, slot_capacity=SLOTS)

        self.hook.create_commitment_reservation_and_assignment(
            resource_id=RESOURCE_ID,
            slots=SLOTS,
            assignment_job_type=JOB_TYPE,
            commitments_duration=COMMITMENT_DURATION,
            project_id=PROJECT_ID,
        )

        create_capacity_commitment_mock.assert_called_once_with(
            parent=parent,
            slots=SLOTS,
            commitments_duration=COMMITMENT_DURATION,
        )

        create_reservation_mock.assert_called_once_with(
            parent=parent, reservation_id=RESOURCE_NAME, slots=SLOTS
        )

        create_assignment_mock.assert_called_once_with(
            parent=create_reservation_mock.return_value.name,
            project_id=PROJECT_ID,
            job_type=JOB_TYPE,
        )

    @mock.patch.object(
        BigQueryReservationServiceHook,
        "create_capacity_commitment",
    )
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "search_assignment",
        return_value=None,
    )
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "create_reservation",
    )
    @mock.patch.object(BigQueryReservationServiceHook, "create_assignment")
    @mock.patch.object(BigQueryReservationServiceHook, "get_bq_client")
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "_is_assignment_attached_in_query",
        side_effect=Exception("Test"),
    )
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "delete_commitment_reservation_and_assignment",
    )
    def test_create_commitment_reservation_and_assignment_failure_query_fail(
        self,
        delete_commitment_reservation_and_assignment_mock,
        _is_assignment_attached_in_query_mock,
        bq_client_mock,
        create_assignment_mock,
        create_reservation_mock,
        search_assignment_mock,
        create_capacity_commitment_mock,
    ):
        with pytest.raises(AirflowException):
            f"projects/{PROJECT_ID}/locations/{self.location}"
            self.hook.reservation = Reservation(name=RESOURCE_NAME)
            self.hook.commitment = CapacityCommitment(name=RESOURCE_NAME)
            self.hook.assignment = Assignment(name=RESOURCE_NAME)

            self.hook.create_commitment_reservation_and_assignment(
                resource_id=RESOURCE_ID,
                slots=SLOTS,
                assignment_job_type=JOB_TYPE,
                commitments_duration=COMMITMENT_DURATION,
                project_id=PROJECT_ID,
            )

            delete_commitment_reservation_and_assignment_mock.assert_called_once_with(
                commitment_name=RESOURCE_NAME,
                reservation_name=RESOURCE_NAME,
                assignment_name=RESOURCE_NAME,
                slots=SLOTS,
            )

    @mock.patch.object(
        ReservationServiceClient,
        "create_capacity_commitment",
        side_effect=Exception("Test"),
    )
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "delete_commitment_reservation_and_assignment",
    )
    def test_create_commitment_reservation_and_assignment_failure_none_create(
        self, delete_commitment_reservation_and_assignment_mock, call_failure
    ):
        with pytest.raises(AirflowException):
            self.hook.create_commitment_reservation_and_assignment(
                RESOURCE_ID,
                SLOTS,
                JOB_TYPE,
                COMMITMENT_DURATION,
                PROJECT_ID,
            )
            delete_commitment_reservation_and_assignment_mock.assert_called_once_with(
                commitment_name=None,
                reservation_name=None,
                assignment_name=None,
                slots=SLOTS,
            )

    # Delete Commitment Reservation And assignment
    def test_delete_commitment_reservation_and_assignment_none(self, caplog):
        self.hook.delete_commitment_reservation_and_assignment(slots=SLOTS)
        assert "None BigQuery commitment to delete" in caplog.text
        assert "None BigQuery reservation to update or delete" in caplog.text

    @mock.patch.object(BigQueryReservationServiceHook, "delete_capacity_commitment")
    @mock.patch.object(BigQueryReservationServiceHook, "delete_reservation")
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "get_reservation",
        return_value=Reservation(name=RESOURCE_NAME, slot_capacity=SLOTS),
    )
    def test_delete_commitment_reservation_and_assignment_false_slots_condition_none_assignment(
        self,
        get_reservation_mock,
        delete_reservation_mock,
        delete_capacity_commitment_mock,
        caplog,
    ):
        self.hook.delete_commitment_reservation_and_assignment(
            commitment_name=RESOURCE_NAME,
            reservation_name=RESOURCE_NAME,
            assignment_name=None,
            slots=SLOTS,
        )

        delete_capacity_commitment_mock.assert_called_once_with(name=RESOURCE_NAME)
        delete_reservation_mock.assert_called_once_with(name=RESOURCE_NAME)

        assert "None BigQuery assignment to update or delete" in caplog.text

    @mock.patch.object(BigQueryReservationServiceHook, "delete_capacity_commitment")
    @mock.patch.object(BigQueryReservationServiceHook, "delete_reservation")
    @mock.patch.object(BigQueryReservationServiceHook, "delete_assignment")
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "get_reservation",
        return_value=Reservation(name=RESOURCE_NAME, slot_capacity=SLOTS),
    )
    def test_delete_commitment_reservation_and_assignment_false_slots_condition(
        self,
        get_reservation_mock,
        delete_assignment_mock,
        delete_reservation_mock,
        delete_capacity_commitment_mock,
    ):
        self.hook.delete_commitment_reservation_and_assignment(
            commitment_name=RESOURCE_NAME,
            reservation_name=RESOURCE_NAME,
            assignment_name=RESOURCE_NAME,
            slots=SLOTS,
        )

        delete_capacity_commitment_mock.assert_called_once_with(name=RESOURCE_NAME)
        delete_reservation_mock.assert_called_once_with(name=RESOURCE_NAME)
        delete_assignment_mock.assert_called_once_with(name=RESOURCE_NAME)

    @mock.patch.object(BigQueryReservationServiceHook, "update_reservation")
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "get_reservation",
        return_value=Reservation(name=RESOURCE_NAME, slot_capacity=SLOTS_ALL),
    )
    def test_delete_commitment_reservation_and_assignment_true_slots_condition(
        self, get_reservation_mock, update_reservation_mock
    ):
        self.hook.delete_commitment_reservation_and_assignment(
            reservation_name=RESOURCE_NAME,
            slots=SLOTS,
        )

        new_slots = SLOTS_ALL - SLOTS

        update_reservation_mock.assert_called_once_with(
            name=RESOURCE_NAME, slots=new_slots
        )

    @mock.patch.object(
        ReservationServiceClient,
        "delete_capacity_commitment",
        side_effect=Exception("Test"),
    )
    def test_delete_commitment_reservation_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.delete_commitment_reservation_and_assignment(
                commitment_name=RESOURCE_NAME,
                slots=SLOTS,
            )

    @mock.patch.object(
        BigQueryReservationServiceHook,
        "list_capacity_commitments",
        return_value=[
            CapacityCommitment(name="c1"),
            CapacityCommitment(name="c2"),
        ],
    )
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "list_reservations",
        return_value=[
            Reservation(name="r1"),
            Reservation(name="r2"),
            Reservation(name="r3"),
        ],
    )
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "list_assignments",
        return_value=[
            Assignment(name="a1"),
            Assignment(name="a2"),
            Assignment(name="a3"),
            Assignment(name="a4"),
        ],
    )
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "delete_capacity_commitment",
    )
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "delete_reservation",
    )
    @mock.patch.object(
        BigQueryReservationServiceHook,
        "delete_assignment",
    )
    def test_delete_all_commitments(
        self,
        delete_assignment_mock,
        delete_reservation_mock,
        delete_capacity_commitment_mock,
        list_assignments_mock,
        list_reservations_mock,
        list_capacity_commitments_mock,
    ):
        self.hook.delete_all_commitments(
            project_id=PROJECT_ID,
            location=LOCATION,
        )

        delete_assignment_mock.assert_has_calls(
            [
                mock.call(name="a1"),
                mock.call(name="a2"),
                mock.call(name="a3"),
                mock.call(name="a4"),
            ]
        )

        delete_reservation_mock.assert_has_calls(
            [
                mock.call(name="r1"),
                mock.call(name="r2"),
                mock.call(name="r3"),
            ]
        )

        delete_capacity_commitment_mock.assert_has_calls(
            [
                mock.call(name="c1"),
                mock.call(name="c2"),
            ]
        )

    @mock.patch.object(
        ReservationServiceClient,
        "list_capacity_commitments",
        side_effect=Exception("Test"),
    )
    def test_delete_all_commitments_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.delete_all_commitments(
                project_id=PROJECT_ID,
                location=LOCATION,
            )
