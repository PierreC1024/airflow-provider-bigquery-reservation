"""This module contains a BigQuery Reservation Hook."""
from __future__ import annotations
import datetime
import hashlib
import re
import uuid
from time import sleep
from typing import Sequence

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseHook,
)
from google.api_core import retry
from google.cloud import bigquery
from google.cloud.bigquery_reservation_v1 import (
    Assignment,
    CapacityCommitment,
    Reservation,
    ReservationServiceClient,
)
from google.protobuf import field_mask_pb2


class BigQueryReservationServiceHook(GoogleBaseHook):
    """
    Hook for Google Bigquery Reservation API.

    :param location: Location where the reservation is attached.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    conn_name_attr = "gcp_conn_id"
    default_conn_name = "google_cloud_bigquery_reservation_default"
    hook_name = "Google Bigquery Reservation"

    def __init__(
        self,
        location: str,
        gcp_conn_id: str = GoogleBaseHook.default_conn_name,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self.location = location
        self.running_job_id: str | None = None
        self.commitment: CapacityCommitment | None = None
        self.reservation: Reservation | None = None
        self.assignment: Assignment | None = None
        self._client: ReservationServiceClient | None = None

    def _get_commitment(self):
        return self.commitment  # pragma: no cover

    def _get_reservation(self):
        return self.reservation  # pragma: no cover

    def _get_assignment(self):
        return self.assignment  # pragma: no cover

    def get_client(self) -> ReservationServiceClient:
        """
        Get reservation service client.

        :return: Google Bigquery Reservation client
        """
        if not self._client:
            self._client = ReservationServiceClient(
                credentials=self.get_credentials(), client_info=CLIENT_INFO
            )
            return self._client
        else:
            return self._client

    @staticmethod
    def _verify_slots_conditions(slots: int) -> None:
        """
        Slots conditions acceptations validation.

        :param slots: Slots number
        """
        if slots % 100:
            raise AirflowException("Slots can only be reserved in increments of 100.")

    @staticmethod
    def _convert_gb_to_kb(value: int) -> int:
        """
        Convert GB value to KB.

        :param value: Value to convert
        """
        return value * 1073741824

    def format_resource_id(self, resource_id: str) -> str:
        """
        Generate a unique resource id matching google reservation requirements.

        Requiremets:
            - 64 characters maximum
            - contains only letters and dashes
            - begins by a letter
            - not finish by a dash

        :param resource_id: input resource_id

        :return: a resource id
        """
        uniqueness_suffix = hashlib.md5(str(uuid.uuid4()).encode()).hexdigest()[:10]
        resource_id = (
            re.sub(r"[:\_+.]", "-", resource_id.lower())[:59]
            + f"-{uniqueness_suffix[:10]}"
        )

        return resource_id

    def create_capacity_commitment(
        self,
        parent: str,
        slots: int,
        commitments_duration: str,
        name: str,
    ) -> CapacityCommitment:
        """
        Create capacity commitment.

        :param parent: Parent resource name e.g. `projects/myproject/locations/US`
        :param slots: Slots number
        :param commitments_duration: Commitment minimum durations (FLEX, MONTH, YEAR).
        :param name: capacity commitment name
        """
        client = self.get_client()

        try:
            self.commitment = client.create_capacity_commitment(
                request={
                    "parent": parent,
                    "capacity_commitment": CapacityCommitment(
                        plan=commitments_duration, slot_count=slots
                    ),
                    "capacity_commitment_id": name,
                }
            )
            return self.commitment

        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                f"Failed to create {slots} slots capacity commitment"
                f" ({commitments_duration})."
            )

    def list_capacity_commitments(self, parent: str) -> list[CapacityCommitment]:
        """
        List the capacity commitments.

        :param parent: Parent resource name e.g. `projects/myproject/locations/US`
        """
        client = self.get_client()

        try:
            commitments = client.list_capacity_commitments(
                parent=parent,
            )

            return [commitment for commitment in commitments]

        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to list capacity commitment: {parent}.")

    def delete_capacity_commitment(self, name: str) -> None:
        """
        Delete capacity commitment.

        :param name: Commitment name
        """
        client = self.get_client()

        try:
            client.delete_capacity_commitment(
                name=name,
                retry=retry.Retry(deadline=90, predicate=Exception, maximum=2),
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to delete {name} capacity commitment.")

    def create_reservation(
        self, parent: str, reservation_id: str, slots: int
    ) -> Reservation:
        """
        Create reservation.

        :param parent: Parent resource name e.g. `projects/myproject/locations/US`
        :param reservation_id: reservation identifier
        :param slots: Slots number
        """
        client = self.get_client()

        try:
            self.reservation = client.create_reservation(
                parent=parent,
                reservation_id=reservation_id,
                reservation=Reservation(slot_capacity=slots, ignore_idle_slots=True),
            )
            return self.reservation

        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to create {slots} slots reservation.")

    def get_reservation(self, name: str) -> Reservation:
        """
        Get reservation.

        :param name: Resource name e.g. `projects/myproject/locations/US/reservations/test`

         :return: Corresponding BigQuery Reservation
        """
        client = self.get_client()

        try:
            reservation = client.get_reservation(
                name=name,
            )
            return reservation

        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to get reservation: {name}.")

    def list_reservations(self, parent: str) -> list[Reservation]:
        """
        List the reservations.

        :param parent: Parent resource name e.g. `projects/myproject/locations/US`
        """
        client = self.get_client()

        try:
            reservations = client.list_reservations(
                parent=parent,
            )
            return [reservation for reservation in reservations]

        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to list reservation: {parent}.")

    def update_reservation(self, name: str, slots: int) -> None:
        """
        Update reservation with a new slots capacity.

        :param name: Reservation name e.g. `projects/myproject/locations/US/reservations/test`
        :param slots: New slots capacity
        """
        client = self.get_client()
        new_reservation = Reservation(name=name, slot_capacity=slots)
        field_mask = field_mask_pb2.FieldMask(paths=["slot_capacity"])

        try:
            client.update_reservation(
                reservation=new_reservation, update_mask=field_mask
            )
            self.reservation = new_reservation

        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                f"Failed to update {name} reservation: modification of the slot"
                f" capacity to {slots} slots."
            )

    def delete_reservation(self, name: str) -> None:
        """
        Delete reservation.

        :param name: Reservation name e.g. `projects/myproject/locations/US/reservations/test`
        """
        client = self.get_client()
        try:
            client.delete_reservation(name=name)
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to delete {name} reservation.")

    def create_assignment(
        self, parent: str, project_id: str, job_type: str
    ) -> Assignment:
        """
        Create assignment.

        :param parent: Parent resource name e.g. `projects/myproject/locations/US/reservations/team1-prod`
        :param project_id: GCP project where you wich to assign slots
        :param job_type: Type of job for assignment
        """
        client = self.get_client()
        assignee = f"projects/{project_id}"

        try:
            self.assignment = client.create_assignment(
                parent=parent,
                assignment=Assignment(job_type=job_type, assignee=assignee),
            )
            return self.assignment

        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                "Failed to create slots assignment with assignee {assignee} and"
                " job_type {job_type}"
            )

    def list_assignments(self, parent: str) -> list[Assignment]:
        """
        List the assignments.

        :param parent: Parent resource name e.g. `projects/myproject/locations/US/reservations/-`
        """
        client = self.get_client()

        try:
            reservations = client.list_assignments(
                parent=parent,
            )

            return [reservation for reservation in reservations]

        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to list capacity commitment: {parent}.")

    def search_assignment(
        self, parent: str, project_id: str, job_type: str
    ) -> Assignment | None:
        """
        Search the assignment which matches with the specific conditions.

        Conditions:
            - Assignee to the specified project_id
            - active state
            - the job type corresponding to the job type specified

        :param name: Parent resource name e.g. `projects/myproject/locations/US`
        :param project_id: GCP project where you wich to assign slots
        :param job_type: Type of job for assignment

        :return: Corresponding BigQuery assignment
        """
        client = self.get_client()

        query = f"assignee=projects/{project_id}"

        try:
            assignments = client.search_all_assignments(parent=parent, query=query)
            # Filter status active and corresponding job_type
            for assignment in assignments:
                if (
                    assignment.state.name == "ACTIVE"
                    and assignment.job_type.name == job_type
                ):
                    return assignment
            return None
        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                "Failed to search the list of reservation assignment."
            )

    def delete_assignment(self, name: str) -> None:
        """
        Delete assignment.

        :param name: Assignement name
                     e.g. `projects/myproject/locations/US/reservations/test/assignments/8950226598037373530`
        """
        client = self.get_client()
        try:
            client.delete_assignment(
                name=name,
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to delete {name} reservation.")

    @GoogleBaseHook.fallback_to_default_project_id
    def create_bi_reservation(self, project_id: str, size: int) -> None:
        """
        Create BI Engine reservation.

        :param project_id: The name of the project where we want to create/update
            the BI Engine reservation.
        :param size: The BI Engine reservation size in Gb.
        """
        parent = f"projects/{project_id}/locations/{self.location}/biReservation"
        client = self.get_client()
        size = self._convert_gb_to_kb(value=size)

        try:
            bi_reservation = client.get_bi_reservation(name=parent)
            bi_reservation.size = size + bi_reservation.size

            client.update_bi_reservation(bi_reservation=bi_reservation)

            self.log.info(
                "BI Engine reservation {parent} have been updated to {bi_reservation.size}Kb."
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to create BI engine reservation of {size}.")

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_bi_reservation(self, project_id: str, size: int | None = None) -> None:
        """
        Delete/Update BI Engine reservation with the specified memory size.

        :param project_id: The name of the project where we want to delete/update
            the BI Engine reservation.
        :param size: The BI Engine reservation size in Gb.
        """
        parent = f"projects/{project_id}/locations/{self.location}/biReservation"
        client = self.get_client()
        try:
            bi_reservation = client.get_bi_reservation(name=parent)
            if size is not None:
                size = self._convert_gb_to_kb(size)
                bi_reservation.size = max(bi_reservation.size - size, 0)
            else:
                bi_reservation.size = 0

            client.update_bi_reservation(bi_reservation=bi_reservation)
            self.log.info(
                "BI Engine reservation {parent} have been updated to {bi_reservation.size}Kb."
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to delete BI engine reservation of {size}.")

    def get_bq_client(self) -> bigquery.Client:
        """
        Get BQ service client.

        :return: Google Bigquery client
        """
        return bigquery.Client(
            credentials=self.get_credentials(), client_info=CLIENT_INFO
        )

    def _is_assignment_attached_in_query(
        self, client: bigquery.Client, project_id: str, location: str
    ) -> bool:
        """
        Check if assignment has been attached from a dummy query.

        :param client: BigQuery Client
        :param project_id: GCP project
        :param location: BigQuery project
        :return: bool
        """
        dummy_query = """
            SELECT dummy
            FROM UNNEST([STRUCT(true as dummy)])
        """
        query_job = client.query(
            dummy_query,
            project=project_id,
            location=location,
            job_id_prefix="test_assignment_reservation",
            job_config=bigquery.QueryJobConfig(use_query_cache=False),
        )
        if query_job._properties["statistics"].get("reservation_id"):
            return True
        else:
            return False

    @GoogleBaseHook.fallback_to_default_project_id
    def create_commitment_reservation_and_assignment(
        self,
        slots: int,
        assignment_job_type: str,
        commitments_duration: str,
        project_id: str = PROVIDE_PROJECT_ID,
        reservation_project_id: str | None = None,
    ) -> None:
        """
        Create a commitment for a specific amount of slots.

        Attach this commitment to a specified project by creating a new reservation and assignment
        or updating the existing one corresponding to the project assignment.
        Wait the assignment has been attached to a query.
        See https://cloud.google.com/bigquery/docs/reservations-assignments

        :param slots: Slots number to purchase and assign
        :param assignment_job_type: Type of job for assignment
        :param commitments_duration: Commitment minimum durations (FLEX, MONTH, YEAR).
        :param project_id: GCP project where you wich to assign slots
        """
        reservation_project_id = reservation_project_id or project_id
        self.log.info(
            f"The reservation will be on the GCP project: {reservation_project_id}"
        )

        self._verify_slots_conditions(slots=slots)
        parent = f"projects/{reservation_project_id}/locations/{self.location}"
        resource_name = self.format_resource_id(f"airflow_{project_id}_assignement")

        try:
            capacity_commitment = self.create_capacity_commitment(
                parent=parent,
                slots=slots,
                commitments_duration=commitments_duration,
                name=resource_name,
            )

            # Cannot create multiple assignments to the same project on the same job_type.
            # So if it has been already exist update the reservation only to attribute the slots desired.
            existing_assignment = self.search_assignment(
                parent=parent, project_id=project_id, job_type=assignment_job_type
            )

            if existing_assignment:
                self.assignment = existing_assignment
                reservation_parent = existing_assignment.name.split("/assignments")[0]
                current_reservation = self.get_reservation(name=reservation_parent)
                new_slots_reservation = current_reservation.slot_capacity + slots
                self.update_reservation(
                    name=current_reservation.name, slots=new_slots_reservation
                )
            else:
                reservation = self.create_reservation(
                    parent=parent, reservation_id=resource_name, slots=slots
                )
                assignment = self.create_assignment(
                    parent=reservation.name,
                    project_id=project_id,
                    job_type=assignment_job_type,
                )

            # Waiting the assignment attachment to send a dummy query every 15 seconds
            self.log.info("Waiting assignments attachment")

            bq_client = self.get_bq_client()
            while not self._is_assignment_attached_in_query(
                client=bq_client, project_id=project_id, location=self.location
            ):
                sleep(15)  # pragma: no cover

        except Exception as e:
            self.log.error(e)
            commitment_name = self.commitment.name if self.commitment else None
            reservation_name = self.reservation.name if self.reservation else None
            assignment_name = self.assignment.name if self.assignment else None
            self.delete_commitment_reservation_and_assignment(
                commitment_name=commitment_name,
                reservation_name=reservation_name,
                assignment_name=assignment_name,
                slots=slots,
            )
            raise AirflowException(
                "Failed to purchase, to reserve and to attribute"
                f" {slots} {commitments_duration} BigQuery slots commitments."
            )

    def delete_commitment_reservation_and_assignment(
        self,
        slots: int,
        commitment_name: str | None = None,
        reservation_name: str | None = None,
        assignment_name: str | None = None,
    ) -> None:
        """
        If it exists, delete/update the commitment, reservation and assignment resources.

        It will delete/update:
        - a commitment for a specific amount of slots.
        - If the amount of slots deleted is lower than the reservation slots capacity,
        update the reservation to the corresponding slots otherwise delete reservation and assignment.

        :param slots: Slots number to delete
        :param commitment_name: Commitment name e.g. `projects/myproject/locations/US/commitments/test`
        :param reservation_name: Reservation name e.g. `projects/myproject/locations/US/reservations/test`
        :param assignment_name: Assignment name e.g. `projects/myproject/locations/US/reservations/test/assignments/8950226598037373530`
        """
        try:
            if reservation_name:
                self._verify_slots_conditions(slots=slots)
                reservation = self.get_reservation(name=reservation_name)

                # If reservation have more capacity_slots than requested only update reservation
                if reservation.slot_capacity > slots:
                    new_slots_reservation = reservation.slot_capacity - slots
                    self.update_reservation(
                        name=reservation.name, slots=new_slots_reservation
                    )
                    self.log.info(
                        f"BigQuery reservation {reservation_name} has been updated"
                        + f"to {reservation.slot_capacity} ->"
                        f" {new_slots_reservation} slots"
                    )
                else:
                    if assignment_name:
                        self.delete_assignment(name=assignment_name)
                        self.log.info(
                            f"BigQuery Assigmnent {assignment_name} has been deleted"
                        )
                    else:
                        self.log.warning("None BigQuery assignment to update or delete")
                    self.delete_reservation(name=reservation_name)
                    self.log.info(
                        f"BigQuery reservation {reservation_name} has been deleted"
                    )
            else:
                self.log.warning("None BigQuery reservation to update or delete")

            if commitment_name:
                self.delete_capacity_commitment(name=commitment_name)
                self.log.info(f"BigQuery commitment {commitment_name} has been deleted")
            else:
                self.log.warning("None BigQuery commitment to delete")
        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                "Failed to delete flex BigQuery slots("
                + "assignement: {assignment_name}, "
                + "reservation: {reservation_name}, "
                + "commitments: {commitment_name}."
            )

    def delete_all_commitments(self, project_id: str, location: str) -> None:
        """
        Delete all commitments, reservation and assignment associated to a specific project and location.

        :param project_id: Commitment project
        :param location: Commitment location
        """
        parent = f"projects/{project_id}/locations/{self.location}"
        try:
            commitments = self.list_capacity_commitments(parent)
            reservations = self.list_reservations(parent)
            assignments = self.list_assignments(f"{parent}/reservations/-")

            for assignment in assignments:
                self.delete_assignment(name=assignment.name)

            for reservation in reservations:
                self.delete_reservation(name=reservation.name)

            for commitment in commitments:
                self.delete_capacity_commitment(name=commitment.name)

        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to delete commitments in {parent}")

    def delete_commitments_assignment_associated(
        self, project_id: str, location: str, reservation_project_id: str
    ) -> None:
        """
        Delete all commitments, reservation and assignment associated to a specific project assignment.

        :param project_id: Reservation project assignation
        :param location: Commitment location
        :param reservation_project_id: Commitment project
        """
        parent = f"projects/{reservation_project_id}/locations/{self.location}"
        try:
            assignments = self.list_assignments(f"{parent}/reservations/-")
            assignments_updated = []
            reservations = set()
            commitments = self.list_capacity_commitments(parent)

            for assignment in assignments:
                if assignment.assignee == f"projects/{project_id}":
                    reservations.add(assignment.name.split("/assignments")[0])
                    self.delete_assignment(name=assignment.name)
                else:
                    assignments_updated.append(assignment)

            for reservation in reservations:
                assert all(
                    [
                        False
                        if reservation == assignment.name.split("/assignments")[0]
                        else True
                        for assignment in assignments_updated
                    ]
                ), "Reservation is in use. We cannot delete it."
                self.delete_reservation(name=reservation)

            for commitment in commitments:
                if f"airflow-{project_id}-assignement" in commitment.name:
                    self.delete_capacity_commitment(name=commitment.name)

        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                f"Failed to delete commitments in {parent} for project assignee {project_id}."
            )
