"""This module contains a BigQuery Reservation Hook."""
from __future__ import annotations

import hashlib
import re
import uuid

from time import sleep
from typing import Dict

from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseHook,
)
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.exceptions import AirflowException

from google.cloud.bigquery_reservation_v1 import (
    ReservationServiceClient,
    CapacityCommitment,
    Reservation,
    Assignment,
    UpdateReservationRequest,
    DeleteAssignmentRequest,
    DeleteCapacityCommitmentRequest,
    DeleteReservationRequest,
    GetReservationRequest,
    SearchAllAssignmentsRequest,
    UpdateBiReservationRequest,
    GetBiReservationRequest,
)

from google.protobuf import field_mask_pb2
from google.api_core import retry


class BigQueryReservationServiceHook(GoogleBaseHook):
    """
    Hook for Google Bigquery Reservation API.
    """

    conn_name_attr = "gcp_conn_id"
    default_conn_name = "google_cloud_bigquery_reservation_default"
    conn_type = "gcp_bigquery_reservation"
    hook_name = "Google Bigquery Reservation"

    def __init__(
        self,
        gcp_conn_id: str = GoogleBaseHook.default_conn_name,
        location: str | None = None,
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.location = location
        self.running_job_id: str | None = None

    def get_client(self) -> ReservationServiceClient:
        """
        Get reservation service client.

        :return: Google Bigquery Reservation client
        """
        return ReservationServiceClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO
        )

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

    def generate_resource_id(
        self,
        dag_id: str,
        task_id: str,
        logical_date: str,
    ) -> str:
        """
        Generate a unique resource id matching google reservation requirements:
            - 64 characters maximum
            - contains only letters and dashes
            - begins by a letter
            - not finish by a dash

        :param dag_id: Airflow DAG id
        :param task_id: Airflow task id
        :param logical_date: Logical execution date

        :return: a resource id
        """
        uniqueness_suffix = hashlib.md5(str(uuid.uuid4()).encode()).hexdigest()[:5]
        exec_date = logical_date.isoformat()
        resource_id = f"airflow__{dag_id}_{task_id}__{exec_date}"
        resource_id = (
            re.sub(r"[:\_+.]", "-", resource_id.lower())[:59]
            + f"-{uniqueness_suffix[:4]}"
        )

        return resource_id

    def create_capacity_commitment(
        self,
        parent: str,
        slots: int,
        commitments_duration: str,
    ) -> None:
        """
        Create capacity commitment.

        :param parent: Parent resource name e.g. `projects/myproject/locations/US`
        :param slots: Slots number
        :param commitments_duration: Commitment minimum durations (FLEX, MONTH, YEAR).
        """
        client = self.get_client()

        try:
            commitment = client.create_capacity_commitment(
                parent=parent,
                capacity_commitment=CapacityCommitment(
                    plan=commitments_duration, slot_count=slots
                ),
            )
            self.commitment = commitment

        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                f"Failed to create {slots} slots capacity commitment ({commitments_duration})."
            )

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

    def create_reservation(self, parent: str, reservation_id: str, slots: int) -> None:
        """
        Create reservation.

        :param parent: Parent resource name e.g. `projects/myproject/locations/US`
        :param reservation_id: reservation identifier
        :param slots: Slots number
        """
        client = self.get_client()

        try:
            reservation = client.create_reservation(
                parent=parent,
                reservation_id=reservation_id,
                reservation=Reservation(slot_capacity=slots, ignore_idle_slots=True),
            )
            self.reservation = reservation

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
                GetReservationRequest(
                    name=name,
                )
            )
            return reservation

        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to get reservation: {name}.")

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
                f"Failed to update {name} reservation: modification of the slot capacity to {slots} slots."
            )

    def delete_reservation(self, name: str) -> None:
        """
        Delete reservation.

        :param name: Reservation name e.g. `projects/myproject/locations/US/reservations/test`
        """
        client = self.get_client()
        try:
            client.delete_reservation(request=DeleteReservationRequest(name=name))
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to delete {name} reservation.")

    def create_assignment(self, parent: str, project_id: str, job_type: str) -> None:
        """
        Create assignment.

        :param parent: Parent resource name e.g. `projects/myproject/locations/US/reservations/team1-prod`
        :param project_id: GCP project where you wich to assign slots
        :param job_type: Type of job for assignment
        """
        client = self.get_client()
        assignee = f"projects/{project_id}"

        try:
            assignment = client.create_assignment(
                parent=parent,
                assignment=Assignment(job_type=job_type, assignee=assignee),
            )
            self.assignment = assignment

        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                "Failed to create slots assignment with assignee {assignee} and job_type {job_type}"
            )

    def search_assignment(
        self, parent: str, project_id: str, job_type: str
    ) -> Assignment | None:
        """
        Search the assignment which matches with the conditions:
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
            assignments = client.search_all_assignments(
                request=SearchAllAssignmentsRequest(parent=parent, query=query)
            )
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
                request=DeleteAssignmentRequest(
                    name=name,
                )
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to delete {name} reservation.")

    def create_bi_reservation(self, parent: str, size: int):
        """
        Create BI Engine reservation

        :param parent: Parent resource name e.g. `projects/myproject/locations/US/biReservation
        :param size: Memory reservation size in Gigabyte
        """
        client = self.get_client()
        size = self._convert_gb_to_kb(value=size)

        try:
            bi_reservation = client.get_bi_reservation(
                request=GetBiReservationRequest(name=parent)
            )

            bi_reservation.size = size

            client.update_bi_reservation(
                request=UpdateBiReservationRequest(bi_reservation=bi_reservation)
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to create BI engine reservation of {size}.")

    def delete_bi_reservation(self, parent: str, size: int):
        """
        Delete/Update BI Engine reservation with the specified memory size

        :param parent: Parent resource name e.g. `projects/myproject/locations/US/biReservation
        :param size: Memory reservation size in Gigabyte
        """
        client = self.get_client()
        try:
            size = self._convert_gb_to_kb(size)
            bi_reservation = client.get_bi_reservation(
                request=GetBiReservationRequest(name=parent)
            )

            bi_reservation.size = max(bi_reservation.size - size, 0)

            client.update_bi_reservation(
                request=UpdateBiReservationRequest(bi_reservation=bi_reservation)
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to delete BI engine reservation of {size}.")

    @GoogleBaseHook.fallback_to_default_project_id
    def create_commitment_reservation_and_assignment(
        self,
        resource_id: str,
        slots: int,
        assignment_job_type: str,
        commitments_duration: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> None:
        """
        Create a commitment for a specific amount of slots.
        Attach this commitment to a specified project by creating a new reservation and assignment
        or updating the existing one corresponding to the project assignment.

        :param resource_id: Resource id
        :param slots: Slots number to purchase and assign
        :param assignment_job_type: Type of job for assignment
        :param commitments_duration: Commitment minimum durations (FLEX, MONTH, YEAR).
        :param project_id: GCP project where you wich to assign slots
        """
        self._verify_slots_conditions(slots=slots)
        parent = f"projects/{project_id}/locations/{self.location}"

        try:
            self.create_capacity_commitment(
                parent=parent, slots=slots, commitments_duration=commitments_duration
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
                self.create_reservation(
                    parent=parent, reservation_id=resource_id, slots=slots
                )
                self.create_assignment(
                    parent=self.reservation.name,
                    project_id=project_id,
                    job_type=assignment_job_type,
                )

            # Wait 5min. See documentation https://cloud.google.com/bigquery/docs/reservations-assignments#assign-project-to-none
            self.log.info(f"Waiting 5 minutes to take into account assignments...")
            sleep(300)

        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                f"Failed to purchase, to reserve and to attribute {slots} {commitments_duration} BigQuery slots commitments."
            )

    def delete_commitment_reservation_and_assignment(
        self,
        commitment_name: str | None = None,
        reservation_name: str | None = None,
        assignment_name: str | None = None,
        slots: int | None = None,
    ) -> None:
        """
        If it exists, delete/update the following resources:
        - a commitment for a specific amount of slots.
        - If the amount of slots deleted is lower than the reservation slots capacity,
        update the reservation to the corresponding slots otherwise delete reservation and assignment.

        :param commitment_name: Commitment name e.g. `projects/myproject/locations/US/commitments/test`
        :param reservation_name: Reservation name e.g. `projects/myproject/locations/US/reservations/test`
        :param assignment_name: Assignment name e.g. `projects/myproject/locations/US/reservations/test/assignments/8950226598037373530`
        :param slots: Slots number to delete
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
                        + f"to {reservation.slot_capacity} -> {new_slots_reservation} slots"
                    )
                else:
                    if assignment_name:
                        self.delete_assignment(name=assignment_name)
                        self.log.info(
                            f"BigQuery Assigmnent {assignment_name} has been deleted"
                        )
                    else:
                        self.log.info(f"None BigQuery assignment to update or delete")
                    self.delete_reservation(name=reservation_name)
                    self.log.info(
                        f"BigQuery reservation {reservation_name} has been deleted"
                    )
            else:
                self.log.info(f"None BigQuery reservation to update or delete")

            if commitment_name:
                self.delete_capacity_commitment(name=commitment_name)
                self.log.info(f"BigQuery commitment {commitment_name} has been deleted")
            else:
                self.log.info(f"None BigQuery commitment to delete")
        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                "Failed to delete flex BigQuery slots("
                + "assignement: {assignment_name}, "
                + "reservation: {reservation_name}, "
                + "commitments: {commitment_name}."
            )
