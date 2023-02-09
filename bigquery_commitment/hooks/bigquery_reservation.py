"""This module contains a BigQuery Reservation Hook."""
from __future__ import annotations

from time import sleep
from typing import Dict

from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseHook,
)
from airflow.providers.google.common.consts import CLIENT_INFO

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
)

from google.cloud.bigquery_reservation_v1.types.CapacityCommitment import State

from google.api_core import retry
from airflow.exceptions import AirflowException


class BiqQueryReservationServiceHook(GoogleBaseHook):
    """
    Hook for Google Bigquery Reservatuin API.
    """

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
    def _verify_slots_conditions(slots):
        """
        Verification of slots conditions acceptations.

        :param slots: Slots number
        """
        if slots % 100:
            raise AirflowException(
                "Commitment slots can only be reserved in increments of 100."
            )

    def create_capacity_commitment(
        self, parent: str, slots: int, commitments_duration: str
    ):
        """
        Create capacity slots commitment.

        :param parent: The parent resource name e.g. `projects/myproject/locations/US`
        :param slots: Slots number to purchase
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

    def delete_capacity_commitment(self, name: str):
        """
        Delete capacity slots commitment.

        :param name: Commintment name
        """
        client = self.get_client()

        try:
            client.delete_capacity_commitment(
                request=DeleteCapacityCommitmentRequest(
                    name=name,
                    retry=retry.Retry(deadline=90, predicate=Exception, maximum=2),
                )
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to delete {name} capacity commitment.")

    def create_slots_reservation(self, parent: str, reservation_id: str, slots: int):
        """
        Create slots reservation.

        :param parent: The parent resource name e.g. `projects/myproject/locations/US`
        :param reservation_id: reservation identifier
        :param slots: Slots number to reserve
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
            raise AirflowException(f"Failed to create reservation of {slots} slots.")

    def get_slots_reservation(self, name) -> Reservation:
        """
        get slots reservation.

        :param name: The resource name e.g. `projects/myproject/locations/US/reservations/test`
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

    def update_slots_reservation(self, name: str, slots: int):
        """
        Update slots reservation.

        :param name: The reservation name e.g. `projects/myproject/locations/US/reservations/test`
        :param slots: New slots capacity
        """
        client = self.get_client()

        try:
            client.update_reservation(
                request=UpdateReservationRequest(name=name, slotCapacity=slots)
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                f"Failed to update {name} reservation (adding {slots} slots)."
            )

    def delete_slots_reservation(self, name: str):
        """
        Delete slots reservation.

        :param name: The reservation name e.g. `projects/myproject/locations/US/reservations/test`
        """
        client = self.get_client()
        try:
            client.delete_reservation(request=DeleteReservationRequest(name=name))
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to delete {name} reservation.")

    def create_slots_assignment(self, parent: str, project_id: str, job_type: str):
        """
        Create slots assignment.

        :param parent: The parent resource name e.g. `projects/myproject/locations/US/reservations/team1-prod`
        :param project_id: The GCP project where you wich to assign slots
        :param job_type: Type of job for assignment
        """
        client = self.get_client()
        assignee = f"projects/{project_id}"

        try:
            assignment = client.create_assignment(
                parent=reservation.name,
                assignment=Assignment(job_type=job_type, assignee=assignee),
            )
            self.assigment = assigment

        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                "Failed to create slots assignment with assignee {assignee} and job_type {job_type}"
            )

    def search_reservation_assigments(
        self, parent: str, project_id: str, job_type: str
    ) -> Assignment:
        """
        Get reservation assignment

        :param name: The parent resource name e.g. `projects/myproject/locations/US`
        :param project_id: The GCP project where you wich to assign slots
        :param job_type: Type of job for assignment
        """
        client = self.get_client()

        parent = f"assignee=projects/{project_id}"

        try:
            assignments = client.client.search_all_assignments(
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

    def delete_reservation_assignment(self, name: str) -> None:
        """
        Delete reservation assignment.

        :param name: The assignement name e.g. `projects/myproject/locations/US/reservations/test/assignments/8950226598037373530`
        """
        client = self.get_client()
        try:
            client.delete_assignement(
                request=DeleteAssignmentRequest(
                    name=name,
                )
            )
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to delete {name} reservation.")

    @GoogleBaseHook.fallback_to_default_project_id
    def create_slots_reservation_and_assignment(
        self, slots: int, job_type: str, project_id: str = PROVIDE_PROJECT_ID
    ) -> None:
        """
        Create a commitment for a specific amount of slots.
        Attach this commitment to a specified project by creating a new reservation and assignment
        or updating it the existing one.

        :param slots: Slots number to purchase and assign
        :param job_type: Type of job for assignment
        :param project_id: The GCP project where you wich to assign slots
        """
        self._verify_slots_conditions(slots)
        parent = f"projects/{project_id}/locations/{self.location}"

        try:
            self.create_capacity_commitment(parent, slots, commitments_duration)

            # We cannot create multiple assignments to the same project on the same job_type.
            # So if it has been already exist we update the reservation only to attribute the slots desired.
            existing_assignment = self.search_reservation_assigments(
                parent, project_id, job_type
            )

            if existing_assignment:
                reservation_parent = existing_assignment.name.split("/assignments")[0]
                current_reservation = self.get_slots_reservation(
                    parent=reservation_parent
                )
                new_slots_reservation = current_reservation.slot_capacity + slots
                self.update_slots_reservation(name, new_slots_reservation)
            else:
                self.create_slots_reservation(parent, reservation_id, slots)
                self.create_slots_assignment(
                    self.reservation.name, project_id, job_type
                )

            # Wait 5min. See documentation https://cloud.google.com/bigquery/docs/reservations-assignments#assign-project-to-none
            self.log.info(f"Waiting 5 minutes to take into account assignments...")
            sleep(300)

        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                f"Failed to purchase {slots} flex BigQuery slots commitments (parent: {parent}, commitment: {commitment.name})."
            )

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_slots_reservation_and_assignment(
        self,
        commitment_name: str,
        reservation_name: str,
        assignment_name: str,
        slots: int,
    ) -> None:
        """
        Delete a commitment for a specific amount of slots.
        Attach this commitment to a specified project by creating a new reservation and assignment
        or updating it the existing one.

        :param commitment_name: The commitment name e.g. `projects/myproject/locations/US/commitments/test`
        :param reservation_name: The reservation name e.g. `projects/myproject/locations/US/reservations/test`
        :param assignment_name: The assignment name e.g. `projects/myproject/locations/US/reservations/test/assignments/8950226598037373530`
        :param slots: Slots number to delete
        """
        try:
            self._verify_slots_conditions(slots)

            reservation = self.get_slots_reservation(name=reservation_name)

            # If reservation have more capacity_slots than requested only update reservation
            if reservation.capacity_slots > slots:
                new_slots_reservation = reservation.capacity_slots - slots
                self.update_slots_reservation(
                    name=reservation_name, slots=new_slots_reservation
                )
            else:
                self.delete_reservation_assignment(name=assignment_name)
                self.log.info(
                    f"BigQuery Assigmnent {self.assignment_name} has been deleted"
                )
                self.delete_slots_reservation(name=reservation_name)
                self.log.info(
                    f"BigQuery reservation {self.commitment} has been deleted"
                )

            self.delete_capacity_commitment(name=commitment_name)
            self.log.info(f"BigQuery commitment {commitment_name} has been deleted")

        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                "Failed to delete flex BigQuery slots("
                + "assignement: {assignation_name}, "
                + "reservation: {reservation_name}, "
                + "commitments: {commitment_name}."
            )
