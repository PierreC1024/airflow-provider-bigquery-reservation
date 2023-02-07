"""This module contains a BigQuery Reservation Hook."""
from __future__ import annotations

from time import sleep

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
)
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
            self.commitment = commitment.name

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
            self.reservation = reservation.name

        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to create reservation of {slots} slots.")

    def update_slots_reservation(self, name: str, slots: int):
        """
        Update slots reservation.

        :param name: reservation name
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

        :param name: The reservation name
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
            self.assigment = assigment.name

        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                "Failed to create slots assignment with assignee {assignee} and job_type {job_type}"
            )

    def get_reservation_assigments(self, parent: str):
        """
        Get reservation assignment

        :param parent: The parent resource name e.g. `projects/myproject/locations/US/reservations/team1-prod`
        """
        client = self.get_client()

        try:
            assignments = client.list_assignments(
                requests=ListAssignmentsRequest(
                    parent=parent,
                )
            )
            return assignments
        except Exception as e:
            self.log.error(e)
            raise AirflowException("Failed to get the list of reservation assignment.")

    def delete_reservation_assignment(self, name: str):
        """
        Delete reservation assignment.

        :param name: The assignement name
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

    @staticmethod
    def _existing_slots(assignments_list, project_id: str, location: str) -> int:
        # ToDo
        pass

    @GoogleBaseHook.fallback_to_default_project_id
    def create_flex_slots_reservation_and_assignment(
        self, slots: int, job_type: str, project_id: str = PROVIDE_PROJECT_ID
    ) -> None:
        """
        Create a commitment for a specific amount of slots.
        Attach this commitment to a specified project by creating a new reservation and assignment
        or updating it the existing one.

        :param slots: Slots number to purchase and assign
        """
        self._verify_slots_conditions(slots)
        parent = f"projects/{project_id}/locations/{self.location}"

        try:
            self.create_capacity_commitment(parent, slots, commitments_duration)

            existing_assignments = self.get_reservation_assigments(parent)
            new_slots_reservation = _existing_slots(
                existing_assignments, project_id, self.location
            )

            if existing_slots:
                new_slots_reservation = existing_slots + slots
                self.update_slots_reservation(name, new_slots_reservation)
            else:
                self.create_slots_reservation(parent, reservation_id, slots)
                self.create_slots_assignment(parent, project_id, job_type)

            # Wait 5min. See documentation https://cloud.google.com/bigquery/docs/reservations-assignments#assign-project-to-none
            self.log.info(f"Waiting 5 minutes to take into account assignments...")
            sleep(300)

        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                f"Failed to purchase {slots} flex BigQuery slots commitments (parent: {parent}, commitment: {commitment.name})."
            )

    def create_flex_slots_reservation_and_assignment(self) -> None:
        """
        Delete a flex slots reserved
        """
        try:

            self.log.info(f"BigQuery commitment {self.commitment} has been deleted")

        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                "Failed to delete flex BigQuery slots("
                + "assignement: {self.assignement}, "
                + "reservation: {self.reservation}, "
                + "commitments: {self.commitment}."
            )
