"""This module contains Google BigQuery Commitment Slot reservation operators."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from bigquery_commitment.hooks.bigquery_reservation import (
    BiqQueryReservationServiceHook,
)
from google.api_core.exceptions import Conflict
from airflow.exceptions import AirflowException


bq_reservation_operator_color = "#9c5fff"


class BigQueryCommitmentSlotReservationOperator(BaseOperator):
    """
    Buy BigQuery slots and assign them to a GCP project.
    This operator work in the following way:

    - creates slots capacity commitment with the number of slots specified (increment of 100)
    - creates or update (if an assignment on the specified project already exists) reservation
      with the number of slots specified.
    - creates or update (if an assignment on the specified project already exists) assignment
    - waits 600 seconds to validate the assignment could be use to a BigQuery query.
      See documentation: https://cloud.google.com/bigquery/docs/reservations-assignments#assign-project-to-none

    For BigQuery reservation API see here:
        https://cloud.google.com/bigquery/docs/reference/reservations


    :param project_id: Google Cloud Project where the reservation is attached
    :param location: location where the reservation is attached
    :param slots_provisioning: Number of slots to buy. Slots can only be reserved in increments of 100.
    :param commitments_duration: Commitment minimum durations: one minute (FLEX, default), one month (MONTH) or one year (YEAR).
    :param assignment_job_type: Commitment assignment job type
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called
    """

    template_fields: Sequence[str] = (
        "project_id",
        "location",
        "slots_provisioning",
        "commitments_duration",
    )
    ui_color = bq_reservation_operator_color

    def __init__(
        self,
        project_id: str | None,
        location: str | None,
        slots_provisioning: int,
        commitments_duration: str = "FLEX",
        assignment_job_type: str = "QUERY",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_on_kill: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.slots_provisioning = slots_provisioning
        self.commitments_duration = commitments_duration
        self.assignment_job_type = assignment_job_type
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.cancel_on_kill = cancel_on_kill
        self.hook: BigQueryHook | None = None

    def execute(self, context: Any) -> None:
        hook = BiqQueryReservationServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
            location=self.location,
        )

        resource_name = hook.generate_resource_name(
            dag_id=self.dag_id,
            task_id=self.task_id,
            logical_date=context["logical_date"],
        )

        hook.create_slots_reservation_and_assignment(
            resource_name=resource_name,
            slots=self.slots_provisioning,
            assignment_job_type=self.assignment_job_type,
            commitments_duration=self.commitments_duration,
        )

        context["ti"].xcom_push(key="commitment_name", value=hook.commitment.name)
        context["ti"].xcom_push(key="reservation_name", value=hook.reservation.name)
        context["ti"].xcom_push(key="assignment_name", value=hook.assignment.name)

     def on_kill(self) -> None:
        super().on_kill()
        if self.hook is not None:
            hook = self.hook
            commitment_name = commitment.name if commitment else None
            reservation_name = reservation.name if reservation else None
            assignment_name = assignment.name if assignment else None
            delete_slots_reservation_and_assignment(
                commitment_name=commitment_name,
                reservation_name=reservation_name,
                assignation_name=assignation_name,
                slots=slot_capacity
            )


class BigQueryCommitmentSlotDeletionOperator(BaseOperator):
    """
    Delete BigQuery slots and remove associated ressources.
    This operator work in the following way:

    For BigQuery reservation API see here:
        https://cloud.google.com/bigquery/docs/reference/reservations


    :param location: location where the reservation is attached
    :param slots_provisioning: Number of slots to buy. Slots can only be reserved in increments of 100.
    :param commitment_name: The commitment name
            e.g. `projects/myproject/locations/US/commitments/test`
    :param reservation_name: The reservation name
            e.g. `projects/myproject/locations/US/reservations/test`
    :param assignment_name: The assignment name
            e.g. `projects/myproject/locations/US/reservations/test/assignments/8950226598037373530`
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called
    """

    template_fields: Sequence[str] = (
        "location",
        "slots_provisioning",
        "commitment_name",
        "reservation_name",
        "assignment_name",
    )
    ui_color = bq_reservation_operator_color

    def __init__(
        self,
        location: str | None,
        slots_provisioning: int,
        commitment_name: str,
        reservation_name: str,
        assignment_name: str,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_on_kill: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.slots_provisioning = slots_provisioning
        self.commitment_name = commitment_name
        self.reservation_name = reservation_name
        self.assignment_name = assignment_name
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.cancel_on_kill = cancel_on_kill
        self.hook: BigQueryHook | None = None

    def execute(self, context: Any):
        hook = BiqQueryReservationServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
            location=self.location,
        )

        hook.delete_slots_reservation_and_assignment(
            commitment_name=self.commitment_name,
            reservation_name=self.reservation_name,
            assignment_name=self.assignment_name,
            slots=self.slots_provisioning,
        )
