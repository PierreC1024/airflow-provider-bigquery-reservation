"""This module contains Google BigQuery Commitment Slot reservation operators."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from bigquery_commitment.hooks.bigquery_reservation import (
    BiqQueryReservationServiceHook,
)
from google.api_core.exceptions import Conflict
from airflow.exceptions import AirflowException


class BigQueryCommitmentSlotReservationOperator(BaseOperator):
    """
    Buy BigQuery Flex slots and assign them to a GCP project.
    This operator work in the following way:

    - creates slots capacity commitment with the number of slots specified (increment of 100)
    - creates or update (if an assigment on the specified project already exists) reservation
      with the number of slots specified.
    - creates or update (if an assigment on the specified project already exists) assigment
    - waits 600 seconds to validate the assigment could be use to a BigQuery query.
      See documentation: https://cloud.google.com/bigquery/docs/reservations-assignments#assign-project-to-none

    For BigQuery reservation API see here:
        https://cloud.google.com/bigquery/docs/reference/reservations


    :param project_id: Google Cloud Project where the reservation is attached
    :param location: location where the reservation is attached
    :param slots_provisioning: Number of slots to buy. Slots can only be reserved in increments of 100.
    :param commitments_duration: Commitment minimum durations: one minute (FLEX, default), one month (MONTH) or one year (YEAR).
    :param assignment_job_type: Commitment assigment job type
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

    ui_color = "#9c5fff"

    # ToDo: Add parameter to choose if you use flex provisioner

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

    def execute(self, context: Any):
        hook = BiqQueryReservationServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
            location=self.location,
        )

        try:
            hook.create_flex_slots_reservation_and_assignment(
                slots=self.slots_provisioning,
                commitments_duration=self.commitments_duration,
            )
        except Conflict:
            raise AirflowException(
                "Failed to create commitment reservation and assignment"
                + "for {self.slots_provisioning} slots for the project {self.project_id} in {self.location}"
            )