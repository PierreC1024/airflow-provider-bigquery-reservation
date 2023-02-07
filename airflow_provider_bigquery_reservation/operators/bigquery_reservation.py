"""This module contains Google BigQuery reservation operators."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from google.api_core.exceptions import Conflict
from airflow_provider_bigquery_reservation.hooks.bigquery_reservation import (
    BigQueryReservationServiceHook,
)


bq_reservation_operator_color = "#9c5fff"


class BigQueryReservationCreateOperator(BaseOperator):
    """
    Buy BigQuery slots and assign them to a GCP project.
    This operator works in the following way:

    - create slots capacity commitment with the number of slots specified (increment of 100)
    - create or update (if an assignment on the specified project already exists) reservation
      with the number of slots specified.
    - create or update (if an assignment on the specified project already exists) assignment
    - waits 600 seconds to validate the assignment could be use to a BigQuery query.
      See documentation: https://cloud.google.com/bigquery/docs/reservations-assignments#assign-project-to-none

    For BigQuery reservation API see here:
        https://cloud.google.com/bigquery/docs/reference/reservations


    :param project_id: Google Cloud Project where the reservation is attached.
    :param location: Location where the reservation is attached.
    :param slots_provisioning: Slots number to provision. Slots can only be reserved in increments of 100.
    :param commitments_duration: Commitment minimum durations i.e. one minute (FLEX, default), one month (MONTH) or one year (YEAR).
    :param assignment_job_type: Commitment assignment job type (PIPELINE, QUERY, ML_EXTERNAL, BACKGROUND)
    :param gcp_conn_id: Connection ID used to connect to Google Cloud.
    :param delegate_to: Account to impersonate using domain-wide delegation of authority,
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
        hook = BigQueryReservationServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
            location=self.location,
        )

        resource_id = hook.generate_resource_id(
            dag_id=self.dag_id,
            task_id=self.task_id,
            logical_date=context["logical_date"],
        )

        hook.create_commitment_reservation_and_assignment(
            resource_id=resource_id,
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
            commitment_name = commitment.name if self.hook.commitment else None
            reservation_name = reservation.name if self.hook.reservation else None
            assignment_name = assignment.name if self.hook.assignment else None
            delete_commitment_reservation_and_assignment(
                commitment_name=commitment_name,
                reservation_name=reservation_name,
                assignment_name=assignment_name,
                slots=slot_capacity,
            )


class BigQueryReservationDeleteOperator(BaseOperator):
    """
    Delete BigQuery reservation and remove associated ressources.

    For BigQuery reservation API see here:
        https://cloud.google.com/bigquery/docs/reference/reservations


    :param location: Location where the reservation is attached.
    :param slots_provisioning: Slots number to delete.
    :param commitment_name: Commitment name
            e.g. `projects/myproject/locations/US/commitments/test`.
    :param reservation_name: Reservation name
            e.g. `projects/myproject/locations/US/reservations/test`.
    :param assignment_name: Assignment name
            e.g. `projects/myproject/locations/US/reservations/test/assignments/8950226598037373530`.
    :param gcp_conn_id: Connection ID used to connect to Google Cloud.
    :param delegate_to: Account to impersonate using domain-wide delegation of authority,
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
        hook = BigQueryReservationServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
            location=self.location,
        )

        hook.delete_commitment_reservation_and_assignment(
            commitment_name=self.commitment_name,
            reservation_name=self.reservation_name,
            assignment_name=self.assignment_name,
            slots=self.slots_provisioning,
        )


class BigQueryBiEngineReservationCreateOperator(BaseOperator):
    """
    Create or Update BI engine reservation.

    :param project_id: Google Cloud Project where the reservation is attached.
    :param location: Location where the reservation is attached.
    :param size: Memory size to reserve (GB).
    :param gcp_conn_id: Connection ID used to connect to Google Cloud.
    :param delegate_to: Account to impersonate using domain-wide delegation of authority,
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
        "size",
    )
    ui_color = bq_reservation_operator_color

    def __init__(
        self,
        project_id: str | None,
        location: str | None,
        size: int,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_on_kill: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.size = size
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.cancel_on_kill = cancel_on_kill
        self.hook: BigQueryHook | None = None

    def execute(self, context: Any) -> None:
        parent = f"projects/{self.project_id}/locations/{self.location}/biReservation"

        hook = BigQueryReservationServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
            location=self.location,
        )

        hook.create_bi_reservation(
            parent=parent,
            size=self.size,
        )


class BigQueryBiEngineReservationDeleteOperator(BaseOperator):
    """
    Delete or Update BI engine reservation.

    :param project_id: Google Cloud Project where the reservation is attached.
    :param location: Location where the reservation is attached.
    :param size: Memory size to reserve (GB).
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
        "size",
    )
    ui_color = bq_reservation_operator_color

    def __init__(
        self,
        project_id: str | None,
        location: str | None,
        size: int,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_on_kill: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.size = size
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.cancel_on_kill = cancel_on_kill
        self.hook: BigQueryHook | None = None

    def execute(self, context: Any) -> None:
        parent = f"projects/{self.project_id}/locations/{self.location}/biReservation"

        hook = BigQueryReservationServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
            location=self.location,
        )

        hook.delete_bi_reservation(
            parent=parent,
            size=self.size,
        )
