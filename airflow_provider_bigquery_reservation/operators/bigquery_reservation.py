"""This module contains Google BigQuery reservation operators."""
from __future__ import annotations
from typing import Any, Sequence

from airflow.models import BaseOperator
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


    :param project_id: Google Cloud Project where the reservation is assigned.
    :param reservation_project_id: Google Cloud Project where the reservation is set.
    :param location: Location where the reservation is attached.
    :param slots_provisioning: Slots number to provision. Slots can only be reserved in increments of 100.
    :param commitments_duration: Commitment minimum durations i.e. one minute (FLEX, default), one month (MONTH) or one year (YEAR).
    :param assignment_job_type: Commitment assignment job type (PIPELINE, QUERY, ML_EXTERNAL, BACKGROUND)
    :param gcp_conn_id: Connection ID used to connect to Google Cloud.
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
        "reservation_project_id",
        "location",
        "slots_provisioning",
        "commitments_duration",
    )
    ui_color = bq_reservation_operator_color

    def __init__(
        self,
        project_id: str | None,
        location: str,
        slots_provisioning: int,
        reservation_project_id: str | None = None,
        commitments_duration: str = "FLEX",
        assignment_job_type: str = "QUERY",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_on_kill: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.reservation_project_id = reservation_project_id
        self.location = location
        self.slots_provisioning = slots_provisioning
        self.commitments_duration = commitments_duration
        self.assignment_job_type = assignment_job_type
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.cancel_on_kill = cancel_on_kill
        self.hook: BigQueryReservationServiceHook | None = None

    def execute(self, context: Any) -> None:
        """Create a slot reservation."""
        self.hook = BigQueryReservationServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            location=self.location,
        )

        self.hook.create_commitment_reservation_and_assignment(
            slots=self.slots_provisioning,
            assignment_job_type=self.assignment_job_type,
            commitments_duration=self.commitments_duration,
            project_id=self.project_id,
            reservation_project_id=self.reservation_project_id,
        )

        context["ti"].xcom_push(
            key="commitment_name", value=self.hook._get_commitment().name
        )
        context["ti"].xcom_push(
            key="reservation_name", value=self.hook._get_reservation().name
        )
        context["ti"].xcom_push(
            key="assignment_name", value=self.hook._get_assignment().name
        )

    def on_kill(self) -> None:
        """Delete the reservation if task is cancelled."""
        super().on_kill()
        if self.hook is not None:
            commitment_name = (
                self.hook.commitment.name if self.hook.commitment else None
            )
            reservation_name = (
                self.hook.reservation.name if self.hook.reservation else None
            )
            assignment_name = (
                self.hook.assignment.name if self.hook.assignment else None
            )
            if commitment_name:
                self.hook.delete_commitment_reservation_and_assignment(
                    commitment_name=commitment_name,
                    reservation_name=reservation_name,
                    assignment_name=assignment_name,
                    slots=self.slots_provisioning,
                )


class BigQueryReservationDeleteOperator(BaseOperator):
    """
    Delete BigQuery reservation and remove associated ressources.

    For BigQuery reservation API see here:
        https://cloud.google.com/bigquery/docs/reference/reservations


    :param location: Location where the reservation is attached.
    :param project_id: Google Cloud Project where the reservation is assigned.
    :param reservation_project_id: Google Cloud Project where the reservation is set.
    :param slots_provisioning: Slots number to delete.
    :param commitment_name: Commitment name
            e.g. `projects/myproject/locations/US/commitments/test`.
    :param reservation_name: Reservation name
            e.g. `projects/myproject/locations/US/reservations/test`.
    :param assignment_name: Assignment name
            e.g. `projects/myproject/locations/US/reservations/test/assignments/8950226598037373530`.
    :param gcp_conn_id: Connection ID used to connect to Google Cloud.
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
        "project_id",
        "reservation_project_id",
        "slots_provisioning",
        "commitment_name",
        "reservation_name",
        "assignment_name",
    )
    ui_color = bq_reservation_operator_color

    def __init__(
        self,
        location: str,
        project_id: str | None = None,
        reservation_project_id: str | None = None,
        slots_provisioning: int | None = None,
        commitment_name: str | None = None,
        reservation_name: str | None = None,
        assignment_name: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_on_kill: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.project_id = project_id
        self.reservation_project_id = reservation_project_id
        self.slots_provisioning = slots_provisioning
        self.commitment_name = commitment_name
        self.reservation_name = reservation_name
        self.assignment_name = assignment_name
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.cancel_on_kill = cancel_on_kill

    def execute(self, context: Any):
        """Delete a slot reservation."""
        hook = BigQueryReservationServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            location=self.location,
        )

        if self.commitment_name or self.reservation_name or self.assignment_name:
            assert (
                self.slots_provisioning
            ), "Need to define `slots_provisioning`: Number of slots to delete"
            hook.delete_commitment_reservation_and_assignment(
                commitment_name=self.commitment_name,
                reservation_name=self.reservation_name,
                assignment_name=self.assignment_name,
                slots=self.slots_provisioning,
            )
        else:
            reservation_project_id = self.reservation_project_id or self.project_id
            self.log.info(
                f"The reservation deleted is on the GCP project: {reservation_project_id}"
            )
            assert (
                self.project_id and reservation_project_id
            ), "Need to define `project_id` i.e. the project owns the commitments."
            self.log.info(
                "Delete all reservations on"
                f" projects/{self.reservation_project_id}/locations/{self.location}"
            )
            hook.delete_commitments_assignment_associated(
                project_id=self.project_id,
                location=self.location,
                reservation_project_id=reservation_project_id,
            )


class BigQueryBiEngineReservationCreateOperator(BaseOperator):
    """
    Create or Update BI engine reservation.

    :param location: The BI engine reservation location. (templated)
    :param size: The BI Engine reservation memory size (GB). (templated)
    :param project_id: (Optional) The name of the project where the reservation
        will be attached from. (templated)
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud. (templated)
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account. (templated)
    :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called
    """

    template_fields: Sequence[str] = (
        "project_id",
        "location",
        "size",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        location: str,
        size: int,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_on_kill: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.size = size
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.cancel_on_kill = cancel_on_kill

    def execute(self, context: Any) -> None:
        """Create a BI Engine reservation."""
        hook = BigQueryReservationServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            location=self.location,
        )

        hook.create_bi_reservation(project_id=self.project_id, size=self.size)


class BigQueryBiEngineReservationDeleteOperator(BaseOperator):
    """
    Delete or Update BI engine reservation.

    :param project_id: Google Cloud Project where the reservation is attached. (templated)
    :param location: Location where the reservation is attached. (templated)
    :param size: (Optional) BI Engine reservation size to delete (GB).
    :param gcp_conn_id: The connection ID used to connect to Google Cloud. (templated)
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account. (templated)
    :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called
    """

    template_fields: Sequence[str] = (
        "project_id",
        "location",
        "size",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str | None,
        location: str,
        size: int | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_on_kill: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.size = size
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.cancel_on_kill = cancel_on_kill

    def execute(self, context: Any) -> None:
        """Delete BI Engine reservation."""
        hook = BigQueryReservationServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            location=self.location,
        )

        hook.delete_bi_reservation(
            project_id=self.project_id,
            size=self.size,
        )
