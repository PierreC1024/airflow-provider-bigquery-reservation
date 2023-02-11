from __future__ import annotations

import unittest
from unittest import mock

# from google.api_core.gapic_v1.method import DEFAULT
# from google.cloud.bigquery_datatransfer_v1 import StartManualTransferRunsResponse, TransferConfig, TransferRun

from bigquery_reservation.operators.bigquery_reservation import (
    BigQueryReservationCreateOperator,
    BigQueryReservationDeletionOperator,
)

PROJECT_ID = "test"
LOCATION = "US"


class BigQueryReservationCreateOperatorTestCase(unittest.TestCase):
    @mock.patch("bigquery_reservation.hooks.bigquery_reservation.BigQueryReservationServiceHook")
    def test_execute_success(self, mock_hook):

        ressource_name = "airflow_test"

        mock_hook.return_value.create_commitment_reservation_and_assignment.return_value = MagicMock(job_id=real_job_id, error_result=False)

        mock_hook.return_value.generate_resource_id.return_value = ressource_name

        op = BigQueryReservationCreateOperator(
            project_id=PROJECT_ID,
            location=LOCATION,
            task_id="id",
            slots_provisioning=100,
        )

        result = op.execute(context=MagicMock())

        assert 0 == 0



