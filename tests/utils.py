"""utils functions for mock."""


def mock_base_gcp_hook_no_default_project_id(
    self,
    gcp_conn_id="google_cloud_default",
    impersonation_chain=None,
):
    """Mock base gcp hook."""
    self.extras_list = {}
    self._conn = gcp_conn_id
    self.impersonation_chain = impersonation_chain
    self._client = None
    self._conn = None
    self._cached_credentials = None
    self._cached_project_id = None


class QueryJob:
    """Query Job Mock."""

    def __init__(self, reservation_id: bool) -> None:
        id_ = "test" if reservation_id else None
        self._properties = {"statistics": {"reservation_id": id_}}
