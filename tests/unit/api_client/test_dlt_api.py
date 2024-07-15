import pytest
from mock import ANY
from tests.unit.api_client.api_test_base import ApiTestBase
from dbt.adapters.databricks.api_client import DltApi

class TestDltApi(ApiTestBase):
    @pytest.fixture
    def api(self, session, host):
        return DltApi(session, host, 1, 2)

    def test_create___200(self, api, session, host):
        my_name = "my_dlt_pipeline"
        my_notebook_path = "/Repos/user@databricks.com/example-repo/DLT_job"
        my_catalog = "main"
        my_schema = "dustin_vannoy"

        session.post.return_value.status_code = 200
        session.post.return_value.json.return_value = {"pipeline_id": "pipeline_id"}
        clusters = [{"label": "default","autoscale": {"min_workers": 1, "max_workers": 1, "mode": "ENHANCED"}}]
        assert api.create(my_name, my_notebook_path, my_catalog, my_schema, True, False, clusters) == "pipeline_id"
        session.post.assert_called_once_with(
            f"https://{host}/api/2.0/pipelines",
            json={'name': 'my_dlt_pipeline', 'catalog': 'main', 'target': 'dustin_vannoy', 'libraries': [{'notebook': {'path': '/Repos/user@databricks.com/example-repo/DLT_job'}}], 'development': ANY, 'continuous': ANY, 'edition': ANY, 'configuration': ANY, 'serverless': ANY, 'clusters': ANY},
            params=None,
        )

    def test_get_200(self, api, session, host):
        my_name = "my_dlt_pipeline"
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {
            "statuses": [{"name": "my_dlt_pipeline", "pipeline_id": "pipeline_id"}]}

        assert api.get_pipeline(my_name) == "pipeline_id"
        session.get.assert_called_once_with(
            f"https://{host}/api/2.0/pipelines?filter=name LIKE 'my_dlt_pipeline'",
            json={},
            params=None,
        )

    def test_update_200(self, api, session, host):
        my_name = "my_dlt_pipeline"
        my_notebook_path = "/Repos/user@databricks.com/example-repo/DLT_job"
        my_catalog = "main"
        my_schema = "dustin_vannoy"
        session.put.return_value.status_code = 200
        session.put.return_value.json.return_value = {"pipeline_id": "pipeline_id"}

        assert api.update("pipeline_id", my_name, my_notebook_path, my_catalog, my_schema, True, True, {}) is None
        session.put.assert_called_once_with(
            f"https://{host}/api/2.0/pipelines/pipeline_id",
            json={'name': 'my_dlt_pipeline', 'catalog': 'main', 'target': 'dustin_vannoy',
                  'libraries': [{'notebook': {'path': '/Repos/user@databricks.com/example-repo/DLT_job'}}],
                  'development': ANY, 'continuous': ANY, 'edition': ANY, 'configuration': ANY, 'serverless': True},
            params=None,
        )


