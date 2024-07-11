import pytest
import re
import os
from mock import Mock, ANY
from tests.unit.api_client.api_test_base import ApiTestBase
from dbt.adapters.databricks.api_client import DltApi

class TestDltApi(ApiTestBase):
    @pytest.fixture
    def api(self, session, host):
        return DltApi(session, host, 1, 2)

    @pytest.fixture
    def integration_session(self):
        import os
        import requests

        host = os.getenv("DBT_DATABRICKS_HOST")
        token = os.getenv("DBT_DATABRICKS_TOKEN")

        auth = ('token', token)
        headers = {'Host': host, 'Content-Type': 'application/json'}

        my_session = requests.Session()
        my_session.auth = auth
        my_session.headers = headers
        return my_session

    def test_create_integration__200(self, api, session):
        import os
        import requests

        host = os.getenv("DBT_DATABRICKS_HOST")
        token = os.getenv("DBT_DATABRICKS_TOKEN")

        auth = ('token', token)
        headers = {'Host': host, 'Content-Type': 'application/json'}

        my_session = requests.Session()
        my_session.auth = auth
        my_session.headers = headers

        my_name = "my_dlt_pipeline6"
        my_notebook_path = "/Repos/dustin.vannoy@databricks.com/workshop_production_delta_official/notebooks/02-DLT for Data Engineering Pipelines (Python)"
        my_catalog = "main"
        my_schema = "dustin_vannoy"

        my_api = DltApi(my_session, host, 1, 2)
        uuid_regex = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z')
        assert bool(uuid_regex.match(my_api.create(my_name, my_notebook_path, my_catalog, my_schema)))

    def test_get_integration__200(self, api, integration_session):
        my_name = "my_dlt_pipeline6"
        my_notebook_path = "/Repos/dustin.vannoy@databricks.com/workshop_production_delta_official/notebooks/02-DLT for Data Engineering Pipelines (Python)"

        host = os.getenv("DBT_DATABRICKS_HOST")
        my_api = DltApi(integration_session, host, 1, 2)
        uuid_regex = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z')
        assert bool(uuid_regex.match(my_api.get_pipeline(my_name)['pipeline_id']))
        # assert my_api.get_pipeline(my_name, my_notebook_path) == {"pipeline_id": "pipeline_id"}

    def test_update_integration__200(self, api, integration_session):
        my_pipeline_id = "0e116d3-9c36-4a1f-ab04-ad943b69b3e3"
        my_name = "my_dlt_pipeline6"
        my_notebook_path = "/Repos/user@databricks.com/example-repo/DLT_job"
        my_catalog = "main"
        my_schema = "dustin_vannoy"

        host = os.getenv("DBT_DATABRICKS_HOST")
        my_api = DltApi(integration_session, host, 1, 2)
        assert my_api.update(my_pipeline_id, my_name, my_notebook_path, my_catalog, my_schema) == "pipeline_id"

    def test_create___200(self, api, session, host):
        my_name = "my_dlt_pipeline"
        my_notebook_path = "/Repos/user@databricks.com/example-repo/DLT_job"
        my_catalog = "main"
        my_schema = "dustin_vannoy"

        session.post.return_value.status_code = 200
        session.post.return_value.json.return_value = {"pipeline_id": "pipeline_id"}
        assert api.create(my_name, my_notebook_path, my_catalog, my_schema) == "pipeline_id"
        session.post.assert_called_once_with(
            f"https://{host}/api/2.0/pipelines",
            json={'name': 'my_dlt_pipeline', 'catalog': 'main', 'target': 'dustin_vannoy', 'libraries': [{'notebook': {'path': '/Repos/user@databricks.com/example-repo/DLT_job'}}], 'continuous': ANY, 'edition': ANY, 'configuration': ANY, 'serverless': True},
            params=None,
        )

    def test_update_200(self, api, session, host):
        my_name = "my_dlt_pipeline"
        my_notebook_path = "/Repos/user@databricks.com/example-repo/DLT_job"
        my_catalog = "main"
        my_schema = "dustin_vannoy"
        session.post.return_value.status_code = 200
        session.post.return_value.json.return_value = {"pipeline_id": "pipeline_id"}

        assert api.update(my_name, my_notebook_path, my_catalog, my_schema) == "pipeline_id"
        session.post.assert_called_once_with(
            f"https://{host}/api/2.0/pipelines/pipeline_id",
            json={'name': 'my_dlt_pipeline', 'catalog': 'main', 'target': 'dustin_vannoy',
                  'libraries': [{'notebook': {'path': '/Repos/user@databricks.com/example-repo/DLT_job'}}],
                  'continuous': ANY, 'edition': ANY, 'configuration': ANY, 'serverless': True},
            params=None,
        )
        pass

