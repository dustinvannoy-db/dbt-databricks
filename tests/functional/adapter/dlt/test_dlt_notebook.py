import pytest

from tests.functional.adapter.dlt import fixtures
from dbt.tests import util


class BaseTestDltNotebook:
    @pytest.fixture(autouse=True)
    def cleanup(self, adapter):
        yield
        results = util.run_sql_with_adapter(
            adapter, "SHOW TBLPROPERTIES {database}.{schema}.title_count", fetch="all"
        )
        for result in results:
            if result[0] == "pipelines.pipelineId":
                adapter.connections.delete_dlt_pipeline(result[1])


class TestDltNotebook(BaseTestDltNotebook):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"source.csv": fixtures.source_csv, "expected.csv": fixtures.expected_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": fixtures.base_schema, "title_count.sql": fixtures.dlt_notebook}

    def test_dlt_notebook(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run"])

        util.check_relations_equal(project.adapter, ["expected", "title_count"])


class TestReferencingDltNotebook(BaseTestDltNotebook):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"source.csv": fixtures.source_csv, "expected.csv": fixtures.expected_ref_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": fixtures.ref_schema,
            "title_count.sql": fixtures.dlt_notebook,
            "dependent.sql": fixtures.dependent,
        }

    def test_dlt_notebook_reference(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run"])

        util.check_relations_equal(project.adapter, ["expected", "dependent"])
