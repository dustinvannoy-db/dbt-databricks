{% materialization dlt_notebook, adapter='databricks' %}
    {% do log("Building dlt pipeline") %}
    {%- set identifier = model['alias'] -%}
    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {% set target_relation = this.incorporate(type=this.DltNotebook) %}
    {{ adapter.execute_dlt_model(target_relation, config.model.config.extra, compiled_code, old_relation) }}
    {{ dlt_notebook_execute_no_op(target_relation) }}
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

{% macro dlt_notebook_execute_no_op(target_relation) %}
    {% do store_raw_result(
        name="main",
        message="Created " ~ target_relation ~ " through REST.",
        code="skip",
        rows_affected="-1"
    ) %}
{% endmacro %}