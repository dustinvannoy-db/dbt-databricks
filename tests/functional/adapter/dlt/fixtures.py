from weakref import ref


source_csv = """name,title
"Elia","Ms"
"Teo","Mr"
"Fang","Ms"
"Elbert","Dr"
"Mia","Ms"
"Theresa","Dr"
"""

expected_csv = """title,count
"Ms",3
"Mr",1
"Dr",2
"""

dlt_notebook = """
CREATE OR REFRESH MATERIALIZED VIEW title_count(
    CONSTRAINT valid_title_count EXPECT count > 0
)
AS (
    SELECT title, COUNT(*) as count
    FROM {{ ref('source') }}
    GROUP BY title
)
"""

base_schema = """
version: 2

models:
  - name: title_count
    config:
        materialized: dlt_notebook
        upload_path: "Shared/dlt/test_pipeline"
"""

expected_ref_csv = """title,count
"Mr",1
"""

ref_schema = """
version: 2

models:
  - name: title_count
    config:
        materialized: dlt_notebook
  - name: dependent
    config:
        materialized: table
"""

dependent = """
select * from {{ ref('title_count') }} where title = 'Mr'
"""
