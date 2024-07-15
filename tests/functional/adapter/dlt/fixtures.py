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
    CONSTRAINT valid_titles EXPECT (title is not null AND title in ('Ms', 'Mr', 'Dr'))
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
        development: True
        serverless: False
        clusters: [{"label": "default","autoscale": {"min_workers": 1, "max_workers": 1, "mode": "ENHANCED"}}]
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
        development: True
        serverless: False
        clusters: [{"label": "default","autoscale": {"min_workers": 1, "max_workers": 1, "mode": "ENHANCED"}}]
  - name: dependent
    config:
        materialized: table
"""

dependent = """
select * from {{ ref('title_count') }} where title = 'Mr'
"""

expected_updated_csv = """title,count
"Ms",3
"Mr",1
"""

updated_dlt_notebook = """
CREATE OR REFRESH MATERIALIZED VIEW title_count(
    CONSTRAINT valid_titles EXPECT (title is not null AND title in ('Ms', 'Mr', 'Dr'))
)
AS (
    SELECT title, COUNT(*) as count
    FROM {{ ref('source') }}
    WHERE title != 'Dr'
    GROUP BY title
)
"""
