{{ config(
    materialized='table',
    post_hook=[
        "CREATE OR REPLACE TRIGGER trg_audit_authors AFTER INSERT OR UPDATE OR DELETE ON dim.dim_authors FOR EACH ROW EXECUTE FUNCTION func_audit_log()"
    ]
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['author']) }} AS author_id,
    author AS author_name
FROM (
    SELECT DISTINCT author
    FROM {{ ref('stg_books') }}
    WHERE author IS NOT NULL
) a