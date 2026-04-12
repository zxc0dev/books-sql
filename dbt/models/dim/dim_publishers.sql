{{ config(
    materialized='table',
    post_hook=[
        "CREATE OR REPLACE TRIGGER trg_audit_authors AFTER INSERT OR UPDATE OR DELETE ON dim.dim_authors FOR EACH ROW EXECUTE FUNCTION func_audit_log()"
    ]
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['publisher']) }} AS publisher_id,
    publisher AS publisher_name
FROM (
    SELECT DISTINCT publisher
    FROM {{ ref('stg_books') }}
    WHERE publisher IS NOT NULL
) p