{{ config(
    materialized='table',
    post_hook=[
        "CREATE OR REPLACE TRIGGER trg_audit_authors AFTER INSERT OR UPDATE OR DELETE ON dim.dim_authors FOR EACH ROW EXECUTE FUNCTION func_audit_log()"
    ]
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['id']) }} AS user_id,
    id AS source_id,
    location,
    age
FROM {{ ref('stg_users') }}