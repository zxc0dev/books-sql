{{ config(
    materialized='table',
    post_hook=[
        "CREATE OR REPLACE TRIGGER trg_audit_authors AFTER INSERT OR UPDATE OR DELETE ON dim.dim_authors FOR EACH ROW EXECUTE FUNCTION func_audit_log()"
    ]
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['s.isbn']) }} AS book_id,
    s.isbn,
    s.title AS book_title,
    a.author_id,
    p.publisher_id,
    s.year_of_publication AS year_published
FROM {{ ref('stg_books') }} s
JOIN {{ ref('dim_authors') }} a    ON a.author_name    = s.author
JOIN {{ ref('dim_publishers') }} p ON p.publisher_name = s.publisher