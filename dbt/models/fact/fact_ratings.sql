{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['u.user_id', 'b.book_id']) }} AS rating_id,
    u.user_id,
    b.book_id,
    s.rating
FROM {{ ref('stg_ratings') }} s
JOIN {{ ref('dim_users') }} u  ON u.source_id = s.user_id
JOIN {{ ref('dim_books') }} b  ON b.isbn       = s.isbn