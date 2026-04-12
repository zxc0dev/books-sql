{{ config(materialized='view') }}

SELECT
    book_id,
    COUNT(*) AS rating_count,
    AVG(rating) AS avg_rating
FROM {{ ref('fact_ratings') }}
GROUP BY book_id