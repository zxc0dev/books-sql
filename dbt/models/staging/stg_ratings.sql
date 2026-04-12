SELECT
    user_id,
    isbn,
    CAST(rating AS SMALLINT)            AS rating
FROM raw_ratings
WHERE rating IS NOT NULL