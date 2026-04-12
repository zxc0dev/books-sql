SELECT
    user_id                             AS id,
    TRIM(LOWER(location))               AS location,
    NULLIF(CAST(age AS INT), 0)         AS age
FROM raw_users
WHERE user_id IS NOT NULL