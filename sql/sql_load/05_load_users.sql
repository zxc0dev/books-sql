INSERT INTO users (id, location, age)
    SELECT
        user_id,
        location,
        NULLIF(CAST(age AS INT), 0)
    FROM
        staging_users
    WHERE
        user_id IS NOT NULL;