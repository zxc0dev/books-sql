INSERT INTO users (id, location, age)
    SELECT
        user_id,
        location,
        NULLIF(CAST(age AS FLOAT), 0)
    FROM
        staging_users;