INSERT INTO publishers (publisher_name)
    SELECT
        DISTINCT publisher AS publisher_name
    FROM
        staging_books
    WHERE
        publisher IS NOT NULL;