INSERT INTO authors (author_name)
    SELECT
        DISTINCT book_author AS author_name
    FROM
        staging_books
    WHERE
        book_author IS NOT NULL;