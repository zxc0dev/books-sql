SELECT
    TRIM(isbn)                          AS isbn,
    TRIM(book_title)                    AS title,
    INITCAP(TRIM(book_author))          AS author,
    NULLIF(CAST(year_of_publication AS INT), 0) AS year_of_publication,
    INITCAP(TRIM(publisher))            AS publisher
FROM raw_books
WHERE isbn IS NOT NULL
    AND isbn != ''