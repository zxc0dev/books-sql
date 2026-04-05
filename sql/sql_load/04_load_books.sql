INSERT INTO books (isbn, book_title, author_id, year_published, publisher_id)
    SELECT DISTINCT
        s.ISBN,
        s.book_title,
        a.id,
        NULLIF(CAST(s.year_of_publication AS INT), 0),
        p.id
    FROM staging_books s
    LEFT JOIN authors a ON a.author_name = s.book_author
    LEFT JOIN publishers p ON p.publisher_name = s.publisher
    WHERE s.ISBN IS NOT NULL
      AND s.book_title IS NOT NULL
      AND s.year_of_publication ~ '^\d+$';