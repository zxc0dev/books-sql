INSERT INTO books (isbn, book_title, author_id, year_published, publisher_id)
    SELECT DISTINCT
        s.ISBN,
        s.book_title,
        a.id,
        NULLIF(CAST(s.year_of_publication AS INT), 0),
        p.id
    FROM staging_books s
    JOIN authors a ON a.author_name = s.book_author
    JOIN publishers p ON p.publisher_name = s.publisher;