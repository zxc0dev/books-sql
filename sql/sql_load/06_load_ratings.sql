INSERT INTO ratings (user_id, book_id, rating)
    SELECT
        u.id AS user_id,
        b.id AS book_id,
        s.rating

    FROM staging_ratings s
    JOIN users u ON s.user_id = u.id
    JOIN books b ON s.ISBN = b.ISBN;