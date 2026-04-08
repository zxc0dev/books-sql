SELECT 
    b.id,
    b.book_title,
    AVG(r.rating) AS average_rating,
    COUNT(r.rating) AS total_ratings
FROM books b
LEFT JOIN ratings r ON b.id = r.book_id
GROUP BY b.id, b.book_title
HAVING AVG(r.rating) IS NOT NULL
ORDER BY average_rating DESC, total_ratings DESC;