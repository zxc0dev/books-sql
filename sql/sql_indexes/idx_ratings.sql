DROP INDEX IF EXISTS idx_ratings_user_id;
DROP INDEX IF EXISTS idx_ratings_book_id;
CREATE INDEX IF NOT EXISTS idx_ratings_user_id ON fact.fact_ratings(user_id);
CREATE INDEX IF NOT EXISTS idx_ratings_book_id ON fact.fact_ratings(book_id);