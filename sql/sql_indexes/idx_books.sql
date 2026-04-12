DROP INDEX IF EXISTS idx_books_author_id;
DROP INDEX IF EXISTS idx_books_publisher_id;
CREATE INDEX IF NOT EXISTS idx_books_author_id ON dim.dim_books(author_id);
CREATE INDEX IF NOT EXISTS idx_books_publisher_id ON dim.dim_books(publisher_id);