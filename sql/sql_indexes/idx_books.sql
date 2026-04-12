DROP INDEX IF EXISTS idx_books_author_id;
DROP INDEX IF EXISTS idx_books_publisher_id;
CREATE INDEX IF NOT EXISTS idx_books_author_id ON books(author_id);
CREATE INDEX IF NOT EXISTS idx_books_publisher_id ON books(publisher_id);