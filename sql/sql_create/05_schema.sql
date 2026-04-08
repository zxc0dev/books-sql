CREATE TABLE authors (
    id SERIAL PRIMARY KEY,
    author_name VARCHAR(500) NOT NULL
);

CREATE TABLE publishers (
    id SERIAL PRIMARY KEY,
    publisher_name VARCHAR(500) NOT NULL
);

CREATE TABLE books (
    id SERIAL PRIMARY KEY,
    ISBN VARCHAR(20) NOT NULL UNIQUE,
    book_title VARCHAR(500) NOT NULL,
    author_id INT NOT NULL,
    year_published INT,
    publisher_id INT NOT NULL,

    FOREIGN KEY (author_id) REFERENCES authors(id) 
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (publisher_id) REFERENCES publishers(id) 
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    location VARCHAR(500),
    age INT CHECK (age >= 0)
);

CREATE TABLE ratings (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    book_id INT NOT NULL,
    rating SMALLINT CHECK (rating >= 0 AND rating <= 10) NOT NULL,

    FOREIGN KEY (user_id) REFERENCES users(id) 
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (book_id) REFERENCES books(id) 
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    UNIQUE (user_id, book_id)

);


-- Indexes for the main tables to improve query performance 
CREATE INDEX idx_books_author_id ON books(author_id);
CREATE INDEX idx_books_publisher_id ON books(publisher_id);
CREATE INDEX idx_ratings_user_id ON ratings(user_id);
CREATE INDEX idx_ratings_book_id ON ratings(book_id);