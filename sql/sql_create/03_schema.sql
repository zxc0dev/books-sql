DROP TABLE IF EXISTS authors CASCADE;
DROP TABLE IF EXISTS publishers CASCADE;
DROP TABLE IF EXISTS books CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS ratings CASCADE;
DROP TABLE IF EXISTS audit_log CASCADE;

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
    id PRIMARY KEY,
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

CREATE TABLE audit_log (
    id SERIAL PRIMARY KEY,
    entity_table VARCHAR(50) NOT NULL,
    entity_id INT NOT NULL,
    action VARCHAR(10) NOT NULL CHECK (action IN ('INSERT','UPDATE','DELETE')),
    changed_by INT,
    changed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    old_data JSONB,
    new_data JSONB,
    ip_address INET
);