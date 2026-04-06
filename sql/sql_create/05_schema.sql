CREATE TABLE authors (
    id SERIAL PRIMARY KEY,
    author_name VARCHAR(255) NOT NULL
);

CREATE TABLE publishers (
    id SERIAL PRIMARY KEY,
    publisher_name VARCHAR(255) NOT NULL
);

CREATE TABLE books (
    id SERIAL PRIMARY KEY,
    ISBN VARCHAR(20) NOT NULL UNIQUE,
    book_title VARCHAR(255) NOT NULL,
    author_id INT NOT NULL,
    year_published INT,
    publisher_id INT NOT NULL,

    FOREIGN KEY (author_id) REFERENCES authors(id),
    FOREIGN KEY (publisher_id) REFERENCES publishers(id)
);

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    location VARCHAR(255),
    age FLOAT CHECK (age >= 0)
);

CREATE TABLE ratings (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    book_id INT NOT NULL,
    rating SMALLINT CHECK (rating >= 0 AND rating <= 10) NOT NULL,

    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (book_id) REFERENCES books(id),
    UNIQUE (user_id, book_id)

);



