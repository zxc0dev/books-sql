CREATE TABLE staging_books (
    ISBN VARCHAR(20) NOT NULL UNIQUE,
    book_title VARCHAR NOT NULL,
    book_author VARCHAR,
    year_of_publication VARCHAR,
    publisher VARCHAR,
    image_url_s VARCHAR,
    image_url_m VARCHAR,
    image_url_l VARCHAR
);

CREATE TABLE staging_users (
    user_id INT,
    location VARCHAR(1000),
    age FLOAT
);

CREATE TABLE staging_ratings (
    user_id INT,
    ISBN VARCHAR(1000),
    rating SMALLINT
);