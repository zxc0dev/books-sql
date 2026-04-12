DROP TABLE IF EXISTS raw_books CASCADE;
DROP TABLE IF EXISTS raw_users CASCADE;
DROP TABLE IF EXISTS raw_ratings CASCADE;

CREATE TABLE raw_books (
    ISBN VARCHAR,
    book_title VARCHAR,
    book_author VARCHAR,
    year_of_publication VARCHAR,
    publisher VARCHAR,
    image_url_s VARCHAR,
    image_url_m VARCHAR,
    image_url_l VARCHAR
);

CREATE TABLE raw_users (
    user_id INT,
    location VARCHAR,
    age INT
);

CREATE TABLE raw_ratings (
    user_id INT,
    ISBN VARCHAR,
    rating SMALLINT
);