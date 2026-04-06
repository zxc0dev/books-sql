CREATE TABLE staging_books (
    ISBN VARCHAR,
    book_title VARCHAR,
    book_author VARCHAR,
    year_of_publication VARCHAR,
    publisher VARCHAR,
    image_url_s VARCHAR,
    image_url_m VARCHAR,
    image_url_l VARCHAR
);

CREATE TABLE staging_users (
    user_id INT,
    location VARCHAR,
    age FLOAT
);

CREATE TABLE staging_ratings (
    user_id INT,
    ISBN VARCHAR,
    rating SMALLINT
);