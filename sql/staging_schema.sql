CREATE TABLE staging_books (
    ISBN VARCHAR(20),
    book_title VARCHAR(1000),
    book_author VARCHAR(1000),
    year_of_publication VARCHAR(1000),
    publisher VARCHAR(1000),
    image_url_s VARCHAR(1000),
    image_url_m VARCHAR(1000),
    image_url_l VARCHAR(1000)
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