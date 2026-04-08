CREATE TABLE quarantine_books (
    ISBN VARCHAR,
    book_title VARCHAR,
    book_author VARCHAR,
    year_of_publication VARCHAR,
    publisher VARCHAR,
    image_url_s VARCHAR,
    image_url_m VARCHAR,
    image_url_l VARCHAR,
    reason VARCHAR(500),        
    quarantined_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE quarantine_users (
    user_id INT,
    location VARCHAR,
    age INT,
    reason VARCHAR(500),
    quarantined_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE quarantine_ratings (
    user_id INT,
    ISBN VARCHAR,
    rating SMALLINT,
    reason VARCHAR(500),
    quarantined_at TIMESTAMP DEFAULT NOW()
);