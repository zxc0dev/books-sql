COPY staging_books FROM 'D:\projects\sql-books\book-recommendation-dataset\versions\3\Books.csv' DELIMITER ',' CSV HEADER;

COPY staging_users FROM 'D:\projects\sql-books\book-recommendation-dataset\versions\3\Users.csv' DELIMITER ',' CSV HEADER;

COPY staging_ratings FROM 'D:\projects\sql-books\book-recommendation-dataset\versions\3\Ratings.csv' DELIMITER ',' CSV HEADER;