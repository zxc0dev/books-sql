COPY staging_books FROM 'D:\projects\sql-books\data\01_raw\Books.csv' DELIMITER ',' CSV HEADER;

COPY staging_users FROM 'D:\projects\sql-books\data\01_raw\Users.csv' DELIMITER ',' CSV HEADER;

COPY staging_ratings FROM 'D:\projects\sql-books\data\01_raw\Ratings.csv' DELIMITER ',' CSV HEADER;