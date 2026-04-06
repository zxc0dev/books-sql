COPY staging_books FROM 'D:\projects\sql-books\data\02_processed\books_good.csv' DELIMITER ',' CSV HEADER;

COPY staging_users FROM 'D:\projects\sql-books\data\02_processed\users_good.csv' DELIMITER ',' CSV HEADER;

COPY staging_ratings FROM 'D:\projects\sql-books\data\02_processed\ratings_good.csv' DELIMITER ',' CSV HEADER;