-- SCHEMAS
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS fact;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS quarantine;

-- DIM TABLES (no dependencies)
CREATE TABLE IF NOT EXISTS dim.dim_authors (
    author_id    VARCHAR(32) PRIMARY KEY,
    author_name  VARCHAR(500) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim.dim_publishers (
    publisher_id   VARCHAR(32) PRIMARY KEY,
    publisher_name VARCHAR(500) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim.dim_users (
    user_id   VARCHAR(32) PRIMARY KEY,
    source_id INT NOT NULL,
    location  VARCHAR(500),
    age       INT CHECK (age >= 0)
);

CREATE TABLE IF NOT EXISTS dim.dim_books (
    book_id        VARCHAR(32) PRIMARY KEY,
    isbn           VARCHAR(20) NOT NULL UNIQUE,
    book_title     VARCHAR(500) NOT NULL,
    author_id      VARCHAR(32) REFERENCES dim.dim_authors(author_id)    ON DELETE CASCADE ON UPDATE CASCADE,
    publisher_id   VARCHAR(32) REFERENCES dim.dim_publishers(publisher_id) ON DELETE CASCADE ON UPDATE CASCADE,
    year_published INT
);

-- FACT TABLES (depends on dim)
CREATE TABLE IF NOT EXISTS fact.fact_ratings (
    rating_id VARCHAR(32) PRIMARY KEY,
    user_id   VARCHAR(32) NOT NULL REFERENCES dim.dim_users(user_id)   ON DELETE CASCADE ON UPDATE CASCADE,
    book_id   VARCHAR(32) NOT NULL REFERENCES dim.dim_books(book_id)   ON DELETE CASCADE ON UPDATE CASCADE,
    rating    SMALLINT NOT NULL CHECK (rating >= 0 AND rating <= 10),
    UNIQUE (user_id, book_id)
);

-- AUDIT
CREATE TABLE IF NOT EXISTS public.audit_log (
    id           SERIAL PRIMARY KEY,
    entity_table VARCHAR(100) NOT NULL,
    entity_id    TEXT,
    action       VARCHAR(10) NOT NULL CHECK (action IN ('INSERT','UPDATE','DELETE')),
    changed_by   INT,
    changed_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    old_data     JSONB,
    new_data     JSONB,
    ip_address   INET
);