CREATE DATABASE IF NOT EXISTS EAGLE_MovieLens_DB;
USE DATABASE EAGLE_MovieLens_DB;

CREATE SCHEMA IF NOT EXISTS EAGLE_MovieLens_DB.STAGING;
USE SCHEMA EAGLE_MovieLens_DB.STAGING;

CREATE OR REPLACE STAGE EAGLE_MovieLens_STAGE

----------------------------------------------------------

------------------- ratings.csv --------------------------
CREATE OR REPLACE TABLE ratings_staging (
    userId      NUMBER,
    movieId     NUMBER,
    rating      FLOAT,
    timestamp   TIMESTAMP_NTZ
);

------------------- movies.csv --------------------------
CREATE OR REPLACE TABLE movies_staging (
    movieId     NUMBER,
    title       VARCHAR,
    genres      VARCHAR
);

------------------- links.csv --------------------------
CREATE OR REPLACE TABLE links_staging (
    movieId     NUMBER,
    imdbId      NUMBER,
    tmdbId      NUMBER
);

------------------- user.csv --------------------------
CREATE OR REPLACE TABLE user_staging (
    userId      NUMBER,
    age         NUMBER
);

------------------- tags.csv --------------------------
CREATE OR REPLACE TABLE tags_staging (
    userId      NUMBER,
    movieId     NUMBER,
    tag         VARCHAR,
    timestamp   TIMESTAMP_NTZ
);

------------------ copy csv ---------------------------
COPY INTO ratings_staging
FROM @EAGLE_MovieLens_STAGE/ratings.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
);

COPY INTO movies_staging
FROM @EAGLE_MovieLens_STAGE/movies.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
);

COPY INTO user_staging
FROM @EAGLE_MovieLens_STAGE/user.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
);

COPY INTO tags_staging
FROM @EAGLE_MovieLens_STAGE/tags.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
);

COPY INTO links_staging
FROM @EAGLE_MovieLens_STAGE/links.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
);

//Overenie
SELECT COUNT(*) FROM ratings_staging;
SELECT * FROM ratings_staging LIMIT 300;

---------------- Vytvorenie tabuliek -----------------

//dim_users tabulka
CREATE OR REPLACE TABLE dim_users (
    user_key   NUMBER IDENTITY PRIMARY KEY,
    userId     NUMBER UNIQUE,
    age        NUMBER
);

//dim_movies
CREATE OR REPLACE TABLE dim_movies (
    movie_key   NUMBER IDENTITY PRIMARY KEY,
    movieId     NUMBER UNIQUE,
    title       VARCHAR,
    genres      VARCHAR
);

//fact_raitings 
CREATE OR REPLACE TABLE fact_raitings (
    rating_key   NUMBER IDENTITY PRIMARY KEY,
    user_key     NUMBER REFERENCES dim_users(user_key),
    movie_key    NUMBER REFERENCES dim_movies(movie_key),
    rating       FLOAT,
    timestamp    TIMESTAMP_NTZ
);

//dim_tags 
CREATE OR REPLACE TABLE dim_tags (
    tag_key     NUMBER IDENTITY PRIMARY KEY,
    user_key    NUMBER REFERENCES dim_users(user_key),
    movie_key   NUMBER REFERENCES dim_movies(movie_key),
    tag         VARCHAR,
    timestamp   TIMESTAMP_NTZ
);

//dim_links
CREATE OR REPLACE TABLE dim_links (
    link_key    NUMBER IDENTITY PRIMARY KEY,
    movie_key   NUMBER REFERENCES dim_movies(movie_key),
    imdbId      NUMBER,
    tmdbId      NUMBER
);

------------- naplnenie tabuliek -----------
INSERT INTO dim_users (userId, age)
SELECT DISTINCT userId, age
FROM user_staging;

INSERT INTO dim_movies (movieId, title, genres)
SELECT DISTINCT movieId, title, genres
FROM movies_staging;

INSERT INTO dim_tags (user_key, movie_key, tag, timestamp)
SELECT 
    u.user_key,
    m.movie_key,
    tg.tag,
    tg.timestamp
FROM tags_staging tg
JOIN dim_users u ON tg.userId = u.userId
JOIN dim_movies m ON tg.movieId = m.movieId;

INSERT INTO dim_links (movie_key, imdbId, tmdbId)
SELECT 
    m.movie_key,
    l.imdbId,
    l.tmdbId
FROM links_staging l
JOIN dim_movies m ON l.movieId = m.movieId;


INSERT INTO fact_raitings (user_key, movie_key, rating, timestamp)
SELECT 
    u.user_key,
    m.movie_key,
    r.rating,
    r.timestamp
FROM ratings_staging r
JOIN dim_users u ON r.userId = u.userId
JOIN dim_movies m ON r.movieId = m.movieId;


//Overenie
SELECT COUNT(*) FROM dim_users;
SELECT COUNT(*) FROM dim_movies;
SELECT COUNT(*) FROM dim_tags;
SELECT COUNT(*) FROM dim_links;

SELECT COUNT(*) FROM fact_raitings;


  