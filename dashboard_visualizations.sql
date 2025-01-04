-- Graf 1: Najlepšie hodnotené filmy (Top 10)
SELECT 
    m.title,
    AVG(f.rating) AS avg_rating,
    COUNT(f.rating) AS num_ratings
FROM 
    fact_raitings f
JOIN 
    dim_movies m ON f.movie_key = m.movie_key
GROUP BY 
    m.title
HAVING 
    COUNT(f.rating) >= 10 -- Zahrň len filmy s aspoň 10 hodnoteniami
ORDER BY 
    avg_rating DESC
LIMIT 10;

-- Graf 2: Najviac používané tagy na filmy
SELECT 
    t.tag,
    COUNT(*) AS usage_count
FROM 
    dim_tags t
GROUP BY 
    t.tag
ORDER BY 
    usage_count DESC
LIMIT 10;

-- Graf 3: Počet filmov podľa žánru
SELECT 
    genre, 
    COUNT(*) AS num_movies
FROM (
    SELECT 
        TRIM(VALUE) AS genre -- Rozdelený žáner
    FROM 
        dim_movies, 
        LATERAL FLATTEN(INPUT => SPLIT(genres, '|')) -- Rozklad žánrov na jednotlivé riadky
) subquery
WHERE genre != '(no genres listed)' -- Odstrániť prázdne žánre, ak existujú
GROUP BY 
    genre
ORDER BY 
    num_movies DESC;

-- Graf 4: Najaktívnejšia veková kategória podľa počtu hodnotení
SELECT 
    u.age AS age_category,
    COUNT(f.rating) AS total_ratings
FROM 
    fact_raitings f
JOIN 
    dim_users u ON f.user_key = u.user_key
GROUP BY 
    u.age
ORDER BY 
    total_ratings DESC;


-- Graf 5: Najlepšie hodnotené žánre
SELECT 
    genre,
    AVG(rating) AS avg_rating,
    COUNT(*) AS num_ratings
FROM (
    SELECT 
        TRIM(VALUE) AS genre,
        f.rating
    FROM 
        fact_raitings f
    JOIN 
        dim_movies m ON f.movie_key = m.movie_key,
        LATERAL FLATTEN(INPUT => SPLIT(m.genres, '|')) g -- Rozklad žánrov
) genre_ratings
GROUP BY 
    genre
HAVING 
    COUNT(*) >= 20 -- Minimálny počet hodnotení pre žáner
ORDER BY 
    avg_rating DESC
LIMIT 10;
