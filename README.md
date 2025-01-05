# ETL proces datasetu MovieLens

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **MovieLens** datasetu. Projekt sa zameriava na preskúmanie správania používateľov a ich filmových preferencií na základe hodnotení filmov a demografických údajov používateľov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

## 1. Úvod a popis zdrojových dát
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa filmov, používateľov a ich hodnotení. Táto analýza umožňuje identifikovať trendy v diváckom správaní, najpopulárnejšie filmy a preferencie používateľov podľa veku.

Zdrojové dáta obsahujú päť hlavných tabuliek:
- `movies.csv`: Informácie o filmoch vrátane názvu a žánrov
- `ratings.csv`: Hodnotenia filmov od používateľov
- `tags.csv`: Používateľské značky priradené k filmom
- `links.csv`: Prepojenia na externé databázy (IMDB, TMDB)
- `user.csv`: Demografické údaje používateľov (generované pomocou Python skriptu)

### 1.1 Dátová architektúra

#### ERD diagram
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)** Tento diagram zobrazuje vzťahy medzi všetkými tabuľkami v datasete:

<p align="center">
  <img src="https://github.com/szabolcsszaraz/MovieLens/blob/main/erd_schema.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma MovieLens</em>
</p>

## 2. Dimenzionálny model

Navrhnutý bol **hviezdicový model (star schema)** pre efektívnu analýzu, kde centrálny bod predstavuje faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`dim_movies`**: Obsahuje informácie o filmoch (názov, žánre)
- **`dim_users`**: Obsahuje demografické údaje o používateľoch (vek)
- **`dim_tags`**: Obsahuje používateľské značky priradené k filmom
- **`dim_links`**: Obsahuje prepojenia na externé filmové databázy

Štruktúra hviezdicového modelu je znázornená nižšie.

<p align="center">
  <img src="https://github.com/szabolcsszaraz/MovieLens/blob/main/star_schema.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre MovieLens</em>
</p>

## 3. ETL proces v Snowflake
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). 

### 3.1 Extract (Extrahovanie dát)
Dáta boli najprv načítané do Snowflake pomocou interného stage úložiska `EAGLE_MovieLens_STAGE`. Pre každú tabuľku bol použitý príkaz COPY INTO, napríklad:

```sql
COPY INTO ratings_staging
FROM @EAGLE_MovieLens_STAGE/ratings.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
);
```

### 3.2 Transform (Transformácia dát)
V tejto fáze boli vytvorené dimenzionálne tabuľky a faktová tabuľka. Demografické údaje používateľov boli generované pomocou Python skriptu VytvorenieUsers.py, ktorý vytvoril realistické vekové rozloženie používateľov v rozsahu 15-70 rokov.

Príklad vytvorenia dimenzionálnej tabuľky:
```sql
CREATE OR REPLACE TABLE dim_users (
    user_key   NUMBER IDENTITY PRIMARY KEY,
    userId     NUMBER UNIQUE,
    age        NUMBER
);
```

### 3.3 Load (Načítanie dát)
Dáta boli nahrané do dimenzionálnych tabuliek a faktovej tabuľky pomocou INSERT príkazov, ktoré zabezpečili správne prepojenie medzi tabuľkami:

```sql
INSERT INTO fact_raitings (user_key, movie_key, rating, timestamp)
SELECT 
    u.user_key,
    m.movie_key,
    r.rating,
    r.timestamp
FROM ratings_staging r
JOIN dim_users u ON r.userId = u.userId
JOIN dim_movies m ON r.movieId = m.movieId;
```

## 4. Vizualizácia dát

Dashboard zobrazený obsahuje 5 kľúčových vizualizácií:
1. Najlepšie hodnotené filmy (Top 10)
2. Najviac používané tagy na filmy
3. Počet filmov podľa žánru
4. Najaktívnejšia veková kategória podľa počtu hodnotení
5. Najlepšie hodnotené žánre

<p align="center">
  <img src="https://github.com/szabolcsszaraz/MovieLens/blob/main/MovieLens_dashboard.png" alt="ERD Schema">
  <br>
  <em>Obrázok 3 Dashboard MovieLens datasetu</em>
</p>

### Graf 1: Najlepšie hodnotené filmy (Top 10)
Tento dotaz zobrazuje 10 filmov s najvyšším priemerným hodnotením. Filmy sú zoradené podľa priemerného hodnotenia, pričom sú zahrnuté len tie, ktoré majú aspoň 10 hodnotení. To umožňuje identifikovať najlepšie hodnotené filmy medzi používateľmi.

```sql
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
```

### Graf 2: Najviac používané tagy na filmy
Tento dotaz ukazuje 10 najviac používaných tagov pre filmy. Tagy môžu pomôcť identifikovať trendy alebo obľúbené témy, ktoré sa objavujú vo filmoch, čo môže byť užitočné na kategorizovanie alebo odporúčanie filmov.

```sql
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
```

### Graf 3: Počet filmov podľa žánru
Tento dotaz zobrazuje počet filmov v jednotlivých žánroch. Vykonáva sa rozklad žánrov a následné počítanie filmov v každom žánri, čím poskytuje prehľad o rozmanitosti filmov podľa žánru a najčastejšie sa vyskytujúcich žánroch.

```sql
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
```

### Graf 4: Najaktívnejšia veková kategória podľa počtu hodnotení
Tento dotaz ukazuje, ktorá veková kategória používateľov najviac hodnotí filmy. Je to užitočné na analýzu aktivity používateľov v rôznych vekových skupinách a môže pomôcť pri analýze demografických preferencií.

```sql
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
```

### Graf 5: Najlepšie hodnotené žánre
Tento dotaz zobrazuje 10 najlepšie hodnotených žánrov na základe priemerného hodnotenia a počtu hodnotení. Pomáha identifikovať, ktoré žánre majú najväčší vplyv na používateľov a ktoré žánre sú preferované, pokiaľ ide o kvalitu filmov.

```sql
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
```

**Autor:** Szabolcs Száraz
