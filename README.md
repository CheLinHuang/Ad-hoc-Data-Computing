Ad-hoc-Data-Computing
=================================
Syntax
-----------
1. SELECT attribute name should be separated by comma without space.
2. All the other terms should be seperated by space.
-----------

Execution
-----------
python3 translate.py
-----------

Some exmaples
-----------
SELECT title_year,movie_title,Award,imdb_score FROM movies.csv M JOIN oscars.csv A ON M.movie_title = A.Film WHERE M.imdb_score < 7
SELECT title_year,movie_title FROM movies.csv M JOIN oscars.csv A ON M.movie_title = A.Film WHERE NOT ( M.imdb_score < 7 )
SELECT M1.director_name,M1.title_year,M1.movie_title,M2.title_year,M2.movie_title,M3.title_year,M3.movie_title FROM movies.csv M1 JOIN movies.csv M2 ON M1.director_name = M2.director_name JOIN movies.csv M3 ON M1.director_name = M3.director_name WHERE M1.movie_title <> M2.movie_title AND M2.movie_title <> M3.movie_title AND M1.movie_title <> M3.movie_title

