CREATE TABLE result_of_join_table AS
SELECT 
    t.id,
    t.title,
    t.overview,
    t.release_date,
    t.popularity,
    t.vote_average,
    t.vote_count,
    t.release_year,
    d.budget,
    d.revenue
FROM 
    tmdb_rated_movies AS t
INNER JOIN 
    transformed_data AS d
ON 
    t.title = d.original_title;