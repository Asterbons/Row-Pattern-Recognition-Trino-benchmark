SELECT count(*) 
FROM postgres.public.crime_data
MATCH_RECOGNIZE (
  PARTITION BY district
  ORDER BY datetime
  MEASURES
    MIN(X.lat) AS x_min,
    MAX(X.lat) AS x_max
  PATTERN (A B C)
  SUBSET X = (A, B)
  DEFINE
    A AS lat < 53.0,
    B AS lat < 53.0,
    C AS lat < 53.0
)
