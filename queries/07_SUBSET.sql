SELECT *
FROM postgres.public.crime_data
MATCH_RECOGNIZE (
  ORDER BY datetime
  MEASURES
    MIN(X.lat) AS x_min,
    MAX(X.lat) AS x_max
  PATTERN (A (B|A) C)
  SUBSET X = (A, B)
  DEFINE
    A AS lat < 53.0,
    B AS lat < 53.0,
    C AS lat < 53.0
);
