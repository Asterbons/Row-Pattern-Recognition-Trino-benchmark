SELECT *
FROM input_data
MATCH_RECOGNIZE (
  ORDER BY id
  MEASURES
    MIN(X.id) AS x_min,
    MAX(X.id) AS x_max
  PATTERN (A B C)
  SUBSET X = (A, B)
  DEFINE
    A AS val = 1,
    B AS val = 2,
    C AS val = 3
);

