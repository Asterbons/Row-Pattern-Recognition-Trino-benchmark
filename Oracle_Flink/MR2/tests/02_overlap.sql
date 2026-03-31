SELECT *
FROM events
MATCH_RECOGNIZE (
  ORDER BY id
  MEASURES
    FIRST(id) AS start_id,
    LAST(id) AS end_id
  AFTER MATCH SKIP TO NEXT ROW
  PATTERN (A A)
  DEFINE A AS type = 'A'
);