SELECT *
FROM input
MATCH_RECOGNIZE (
  ORDER BY id
  MEASURES
    FIRST(A.id) AS start_id,
    LAST(B.id) AS end_id
  PATTERN (A B+)
  DEFINE
    A AS val IS NOT NULL,
    B AS val > PREV(val)
);
