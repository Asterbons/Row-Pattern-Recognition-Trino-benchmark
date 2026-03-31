SELECT *
FROM T
MATCH_RECOGNIZE (
  ORDER BY id
  MEASURES
    FIRST(UP.id) AS start_id,
    LAST(DOWN.id) AS end_id
  PATTERN (UP+ DOWN+)
  DEFINE
    UP   AS PREV(value) IS NULL OR value > PREV(value),
    DOWN AS PREV(value) IS NULL OR value < PREV(value)
);

