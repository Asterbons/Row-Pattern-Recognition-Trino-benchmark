SELECT *
FROM postgres.public.crime_data
MATCH_RECOGNIZE (
  ORDER BY datetime
  MEASURES
    FIRST(datetime) AS start_ts,
    LAST(datetime)  AS end_ts
  AFTER MATCH SKIP TO NEXT ROW
  PATTERN (A A)
  DEFINE
    A AS primary_type IS NOT NULL
);
