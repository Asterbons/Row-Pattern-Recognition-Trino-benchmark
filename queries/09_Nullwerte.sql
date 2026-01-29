SELECT *
FROM postgres.public.crime_data
MATCH_RECOGNIZE (
  ORDER BY datetime
  MEASURES
    FIRST(A.datetime) AS start_ts,
    LAST(B.datetime) AS end_ts
  PATTERN (A B*)
  DEFINE
    A AS lat IS NOT NULL,        
    B AS lat > PREV(lat)         
);
