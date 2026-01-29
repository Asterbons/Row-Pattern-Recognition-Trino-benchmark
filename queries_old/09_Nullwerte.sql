SELECT count(*)
FROM postgres.public.crime_data
MATCH_RECOGNIZE (
  PARTITION BY district
  ORDER BY datetime
  MEASURES
    FIRST(A.datetime) AS start_ts,
    LAST(B.datetime) AS end_ts
  PATTERN (A B+)
  DEFINE
    A AS lat IS NOT NULL,        
    B AS lat > PREV(lat)         
)
