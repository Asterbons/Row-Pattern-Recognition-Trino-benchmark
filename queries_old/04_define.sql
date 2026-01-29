SELECT count(*) 
FROM postgres.public.crime_data
MATCH_RECOGNIZE (
  PARTITION BY district
  ORDER BY datetime
  MEASURES
    FIRST(UP.datetime) AS start_ts,
    LAST(DOWN.datetime) AS end_ts
  PATTERN (UP+ DOWN+)
  DEFINE
    UP   AS PREV(lat) IS NULL OR lat > PREV(lat),
    DOWN AS PREV(lat) IS NULL OR lat < PREV(lat)
)
