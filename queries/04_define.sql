SELECT *
FROM postgres.public.crime_data
MATCH_RECOGNIZE (
  ORDER BY datetime, id
  MEASURES
    FIRST(lat) AS start_lat,
    LAST(lat) AS end_lat,
    FIRST(datetime) AS start_ts,
    LAST(datetime) AS end_ts
  PATTERN (UP+ | DOWN+ | SPECIAL*)
  DEFINE
    UP      AS PREV(lat) IS NOT NULL AND lat > PREV(lat) + 0.01,
    DOWN    AS PREV(lat) IS NOT NULL AND lat < PREV(lat) - 0.01,
    SPECIAL AS primary_type IN ('BURGLARY','ROBBERY') AND lat BETWEEN 52.5 AND 52.55
);
