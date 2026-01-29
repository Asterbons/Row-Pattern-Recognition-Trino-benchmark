SELECT count(*) 
FROM postgres.public.crime_data
MATCH_RECOGNIZE(
    PARTITION BY district
    ORDER BY datetime
    MEASURES
        C.lat AS lastLat
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (A B* C)
    DEFINE
        A AS A.lat > 52.4,
        B AS B.lat < 52.6,
        C AS C.lat > 52.5
)
