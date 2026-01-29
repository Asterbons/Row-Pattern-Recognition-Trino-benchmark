SELECT count(*) 
FROM postgres.public.crime_data
MATCH_RECOGNIZE(
    PARTITION BY district
    ORDER BY datetime
    MEASURES
        FIRST(A.lat) AS startLat,
        LAST(A.lat) AS topLat
    ALL ROWS PER MATCH
    PATTERN (A+ B)
    DEFINE
        A AS LAST(A.lat, 1) IS NULL OR A.lat > LAST(A.lat, 1),
        B AS B.lat < LAST(A.lat)
)
