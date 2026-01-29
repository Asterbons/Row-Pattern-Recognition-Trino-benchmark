SELECT *
FROM postgres.public.crime_data
MATCH_RECOGNIZE(
    ORDER BY datetime
    MEASURES
        FIRST(A.datetime) AS start_ts,
        LAST(B.datetime)  AS end_ts
    PATTERN (A B)
    WITHIN INTERVAL '1' HOUR
    DEFINE
        A AS A.lat > 0,
        B AS B.lat > 0
);
