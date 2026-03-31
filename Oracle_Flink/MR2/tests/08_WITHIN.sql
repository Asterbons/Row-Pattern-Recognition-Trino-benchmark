SELECT *
FROM input_2
MATCH_RECOGNIZE(
    ORDER BY ts
    MEASURES
        FIRST(A.ts) AS start_ts,
        LAST(B.ts)  AS end_ts
    PATTERN (A B)
    WITHIN INTERVAL '5' SECOND
    DEFINE
        A AS A.value = 1,
        B AS B.value = 2
);

