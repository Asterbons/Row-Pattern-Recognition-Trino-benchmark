SELECT *
FROM Ticker
    MATCH_RECOGNIZE(
        PARTITION BY symbol
        ORDER BY rowtime
        MEASURES
            C.price AS lastPrice
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (A B* C)
        DEFINE
            A AS A.price > 10,
            B AS B.price < 15,
            C AS C.price > 12
    );