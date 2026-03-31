SELECT *
FROM Ticker2
    MATCH_RECOGNIZE(
        PARTITION BY symbol
        ORDER BY rowtime
        MEASURES
            FIRST(A.price) AS startPrice,
            LAST(A.price) AS topPrice,
            B.price AS lastPrice
        ONE ROW PER MATCH
        PATTERN (A+ B)
        DEFINE
            A AS LAST(A.price, 1) IS NULL OR A.price > LAST(A.price, 1),
            B AS B.price < LAST(A.price)
    );