CREATE TABLE "t1" (
    id1 BIGINT NOT NULL,
    x BIGINT NOT NULL
)
WITH ('materialized' = 'true');

CREATE TABLE "t2" (
    id2 BIGINT NOT NULL,
    y BIGINT NOT NULl
)
WITH ('materialized' = 'true');

CREATE VIEW "v1"
AS
    SELECT
        *
    FROM
        t1
    JOIN
        t2
    ON t1.id1 = t2.id2;
