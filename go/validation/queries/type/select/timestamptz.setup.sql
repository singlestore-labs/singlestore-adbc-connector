DROP TABLE IF EXISTS test_timestamptz;

CREATE TABLE test_timestamptz (
    idx INTEGER,
    res TIMESTAMP(6)
);

INSERT INTO test_timestamptz (idx, res) VALUES (1, '2023-05-15 13:45:30' :> TIMESTAMP);
INSERT INTO test_timestamptz (idx, res) VALUES (2, '2000-01-01 00:00:00' :> TIMESTAMP);
INSERT INTO test_timestamptz (idx, res) VALUES (3, '1970-07-20 20:17:40' :> TIMESTAMP);
INSERT INTO test_timestamptz (idx, res) VALUES (4, '2037-12-31 23:59:59' :> TIMESTAMP);
INSERT INTO test_timestamptz (idx, res) VALUES (5, NULL);
