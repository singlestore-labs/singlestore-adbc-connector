DROP TABLE IF EXISTS test_time;

CREATE TABLE test_time (
    idx INTEGER,
    res TIME(6)
);

INSERT INTO test_time (idx, res) VALUES (1, '13:45:31.123456' :> TIME(6));
INSERT INTO test_time (idx, res) VALUES (2, '00:00:00' :> TIME(6));
INSERT INTO test_time (idx, res) VALUES (3, '23:59:59.999999' :> TIME(6));
INSERT INTO test_time (idx, res) VALUES (4, '12:30:45.500' :> TIME(6));
INSERT INTO test_time (idx, res) VALUES (5, NULL);
