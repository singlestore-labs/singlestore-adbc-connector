CREATE TABLE test_float32 (
    idx INTEGER,
    res FLOAT
);

INSERT INTO test_float32 (idx, res) VALUES (1, 3.14);
INSERT INTO test_float32 (idx, res) VALUES (2, 0.0);
INSERT INTO test_float32 (idx, res) VALUES (3, -3.4e38);
INSERT INTO test_float32 (idx, res) VALUES (4, 3.4e38);
INSERT INTO test_float32 (idx, res) VALUES (5, 1.175e-38);
INSERT INTO test_float32 (idx, res) VALUES (6, NULL);