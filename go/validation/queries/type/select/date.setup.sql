CREATE TABLE test_date (
                           idx INTEGER,
                           res DATE
);

INSERT INTO test_date (idx, res) VALUES (1, '2023-05-15' :> DATE);
INSERT INTO test_date (idx, res) VALUES (2, '1000-01-01' :> DATE);
INSERT INTO test_date (idx, res) VALUES (3, '1969-07-20' :> DATE);
INSERT INTO test_date (idx, res) VALUES (4, '9999-12-31' :> DATE);
INSERT INTO test_date (idx, res) VALUES (5, NULL);
