BEGIN;

CREATE TABLE drop_test (
  id          SERIAL           NOT NULL
                               PRIMARY KEY,
  col1        VARCHAR(20)      NOT NULL
                               DEFAULT '',
  col2        VARCHAR(20)      NOT NULL
                               DEFAULT '',
  col3        VARCHAR(20)      NOT NULL
                               DEFAULT ''
);

-- init tablelog
SELECT table_log_init(5, 'public', 'drop_test', 'public', 'drop_test_log');

INSERT INTO drop_test (col1, col2, col3) VALUES ('a1', 'b1', 'c1');
SELECT * FROM drop_test;
SELECT * FROM drop_test_log;

ALTER TABLE drop_test DROP COLUMN col2;
ALTER TABLE drop_test_log DROP COLUMN col2;

INSERT INTO drop_test (col1, col3) VALUES ('a2', 'c2');
SELECT * FROM drop_test;
SELECT * FROM drop_test_log;


ROLLBACK;


