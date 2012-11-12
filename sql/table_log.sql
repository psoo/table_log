CREATE EXTENSION table_log;

-- drop old trigger
DROP TRIGGER test_log_chg ON test; -- ignore any error

-- create demo table
DROP TABLE test; -- ignore any error
CREATE TABLE test (
  id                    INT                 NOT NULL
                                            PRIMARY KEY,
  name                  VARCHAR(20)         NOT NULL
);

-- create the table without data from demo table
DROP TABLE test_log; -- ignore any error
SELECT * INTO test_log FROM test LIMIT 0;
ALTER TABLE test_log ADD COLUMN trigger_mode VARCHAR(10);
ALTER TABLE test_log ADD COLUMN trigger_tuple VARCHAR(5);
ALTER TABLE test_log ADD COLUMN trigger_changed TIMESTAMPTZ;
ALTER TABLE test_log ADD COLUMN trigger_id BIGINT;
CREATE SEQUENCE test_log_id;
SELECT SETVAL('test_log_id', 1, FALSE);
ALTER TABLE test_log ALTER COLUMN trigger_id SET DEFAULT NEXTVAL('test_log_id');

-- create trigger
CREATE TRIGGER test_log_chg AFTER UPDATE OR INSERT OR DELETE ON test FOR EACH ROW
               EXECUTE PROCEDURE table_log();

-- test trigger
INSERT INTO test VALUES (1, 'name');
SELECT id, name FROM test;
SELECT id, name, trigger_mode, trigger_tuple, trigger_id FROM test_log;
UPDATE test SET name='other name' WHERE id=1;
SELECT id, name FROM test;
SELECT id, name, trigger_mode, trigger_tuple, trigger_id FROM test_log;

-- create restore table
SELECT table_log_restore_table('test', 'id', 'test_log', 'trigger_id', 'test_recover', NOW());
SELECT id, name FROM test_recover;

DROP TABLE test;
DROP TABLE test_log;

-- test table_log_init with all arguments
-- trigger_user and trigger_changed might differ, so ignore it

SET client_min_messages TO warning;

CREATE TABLE test(id integer, name text);
SELECT table_log_init(5, 'test');
INSERT INTO test VALUES(1, 'joe');
INSERT INTO test VALUES(2, 'barney');
INSERT INTO test VALUES(3, 'monica');
UPDATE test SET name = 'veronica' WHERE id = 3;
DELETE FROM test WHERE id = 1;
SELECT id, name, trigger_mode, trigger_tuple, trigger_id FROM test_log;
DROP TABLE test;
DROP TABLE test_log;

CREATE TABLE test(id integer, name text);
SELECT table_log_init(4, 'test');
INSERT INTO test VALUES(1, 'joe');
INSERT INTO test VALUES(2, 'barney');
INSERT INTO test VALUES(3, 'monica');
UPDATE test SET name = 'veronica' WHERE id = 3;
DELETE FROM test WHERE id = 1;
SELECT id, name, trigger_mode, trigger_tuple, trigger_id FROM test_log;
DROP TABLE test;
DROP TABLE test_log;

CREATE TABLE test(id integer, name text);
SELECT table_log_init(3, 'test');
INSERT INTO test VALUES(1, 'joe');
INSERT INTO test VALUES(2, 'barney');
INSERT INTO test VALUES(3, 'monica');
UPDATE test SET name = 'veronica' WHERE id = 3;
DELETE FROM test WHERE id = 1;
SELECT id, name, trigger_mode, trigger_tuple FROM test_log;
DROP TABLE test;
DROP TABLE test_log;

RESET client_min_messages;

