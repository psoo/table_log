--
-- table_log () -- log changes to another table
--
--
-- see README.table_log for details
--
--
-- written by Andreas ' ads' Scherbaum (ads@pgug.de)
--
--

-- create function
CREATE FUNCTION table_log ()
    RETURNS TRIGGER
    AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION "table_log_restore_table" (VARCHAR, VARCHAR, CHAR, CHAR, CHAR, TIMESTAMPTZ, CHAR, INT, INT)
    RETURNS VARCHAR
    AS 'MODULE_PATHNAME', 'table_log_restore_table' LANGUAGE C;
CREATE FUNCTION "table_log_restore_table" (VARCHAR, VARCHAR, CHAR, CHAR, CHAR, TIMESTAMPTZ, CHAR, INT)
    RETURNS VARCHAR
    AS 'MODULE_PATHNAME', 'table_log_restore_table' LANGUAGE C;
CREATE FUNCTION "table_log_restore_table" (VARCHAR, VARCHAR, CHAR, CHAR, CHAR, TIMESTAMPTZ, CHAR)
    RETURNS VARCHAR
    AS 'MODULE_PATHNAME', 'table_log_restore_table' LANGUAGE C;
CREATE FUNCTION "table_log_restore_table" (VARCHAR, VARCHAR, CHAR, CHAR, CHAR, TIMESTAMPTZ)
    RETURNS VARCHAR
    AS 'MODULE_PATHNAME', 'table_log_restore_table' LANGUAGE C;
