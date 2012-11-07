
SET search_path TO public;

CREATE OR REPLACE FUNCTION table_log_init(int, text, text, text, text) RETURNS void AS '
DECLARE
    level        ALIAS FOR $1;
    orig_schema  ALIAS FOR $2;
    orig_name    ALIAS FOR $3;
    log_schema   ALIAS FOR $4;
    log_name     ALIAS FOR $5;
    do_log_user  int = 0;
    level_create text = '''';
    orig_qq      text;
    log_qq       text;
BEGIN
    -- Quoted qualified names
    orig_qq := quote_ident(orig_schema)||''.''||quote_ident(orig_name);
    log_qq := quote_ident(log_schema)||''.''||quote_ident(log_name);

    IF level <> 3 THEN
        level_create := level_create
            ||'', trigger_id BIGSERIAL NOT NULL PRIMARY KEY'';
        IF level <> 4 THEN
            level_create := level_create
                ||'', trigger_user VARCHAR(32) NOT NULL'';
            do_log_user := 1;
            IF level <> 5 THEN
                RAISE EXCEPTION 
                    ''table_log_init: First arg has to be 3, 4 or 5.'';
            END IF;
        END IF;
    END IF;
    
    EXECUTE ''CREATE TABLE ''||log_qq
          ||''(LIKE ''||orig_qq
          ||'', trigger_mode VARCHAR(10) NOT NULL''
          ||'', trigger_tuple VARCHAR(5) NOT NULL''
          ||'', trigger_changed TIMESTAMPTZ NOT NULL''
          ||level_create
          ||'')'';
            
    EXECUTE ''CREATE TRIGGER "table_log_trigger" AFTER UPDATE OR INSERT OR DELETE ON ''
          ||orig_qq||'' FOR EACH ROW EXECUTE PROCEDURE table_log(''
          ||quote_literal(log_name)||'',''
          ||do_log_user||'',''
          ||quote_literal(log_schema)||'')'';

    RETURN;
END;
' LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION table_log_init(int, text) RETURNS void AS '
DECLARE
    level        ALIAS FOR $1;
    orig_name    ALIAS FOR $2;
BEGIN
    PERFORM table_log_init(level, orig_name, current_schema());
    RETURN;
END;
' LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION table_log_init(int, text, text) RETURNS void AS '
DECLARE
    level        ALIAS FOR $1;
    orig_name    ALIAS FOR $2;
    log_schema   ALIAS FOR $3;
BEGIN
    PERFORM table_log_init(level, current_schema(), orig_name, log_schema);
    RETURN;
END;
' LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION table_log_init(int, text, text, text) RETURNS void AS '
DECLARE
    level        ALIAS FOR $1;
    orig_schema  ALIAS FOR $2;
    orig_name    ALIAS FOR $3;
    log_schema   ALIAS FOR $4;
BEGIN
    PERFORM table_log_init(level, orig_schema, orig_name, log_schema,
        CASE WHEN orig_schema=log_schema 
            THEN orig_name||''_log'' ELSE orig_name END);
    RETURN;
END;
' LANGUAGE plpgsql;

