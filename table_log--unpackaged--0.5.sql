ALTER EXTENSION table_log ADD FUNCTION table_log();
ALTER EXTENSION table_log ADD FUNCTION table_log_restore_table(varchar,
                                                               varchar,
                                                               char,
                                                               char,
                                                               char,
                                                               timestamp with time zone,
                                                               char,
                                                               integer,
                                                               integer);
ALTER EXTENSION table_log ADD FUNCTION table_log_restore_table(varchar,
                                                               varchar,
                                                               char,
                                                               char,
                                                               char,
                                                               timestamp with time zone,
                                                               char,
                                                               integer);
ALTER EXTENSION table_log ADD FUNCTION table_log_restore_table(varchar,
                                                               varchar,
                                                               char,
                                                               char,
                                                               char,
                                                               timestamp with time zone,
                                                               char);
ALTER EXTENSION table_log ADD FUNCTION table_log_restore_table(varchar,
                                                               varchar,
                                                               char,
                                                               char,
                                                               char,
                                                               timestamp with time zone);

ALTER EXTENSION table_log ADD FUNCTION table_log_init(integer,
                                                      text,
                                                      text,
                                                      text,
                                                      text);
ALTER EXTENSION table_log ADD FUNCTION table_log_init(integer,
                                                      text);
ALTER EXTENSION table_log ADD FUNCTION table_log_init(integer,
                                                      text,
                                                      text);
ALTER EXTENSION table_log ADD FUNCTION table_log_init(integer,
                                                      text,
                                                      text,
                                                      text);
