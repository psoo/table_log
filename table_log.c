/*
 * table_log () -- log changes to another table
 *
 *
 * see README.table_log for details
 *
 *
 * written by Andreas ' ads' Scherbaum (ads@pgug.de)
 *
 */

#include "table_log.h"

#include "postgres.h"
#include "fmgr.h"
#include "executor/spi.h"	/* this is what you need to work with SPI */
#include "commands/trigger.h"	/* -"- and triggers */
#include "mb/pg_wchar.h"	/* support for the quoting functions */
#include <ctype.h>		/* tolower () */
#include <string.h>		/* strlen() */
#include "miscadmin.h"
#include "utils/formatting.h"
#include "utils/builtins.h"
#include <utils/lsyscache.h>
#include <funcapi.h>

/* for PostgreSQL >= 8.2.x */
#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif


#ifndef PG_NARGS
/*
 * Get number of arguments passed to function.
 * this macro isnt defined in 7.2.x
 */
#define PG_NARGS() (fcinfo->nargs)
#endif

extern Datum table_log(PG_FUNCTION_ARGS);
Datum table_log_restore_table(PG_FUNCTION_ARGS);
static char *do_quote_ident(char *iptr);
static char *do_quote_literal(char *iptr);
static void __table_log (TriggerData *trigdata, char *changed_mode, char *changed_tuple, HeapTuple tuple, int number_columns, char *log_table, int use_session_user, char *log_schema);
void __table_log_restore_table_insert(SPITupleTable *spi_tuptable, char *table_restore, char *table_orig_pkey, char *col_query_start, int col_pkey, int number_columns, int i);
void __table_log_restore_table_update(SPITupleTable *spi_tuptable, char *table_restore, char *table_orig_pkey, char *col_query_start, int col_pkey, int number_columns, int i, char *old_key_string);
void __table_log_restore_table_delete(SPITupleTable *spi_tuptable, char *table_restore, char *table_orig_pkey, char *col_query_start, int col_pkey, int number_columns, int i);
char *__table_log_varcharout(VarChar *s);
int count_columns (TupleDesc tupleDesc);

/* this is a V1 (new) function */
/* the trigger function */
PG_FUNCTION_INFO_V1(table_log);
/* build only, if the 'Table Function API' is available */
#ifdef FUNCAPI_H_not_implemented
/* restore one single column */
PG_FUNCTION_INFO_V1(table_log_show_column);
#endif /* FUNCAPI_H */
/* restore a full table */
PG_FUNCTION_INFO_V1(table_log_restore_table);


/*
 * count_columns (TupleDesc tupleDesc)
 * Will count and return the number of columns in the table described by 
 * tupleDesc. It needs to ignore dropped columns.
 */
int count_columns (TupleDesc tupleDesc) {
  int count=0;
  int i;

  for (i = 0; i < tupleDesc->natts; ++i) {
    if (!tupleDesc->attrs[i]->attisdropped) {
      ++count;
    }
  }

  return count;
}


/*
table_log()

trigger function for logging table changes

parameter:
  - log table name (optional)
return:
  - trigger data (for Pg)
*/
Datum table_log(PG_FUNCTION_ARGS) {
  TriggerData    *trigdata = (TriggerData *) fcinfo->context;
  int            ret;
  char           query[250 + NAMEDATALEN];	/* for getting table infos (250 chars (+ one times the length of all names) should be enough) */
  int            number_columns = 0;		/* counts the number columns in the table */
  int            number_columns_log = 0;	/* counts the number columns in the table */
  char           *orig_schema;
  char           *log_schema;
  char           *log_table;
  int            use_session_user = 0;          /* should we write the current (session) user to the log table? */

  /*
   * Some checks first...
   */

#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "start table_log()");
#endif /* TABLE_LOG_DEBUG */

  /* called by trigger manager? */
  if (!CALLED_AS_TRIGGER(fcinfo)) {
    elog(ERROR, "table_log: not fired by trigger manager");
  }

  /* must only be called for ROW trigger */
  if (TRIGGER_FIRED_FOR_STATEMENT(trigdata->tg_event)) {
    elog(ERROR, "table_log: can't process STATEMENT events");
  }

  /* must only be called AFTER */
  if (TRIGGER_FIRED_BEFORE(trigdata->tg_event)) {
    elog(ERROR, "table_log: must be fired after event");
  }

  /* now connect to SPI manager */
  ret = SPI_connect();
  if (ret != SPI_OK_CONNECT) {
    elog(ERROR, "table_log: SPI_connect returned %d", ret);
  }

  /* get schema name for the table, in case we need it later */
  orig_schema = get_namespace_name(RelationGetNamespace(trigdata->tg_relation));

#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "prechecks done, now getting original table attributes");
#endif /* TABLE_LOG_DEBUG */

  number_columns = count_columns(trigdata->tg_relation->rd_att);
  if (number_columns < 1) {
    elog(ERROR, "table_log: number of columns in table is < 1, can this happen?");
  }
#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "number columns in orig table: %i", number_columns);
#endif /* TABLE_LOG_DEBUG */

  if (trigdata->tg_trigger->tgnargs > 3) {
    elog(ERROR, "table_log: too many arguments to trigger");
  }
  
  /* name of the log schema */
  if (trigdata->tg_trigger->tgnargs > 2) {
    /* check if a log schema argument is given, if yes, use it */
    log_schema = trigdata->tg_trigger->tgargs[2];
  } else {
    /* if no, use orig_schema */
    log_schema = orig_schema;
  }

  /* should we write the current user? */
  if (trigdata->tg_trigger->tgnargs > 1) {
    /* check if a second argument is given */  
    /* if yes, use it, if it is true */
    if (atoi(trigdata->tg_trigger->tgargs[1]) == 1) {
      use_session_user = 1;
#ifdef TABLE_LOG_DEBUG
      elog(NOTICE, "will write session user to 'trigger_user'");
#endif /* TABLE_LOG_DEBUG */
    }
  }

  /* name of the log table */
  if (trigdata->tg_trigger->tgnargs > 0) {
    /* check if a logtable argument is given */  
    /* if yes, use it */
    log_table = (char *) palloc((strlen(trigdata->tg_trigger->tgargs[0]) + 2) * sizeof(char));
    sprintf(log_table, "%s", trigdata->tg_trigger->tgargs[0]);
  } else {
    /* if no, use 'table name' + '_log' */
    log_table = (char *) palloc((strlen(do_quote_ident(SPI_getrelname(trigdata->tg_relation))) + 5) * sizeof(char));
    sprintf(log_table, "%s_log", SPI_getrelname(trigdata->tg_relation));
  }

#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "log table: %s", log_table);
#endif /* TABLE_LOG_DEBUG */

#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "now check, if log table exists");
#endif /* TABLE_LOG_DEBUG */

  /* get the number columns in the table */
  snprintf(query, 249, "%s.%s", do_quote_ident(log_schema), do_quote_ident(log_table));
  number_columns_log = count_columns(RelationNameGetTupleDesc(query));
  if (number_columns_log < 1) {
    elog(ERROR, "could not get number columns in relation %s", log_table);
  }
#ifdef TABLE_LOG_DEBUG
    elog(NOTICE, "number columns in log table: %i", number_columns_log);
#endif /* TABLE_LOG_DEBUG */

  /* check if the logtable has 3 (or now 4) columns more than our table */
  /* +1 if we should write the session user */
  if (use_session_user == 0) {
    /* without session user */
    if (number_columns_log != number_columns + 3 && number_columns_log != number_columns + 4) {
      elog(ERROR, "number colums in relation %s(%d) does not match columns in %s(%d)", SPI_getrelname(trigdata->tg_relation), number_columns, log_table, number_columns_log);
    }
  } else {
    /* with session user */
    if (number_columns_log != number_columns + 3 + 1 && number_columns_log != number_columns + 4 + 1) {
      elog(ERROR, "number colums in relation %s does not match columns in %s", SPI_getrelname(trigdata->tg_relation), log_table);
    }
  }
#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "log table OK");
#endif /* TABLE_LOG_DEBUG */


  /* For each column in key ... */

#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "copy data ...");
#endif /* TABLE_LOG_DEBUG */
  if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event)) {
    /* trigger called from INSERT */
#ifdef TABLE_LOG_DEBUG
    elog(NOTICE, "mode: INSERT -> new");
#endif /* TABLE_LOG_DEBUG */
    __table_log(trigdata, "INSERT", "new", trigdata->tg_trigtuple, number_columns, log_table, use_session_user, log_schema);
  } else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event)) {
    /* trigger called from UPDATE */
#ifdef TABLE_LOG_DEBUG
    elog(NOTICE, "mode: UPDATE -> old");
#endif /* TABLE_LOG_DEBUG */
    __table_log(trigdata, "UPDATE", "old", trigdata->tg_trigtuple, number_columns, log_table, use_session_user, log_schema);
#ifdef TABLE_LOG_DEBUG
    elog(NOTICE, "mode: UPDATE -> new");
#endif /* TABLE_LOG_DEBUG */
    __table_log(trigdata, "UPDATE", "new", trigdata->tg_newtuple, number_columns, log_table, use_session_user, log_schema);
  } else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event)) {
    /* trigger called from DELETE */
#ifdef TABLE_LOG_DEBUG
    elog(NOTICE, "mode: DELETE -> old");
#endif /* TABLE_LOG_DEBUG */
    __table_log(trigdata, "DELETE", "old", trigdata->tg_trigtuple, number_columns, log_table, use_session_user, log_schema);
  } else {
    elog(ERROR, "trigger fired by unknown event");
  }

#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "cleanup, trigger done");
#endif /* TABLE_LOG_DEBUG */
  /* clean up */
  pfree(log_table);

  /* close SPI connection */
  SPI_finish();
  /* return trigger data */
  return PointerGetDatum(trigdata->tg_trigtuple);
}

/*
__table_log()

helper function for table_log()

parameter:
  - trigger data
  - change mode (INSERT, UPDATE, DELETE)
  - tuple to log (old, new)
  - pointer to tuple
  - number columns in table
  - logging table
  - flag for writing session user
return:
  none
*/
static void __table_log (TriggerData *trigdata, char *changed_mode, char *changed_tuple, HeapTuple tuple, int number_columns, char *log_table, int use_session_user, char *log_schema) {
  char     *before_char;
  int      i, col_nr, found_col;
  /* start with 100 bytes */
  int      size_query = 100;
  char     *query;
  char     *query_start;
  int      ret;

#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "calculate query size");
#endif /* TABLE_LOG_DEBUG */
  /* add all sizes we need and know at this point */
  size_query += strlen(changed_mode) + strlen(changed_tuple) + strlen(log_table) + strlen(log_schema);

  /* calculate size of the columns */
  col_nr = 0;
  for (i = 1; i <= number_columns; i++) {
    col_nr++;
    found_col = 0;
    do {
      if (trigdata->tg_relation->rd_att->attrs[col_nr - 1]->attisdropped) {
        /* this column is dropped, skip it */
        col_nr++;
        continue;
      } else {
        found_col++;
      }
    } while (found_col == 0);
    /* the column name */
    size_query += strlen(do_quote_ident(SPI_fname(trigdata->tg_relation->rd_att, col_nr))) + 3;
    /* the value */
    before_char = SPI_getvalue(tuple, trigdata->tg_relation->rd_att, col_nr);
    /* old size plus this char and 3 bytes for , and so */
    if (before_char == NULL) {
      size_query += 6;
    } else {
      size_query += strlen(do_quote_literal(before_char)) + 3;
    }
  }

  if (use_session_user == 1) {
    /* add memory for session user */
    size_query += NAMEDATALEN + 20;
  }

#ifdef TABLE_LOG_DEBUG
  // elog(NOTICE, "query size: %i", size_query);
#endif /* TABLE_LOG_DEBUG */
#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "build query");
#endif /* TABLE_LOG_DEBUG */
  /* allocate memory */
  query_start = (char *) palloc(size_query * sizeof(char));
  query = query_start;

  /* build query */
  sprintf(query, "INSERT INTO %s.%s (", do_quote_ident(log_schema), do_quote_ident(log_table));
  query = query_start + strlen(query);

  /* add colum names */
  col_nr = 0;
  for (i = 1; i <= number_columns; i++) {
    col_nr++;
    found_col = 0;
    do {
      if (trigdata->tg_relation->rd_att->attrs[col_nr - 1]->attisdropped) {
        /* this column is dropped, skip it */
        col_nr++;
        continue;
      } else {
        found_col++;
      }
    } while (found_col == 0);
    sprintf(query, "%s, ", do_quote_ident(SPI_fname(trigdata->tg_relation->rd_att, col_nr)));
    query = query_start + strlen(query_start);
  }

  /* add session user */
  if (use_session_user == 1) {
    sprintf(query, "trigger_user, ");
    query = query_start + strlen(query_start);
  }
  /* add the 3 extra colum names */
  sprintf(query, "trigger_mode, trigger_tuple, trigger_changed) VALUES (");
  query = query_start + strlen(query_start);

  /* add values */
  col_nr = 0;
  for (i = 1; i <= number_columns; i++) {
    col_nr++;
    found_col = 0;
    do {
      if (trigdata->tg_relation->rd_att->attrs[col_nr - 1]->attisdropped) {
        /* this column is dropped, skip it */
        col_nr++;
        continue;
      } else {
        found_col++;
      }
    } while (found_col == 0);
    before_char = SPI_getvalue(tuple, trigdata->tg_relation->rd_att, col_nr);
    if (before_char == NULL) {
      sprintf(query, "NULL, ");
    } else {
      sprintf(query, "%s, ", do_quote_literal(before_char));
    }
    query = query_start + strlen(query_start);
  }

  /* add session user */
  if (use_session_user == 1) {
    sprintf(query, "SESSION_USER, ");
    query = query_start + strlen(query_start);
  }
  /* add the 3 extra values */
  sprintf(query, "%s, %s, NOW());", do_quote_literal(changed_mode), do_quote_literal(changed_tuple));
  query = query_start + strlen(query_start);

#ifdef TABLE_LOG_DEBUG_QUERY
  elog(NOTICE, "query: %s", query_start);
#else
#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "execute query");
#endif /* TABLE_LOG_DEBUG */
#endif /* TABLE_LOG_DEBUG_QUERY */
  /* execute insert */
  ret = SPI_exec(query_start, 0);
  if (ret != SPI_OK_INSERT) {
    elog(ERROR, "could not insert log information into relation %s (error: %d)", log_table, ret);
  }
#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "done");
#endif /* TABLE_LOG_DEBUG */

  /* clean up */
  pfree(query_start);

}


#ifdef FUNCAPI_H_not_implemented
/*
table_log_show_column()

show a single column on a date in the past

parameter:
  not yet defined
return:
  not yet defined
*/
Datum table_log_show_column(PG_FUNCTION_ARGS) {
  TriggerData    *trigdata = (TriggerData *) fcinfo->context;
  int            ret;

  /*
   * Some checks first...
   */

#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "start table_log_show_column()");
#endif /* TABLE_LOG_DEBUG */

  /* Connect to SPI manager */
  ret = SPI_connect();
  if (ret != SPI_OK_CONNECT) {
    elog(ERROR, "table_log_show_column: SPI_connect returned %d", ret);
  }

#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "this function isnt available yet");
#endif /* TABLE_LOG_DEBUG */

  /* close SPI connection */
  SPI_finish();
  return PG_RETURN_NULL;
}
#endif /* FUNCAPI_H */


/*
table_log_restore_table()

restore a complete table based on the logging table

parameter:
  - original table name
  - name of primary key in original table
  - logging table
  - name of primary key in logging table
  - restore table name
  - timestamp for restoring data
  - primary key to restore (only this key will be restored) (optional)
  - restore mode
    0: restore from blank table (default)
       needs a complete logging table
    1: restore from actual table backwards
  - dont create table temporarly
    0: create restore table temporarly (default)
    1: create restore table not temporarly
  return:
    not yet defined
*/
Datum table_log_restore_table(PG_FUNCTION_ARGS) {
  /* the original table name */
  char  *table_orig;
  /* the primary key in the original table */
  char  *table_orig_pkey;
  /* number columns in original table */
  int  table_orig_columns;
  /* the log table name */
  char  *table_log;
  /* the primary key in the log table (usually trigger_id) */
  /* cannot be the same then then the pkey in the original table */
  char  *table_log_pkey;
  /* number columns in log table */
  int  table_log_columns;
  /* the restore table name */
  char  *table_restore;
  /* the timestamp in past */
  Datum      timestamp = PG_GETARG_DATUM(5);
  /* the single pkey, can be null (then all keys will be restored) */
  char  *search_pkey = "";
  /* the restore method
    - 0: restore from blank table (default)
         needs a complete log table!
    - 1: restore from actual table backwards
  */
  int            method = 0;
  /* dont create restore table temporarly
    - 0: create restore table temporarly (default)
    - 1: dont create restore table temporarly
  */
  int            not_temporarly = 0;
  int            ret, results, i, number_columns;
  char           query[250 + NAMEDATALEN];	/* for getting table infos (250 chars (+ one times the length of all names) should be enough) */
  int            need_search_pkey = 0;          /* does we have a single key to restore? */
  char           *tmp, *timestamp_string, *old_pkey_string = "";
  char           *trigger_mode, *trigger_tuple, *trigger_changed;
  SPITupleTable  *spi_tuptable = NULL;          /* for saving query results */
  VarChar        *return_name;

  /* memory for dynamic query */
  int      d_query_size = 250;                  /* start with 250 bytes */
  char     *d_query;
  char     *d_query_start;

  /* memory for column names */
  int      col_query_size = 0;
  char     *col_query;
  char     *col_query_start;

  int      col_pkey = 0;

  /*
   * Some checks first...
   */

#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "start table_log_restore_table()");
#endif /* TABLE_LOG_DEBUG */

  /* does we have all arguments? */
  if (PG_ARGISNULL(0)) {
    elog(ERROR, "table_log_restore_table: missing original table name");
  }
  if (PG_ARGISNULL(1)) {
    elog(ERROR, "table_log_restore_table: missing primary key name for original table");
  }
  if (PG_ARGISNULL(2)) {
    elog(ERROR, "table_log_restore_table: missing log table name");
  }
  if (PG_ARGISNULL(3)) {
    elog(ERROR, "table_log_restore_table: missing primary key name for log table");
  }
  if (PG_ARGISNULL(4)) {
    elog(ERROR, "table_log_restore_table: missing copy table name");
  }
  if (PG_ARGISNULL(5)) {
    elog(ERROR, "table_log_restore_table: missing timestamp");
  }

  /* first check number arguments to avoid an segfault */
  if (PG_NARGS() >= 7) {
    /* if argument is given, check if not null */
    if (!PG_ARGISNULL(6)) {
      /* yes, fetch it */
      search_pkey = __table_log_varcharout((VarChar *)PG_GETARG_VARCHAR_P(6));
      /* and check, if we have an argument */
      if (strlen(search_pkey) > 0) {
        need_search_pkey = 1;
#ifdef TABLE_LOG_DEBUG
        elog(NOTICE, "table_log_restore_table: will restore a single key");
#endif /* TABLE_LOG_DEBUG */
      }
    }
  }

  /* same procedere here */
  if (PG_NARGS() >= 8) {
    if (!PG_ARGISNULL(7)) {
      method = PG_GETARG_INT32(7);
      if (method > 0) {
        method = 1;
      } else {
        method = 0;
      }
    }
  }
#ifdef TABLE_LOG_DEBUG
  if (method == 1) {
    elog(NOTICE, "table_log_restore_table: will restore from actual state backwards");
  } else {
    elog(NOTICE, "table_log_restore_table: will restore from begin forward");
  }
#endif /* TABLE_LOG_DEBUG */
  if (PG_NARGS() >= 9) {
    if (!PG_ARGISNULL(8)) {
      not_temporarly = PG_GETARG_INT32(8);
      if (not_temporarly > 0) {
        not_temporarly = 1;
      } else {
        not_temporarly = 0;
      }
    }
  }
#ifdef TABLE_LOG_DEBUG
  if (not_temporarly == 1) {
    elog(NOTICE, "table_log_restore_table: dont create restore table temporarly");
  }
#endif /* TABLE_LOG_DEBUG */
  /* get parameter */
  table_orig = __table_log_varcharout((VarChar *)PG_GETARG_VARCHAR_P(0));
  table_orig_pkey = __table_log_varcharout((VarChar *)PG_GETARG_VARCHAR_P(1));
  table_log = __table_log_varcharout((VarChar *)PG_GETARG_VARCHAR_P(2));
  table_log_pkey = __table_log_varcharout((VarChar *)PG_GETARG_VARCHAR_P(3));
  table_restore = __table_log_varcharout((VarChar *)PG_GETARG_VARCHAR_P(4));

  /* pkey of original table cannot be the same as of log table */
  if (strcmp((const char *)table_orig_pkey, (const char *)table_log_pkey) == 0) {
    elog(ERROR, "pkey of logging table cannot be the pkey of the original table: %s <-> %s", table_orig_pkey, table_log_pkey);
  }

  /* Connect to SPI manager */
  ret = SPI_connect();
  if (ret != SPI_OK_CONNECT) {
    elog(ERROR, "table_log_restore_table: SPI_connect returned %d", ret);
  }

  /* check original table */
  snprintf(query, 249, "SELECT a.attname FROM pg_class c, pg_attribute a WHERE c.relname = %s AND a.attnum > 0 AND a.attrelid = c.oid ORDER BY a.attnum", do_quote_literal(table_orig));
#ifdef TABLE_LOG_DEBUG_QUERY
  elog(NOTICE, "query: %s", query);
#endif /* TABLE_LOG_DEBUG_QUERY */
  ret = SPI_exec(query, 0);
  if (ret != SPI_OK_SELECT) {
    elog(ERROR, "could not check relation: %s", table_orig);
  }
  if (SPI_processed > 0) {
    table_orig_columns = SPI_processed;
  } else {
    elog(ERROR, "could not check relation: %s", table_orig);
  }
  /* check pkey in original table */
  snprintf(query, 249, "SELECT a.attname FROM pg_class c, pg_attribute a WHERE c.relname=%s AND c.relkind='r' AND a.attname=%s AND a.attnum > 0 AND a.attrelid = c.oid", do_quote_literal(table_orig), do_quote_literal(table_orig_pkey));
#ifdef TABLE_LOG_DEBUG_QUERY
  elog(NOTICE, "query: %s", query);
#endif /* TABLE_LOG_DEBUG_QUERY */
  ret = SPI_exec(query, 0);
  if (ret != SPI_OK_SELECT) {
    elog(ERROR, "could not check relation: %s", table_orig);
  }
  if (SPI_processed == 0) {
    elog(ERROR, "could not check relation: %s", table_orig);
  }
#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "original table: OK (%i columns)", table_orig_columns);
#endif /* TABLE_LOG_DEBUG */

  /* check log table */
  snprintf(query, 249, "SELECT a.attname FROM pg_class c, pg_attribute a WHERE c.relname = %s AND a.attnum > 0 AND a.attrelid = c.oid ORDER BY a.attnum", do_quote_literal(table_log));
#ifdef TABLE_LOG_DEBUG_QUERY
  elog(NOTICE, "query: %s", query);
#endif /* TABLE_LOG_DEBUG_QUERY */
  ret = SPI_exec(query, 0);
  if (ret != SPI_OK_SELECT) {
    elog(ERROR, "could not check relation [1]: %s", table_log);
  }
  if (SPI_processed > 0) {
    table_log_columns = SPI_processed;
  } else {
    elog(ERROR, "could not check relation [2]: %s", table_log);
  }
  /* check pkey in log table */
  snprintf(query, 249, "SELECT a.attname FROM pg_class c, pg_attribute a WHERE c.relname=%s AND c.relkind='r' AND a.attname=%s AND a.attnum > 0 AND a.attrelid = c.oid", do_quote_literal(table_log), do_quote_literal(table_log_pkey));
#ifdef TABLE_LOG_DEBUG_QUERY
  elog(NOTICE, "query: %s", query);
#endif /* TABLE_LOG_DEBUG_QUERY */
  ret = SPI_exec(query, 0);
  if (ret != SPI_OK_SELECT) {
    elog(ERROR, "could not check relation [3]: %s", table_log);
  }
  if (SPI_processed == 0) {
    elog(ERROR, "could not check relation [4]: %s", table_log);
  }
#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "log table: OK (%i columns)", table_log_columns);
#endif /* TABLE_LOG_DEBUG */

  /* check restore table */
  snprintf(query, 249, "SELECT pg_attribute.attname AS a FROM pg_class, pg_attribute WHERE pg_class.relname=%s AND pg_attribute.attnum > 0 AND pg_attribute.attrelid=pg_class.oid", do_quote_literal(table_restore));
#ifdef TABLE_LOG_DEBUG_QUERY
  elog(NOTICE, "query: %s", query);
#endif /* TABLE_LOG_DEBUG_QUERY */
  ret = SPI_exec(query, 0);
  if (ret != SPI_OK_SELECT) {
    elog(ERROR, "could not check relation: %s", table_restore);
  }
  if (SPI_processed > 0) {
    elog(ERROR, "restore table already exists: %s", table_restore);
  }
#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "restore table: OK (doesnt exists)");
#endif /* TABLE_LOG_DEBUG */

  /* now get all columns from original table */
  snprintf(query, 249, "SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnum FROM pg_class c, pg_attribute a WHERE c.relname = %s AND a.attnum > 0 AND a.attrelid = c.oid ORDER BY a.attnum", do_quote_literal(table_orig));
#ifdef TABLE_LOG_DEBUG_QUERY
  elog(NOTICE, "query: %s", query);
#endif /* TABLE_LOG_DEBUG_QUERY */
  ret = SPI_exec(query, 0);
  if (ret != SPI_OK_SELECT) {
    elog(ERROR, "could not get columns from relation: %s", table_orig);
  }
  if (SPI_processed == 0) {
    elog(ERROR, "could not check relation: %s", table_orig);
  }
  results = SPI_processed;
  /* store number columns for later */
  number_columns = SPI_processed;
#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "number columns: %i", results);
#endif /* TABLE_LOG_DEBUG */
  for (i = 0; i < results; i++) {
    /* the column name */
    tmp = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
    col_query_size += strlen(do_quote_ident(tmp)) + 2;
    /* now check, if this is the pkey */
    if (strcmp((const char *)tmp, (const char *)table_orig_pkey) == 0) {
      /* remember the (real) number */
      col_pkey = i + 1;
    }
  }
  /* check if we have found the pkey */
  if (col_pkey == 0) {
    elog(ERROR, "cannot find pkey (%s) in table %s", table_orig_pkey, table_orig);
  }
  /* allocate memory for string */
  col_query_size += 10;
  col_query_start = (char *) palloc((col_query_size + 1) * sizeof(char));
  col_query = col_query_start;
  for (i = 0; i < results; i++) {
    if (i > 0) {
      sprintf(col_query, ", ");
      col_query = col_query_start + strlen(col_query_start);
    }
    sprintf(col_query, "%s", do_quote_ident(SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1)));
    col_query = col_query_start + strlen(col_query_start);
  }
#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "string for columns: %s", col_query_start);
#endif /* TABLE_LOG_DEBUG */

  /* create restore table */
#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "create restore table: %s", table_restore);
#endif /* TABLE_LOG_DEBUG */

  snprintf(query, 249, "SELECT * INTO ");
  /* per default create a temporary table */
  if (not_temporarly == 0) {
    strcat(query, "TEMPORARY ");
  }
  strcat(query, "TABLE ");
  strncat(query, table_restore, 249);
  /* from which table? */
  strncat(query, " FROM ", 249);
  strncat(query, table_orig, 249);
  if (need_search_pkey == 1) {
    /* only extract a specific key */
    strncat(query, " WHERE ", 249);
    strncat(query, do_quote_ident(table_orig_pkey), 249);
    strncat(query, "=", 249);
    strncat(query, do_quote_literal(search_pkey), 249);
  }
  if (method == 0) {
    /* restore from begin (blank table) */
    strncat(query, " LIMIT 0", 249);
  }
#ifdef TABLE_LOG_DEBUG_QUERY
  elog(NOTICE, "query: %s", query);
#endif /* TABLE_LOG_DEBUG_QUERY */
  ret = SPI_exec(query, 0);
  if (ret != SPI_OK_SELINTO) {
    elog(ERROR, "could not check relation: %s", table_restore);
  }
  if (method == 1) {
#ifdef TABLE_LOG_DEBUG
    elog(NOTICE, "%i rows copied", SPI_processed);
#endif /* TABLE_LOG_DEBUG */
  }

  /* get timestamp as string */
  timestamp_string = DatumGetCString(DirectFunctionCall1(timestamptz_out, timestamp));

#ifdef TABLE_LOG_DEBUG
  if (method == 0) {
    elog(NOTICE, "need logs from start to timestamp: %s", timestamp_string);
  } else {
    elog(NOTICE, "need logs from end to timestamp: %s", timestamp_string);
  }
#endif /* TABLE_LOG_DEBUG */

  /* now build query for getting logs */
#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "build query for getting logs");
#endif /* TABLE_LOG_DEBUG */

  d_query_size += d_query_size + strlen(col_query_start);
  if (need_search_pkey == 1) {
    /* add size of pkey and size of value */
    d_query_size += strlen(do_quote_ident(table_orig_pkey)) * 2 + strlen(do_quote_literal(search_pkey)) + 3;
  }

  /* allocate memory for string */
  d_query_size += 10;
  d_query_start = (char *) palloc((d_query_size + 1) * sizeof(char));
  d_query = d_query_start;

  snprintf(d_query, d_query_size, "SELECT %s, trigger_mode, trigger_tuple, trigger_changed FROM %s WHERE ", col_query_start, do_quote_ident(table_log));
  d_query = d_query_start + strlen(d_query_start);
  if (method == 0) {
    /* from start to timestamp */
    snprintf(d_query, d_query_size, "trigger_changed <= %s ", do_quote_literal(timestamp_string));
  } else {
    /* from now() backwards to timestamp */
    snprintf(d_query, d_query_size, "trigger_changed >= %s ", do_quote_literal(timestamp_string));
  }
  d_query = d_query_start + strlen(d_query_start);
  if (need_search_pkey == 1) {
    snprintf(d_query, d_query_size, "AND %s = %s ", do_quote_ident(table_orig_pkey), do_quote_literal(search_pkey));
    d_query = d_query_start + strlen(d_query_start);
  }
  if (method == 0) {
    snprintf(d_query, d_query_size, "ORDER BY %s ASC", do_quote_ident(table_log_pkey));
  } else {
    snprintf(d_query, d_query_size, "ORDER BY %s DESC", do_quote_ident(table_log_pkey));
  }
  d_query = d_query_start + strlen(d_query_start);
#ifdef TABLE_LOG_DEBUG_QUERY
  elog(NOTICE, "query: %s", d_query_start);
#endif /* TABLE_LOG_DEBUG_QUERY */
  ret = SPI_exec(d_query_start, 0);
  if (ret != SPI_OK_SELECT) {
    elog(ERROR, "could not get log data from table: %s", table_log);
  }
#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "number log entries to restore: %i", SPI_processed);
#endif /* TABLE_LOG_DEBUG */
  results = SPI_processed;
  /* save results */
  spi_tuptable = SPI_tuptable;

  /* go through all results */
  for (i = 0; i < results; i++) {
    /* get tuple data */
    trigger_mode = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, number_columns + 1);
    trigger_tuple = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, number_columns + 2);
    trigger_changed = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, number_columns + 3);
    /* check for update tuples we doesnt need */
    if (strcmp((const char *)trigger_mode, (const char *)"UPDATE") == 0) {
      if (method == 0 && strcmp((const char *)trigger_tuple, (const char *)"old") == 0) {
        /* we need the old value of the pkey for the update */
        old_pkey_string = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, col_pkey);
#ifdef TABLE_LOG_DEBUG
        elog(NOTICE, "tuple old pkey: %s", old_pkey_string);
#endif /* TABLE_LOG_DEBUG */
        /* then skip this tuple */
        continue;
      }
      if (method == 1 && strcmp((const char *)trigger_tuple, (const char *)"new") == 0) {
        /* we need the old value of the pkey for the update */
        old_pkey_string = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, col_pkey);
#ifdef TABLE_LOG_DEBUG
        elog(NOTICE, "tuple: old pkey: %s", old_pkey_string);
#endif /* TABLE_LOG_DEBUG */
        /* then skip this tuple */
        continue;
      }
    }

    if (method == 0) {
      /* roll forward */
#ifdef TABLE_LOG_DEBUG
      elog(NOTICE, "tuple: %s  %s  %s", trigger_mode, trigger_tuple, trigger_changed);
#endif /* TABLE_LOG_DEBUG */
      if (strcmp((const char *)trigger_mode, (const char *)"INSERT") == 0) {
        __table_log_restore_table_insert(spi_tuptable, table_restore, table_orig_pkey, col_query_start, col_pkey, number_columns, i);
      } else if (strcmp((const char *)trigger_mode, (const char *)"UPDATE") == 0) {
        __table_log_restore_table_update(spi_tuptable, table_restore, table_orig_pkey, col_query_start, col_pkey, number_columns, i, old_pkey_string);
      } else if (strcmp((const char *)trigger_mode, (const char *)"DELETE") == 0) {
        __table_log_restore_table_delete(spi_tuptable, table_restore, table_orig_pkey, col_query_start, col_pkey, number_columns, i);
      } else {
        elog(ERROR, "unknown trigger_mode: %s", trigger_mode);
      }
    } else {
      /* roll back */
      char rb_mode[10]; /* reverse the method */
      if (strcmp((const char *)trigger_mode, (const char *)"INSERT") == 0) {
        sprintf(rb_mode, "DELETE");
      } else if (strcmp((const char *)trigger_mode, (const char *)"UPDATE") == 0) {
        sprintf(rb_mode, "UPDATE");
      } else if (strcmp((const char *)trigger_mode, (const char *)"DELETE") == 0) {
        sprintf(rb_mode, "INSERT");
      } else {
        elog(ERROR, "unknown trigger_mode: %s", trigger_mode);
      }
#ifdef TABLE_LOG_DEBUG
      elog(NOTICE, "tuple: %s  %s  %s", rb_mode, trigger_tuple, trigger_changed);
#endif /* TABLE_LOG_DEBUG */
      if (strcmp((const char *)trigger_mode, (const char *)"INSERT") == 0) {
        __table_log_restore_table_delete(spi_tuptable, table_restore, table_orig_pkey, col_query_start, col_pkey, number_columns, i);
      } else if (strcmp((const char *)trigger_mode, (const char *)"UPDATE") == 0) {
        __table_log_restore_table_update(spi_tuptable, table_restore, table_orig_pkey, col_query_start, col_pkey, number_columns, i, old_pkey_string);
      } else if (strcmp((const char *)trigger_mode, (const char *)"DELETE") == 0) {
        __table_log_restore_table_insert(spi_tuptable, table_restore, table_orig_pkey, col_query_start, col_pkey, number_columns, i);
      }
    }
  }

#ifdef TABLE_LOG_DEBUG
  elog(NOTICE, "table_log_restore_table() done, results in: %s", table_restore);
#endif /* TABLE_LOG_DEBUG */

  /* convert string to VarChar for result */
  return_name = DatumGetVarCharP(DirectFunctionCall2(varcharin, CStringGetDatum(table_restore), Int32GetDatum(strlen(table_restore) + VARHDRSZ)));

  /* close SPI connection */
  SPI_finish();
  /* and return the name of the restore table */
  PG_RETURN_VARCHAR_P(return_name);
}

void __table_log_restore_table_insert(SPITupleTable *spi_tuptable, char *table_restore, char *table_orig_pkey, char *col_query_start, int col_pkey, int number_columns, int i) {
  int           size_of_values, j, ret;
  char          *tmp;

  /* memory for dynamic query */
  int           d_query_size;
  char          *d_query;
  char          *d_query_start;

  /* get the size of values */
  size_of_values = 0;
  /* go through all columns in this result */
  for (j = 1; j <= number_columns; j++) {
    tmp = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, j);
    if (tmp == NULL) {
      size_of_values += 6;
    } else {
      size_of_values += strlen(do_quote_literal(tmp)) + 3;
    }
  }
  /* reserve memory */
  d_query_size = 250 + strlen(col_query_start) + size_of_values;
  d_query_start = (char *) palloc((d_query_size + 1) * sizeof(char));
  d_query = d_query_start;

  /* build query */
  sprintf(d_query, "INSERT INTO %s (%s) VALUES (", do_quote_ident(table_restore), col_query_start);
  d_query = d_query_start + strlen(d_query_start);

  for (j = 1; j <= number_columns; j++) {
    if (j > 1) {
      strncat(d_query_start, (const char *)", ", d_query_size);
    }
    tmp = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, j);
    if (tmp == NULL) {
      strncat(d_query_start, (const char *)"NULL", d_query_size);
    } else {
      strncat(d_query_start, do_quote_literal(tmp), d_query_size);
    }
  }
  strncat(d_query_start, (const char *)")", d_query_size);
#ifdef TABLE_LOG_DEBUG_QUERY
  elog(NOTICE, "query: %s", d_query_start);
#endif /* TABLE_LOG_DEBUG_QUERY */

  ret = SPI_exec(d_query_start, 0);
  if (ret != SPI_OK_INSERT) {
    elog(ERROR, "could not insert data into: %s", table_restore);
  }
  /* done */
}

void __table_log_restore_table_update(SPITupleTable *spi_tuptable, char *table_restore, char *table_orig_pkey, char *col_query_start, int col_pkey, int number_columns, int i, char *old_pkey_string) {
  int           size_of_values, j, ret;
  char          *tmp, *tmp2;

  /* memory for dynamic query */
  int           d_query_size;
  char          *d_query;
  char          *d_query_start;

  /* get the size of names and values */
  size_of_values = 0;
  /* go through all columns in this result */
  for (j = 1; j <= number_columns; j++) {
    /* get value */
    tmp = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, j);
    /* and get name of column */
    tmp2 = SPI_fname(spi_tuptable->tupdesc, j);
    if (tmp == NULL) {
      size_of_values += 6 + strlen(do_quote_ident(tmp2)) + 2;
    } else {
      size_of_values += strlen(do_quote_literal(tmp)) + strlen(do_quote_ident(tmp2)) + 3;
    }
  }
  /* reserve memory */
  d_query_size = 250 + size_of_values + NAMEDATALEN + strlen(do_quote_literal(old_pkey_string));
  d_query_start = (char *) palloc((d_query_size + 1) * sizeof(char));
  d_query = d_query_start;

  /* build query */
  sprintf(d_query, "UPDATE %s SET ", do_quote_ident(table_restore));
  d_query = d_query_start + strlen(d_query_start);

  for (j = 1; j <= number_columns; j++) {
    if (j > 1) {
      strncat(d_query_start, (const char *)", ", d_query_size);
      d_query += 2;
    }
    tmp = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, j);
    tmp2 = SPI_fname(spi_tuptable->tupdesc, j);
    if (tmp == NULL) {
      snprintf(d_query, d_query_size, "%s=NULL", do_quote_ident(tmp2));
    } else {
      snprintf(d_query, d_query_size, "%s=%s", do_quote_ident(tmp2), do_quote_literal(tmp));
    }
    d_query = d_query_start + strlen(d_query_start);
  }

  snprintf(d_query, d_query_size, " WHERE %s=%s", do_quote_ident(table_orig_pkey), do_quote_literal(old_pkey_string));
  d_query = d_query_start + strlen(d_query_start);

#ifdef TABLE_LOG_DEBUG_QUERY
  elog(NOTICE, "query: %s", d_query_start);
#endif /* TABLE_LOG_DEBUG_QUERY */

  ret = SPI_exec(d_query_start, 0);
  if (ret != SPI_OK_UPDATE) {
    elog(ERROR, "could not update data in: %s", table_restore);
  }
  /* done */
}

void __table_log_restore_table_delete(SPITupleTable *spi_tuptable, char *table_restore, char *table_orig_pkey, char *col_query_start, int col_pkey, int number_columns, int i) {
  int           ret;
  char          *tmp;

  /* memory for dynamic query */
  int           d_query_size;
  char          *d_query;
  char          *d_query_start;

  /* get the size of value */
  tmp = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, col_pkey);
  if (tmp == NULL) {
    elog(ERROR, "pkey cannot be NULL");
  }
  /* reserve memory */
  d_query_size = 250 + strlen(do_quote_ident(table_restore)) + strlen(do_quote_ident(table_orig_pkey)) + strlen(do_quote_literal(tmp));
  d_query_start = (char *) palloc((d_query_size + 1) * sizeof(char));
  d_query = d_query_start;

  /* build query */
  sprintf(d_query, "DELETE FROM %s WHERE %s=%s", do_quote_ident(table_restore), do_quote_ident(table_orig_pkey), do_quote_literal(tmp));
  d_query = d_query_start + strlen(d_query_start);

#ifdef TABLE_LOG_DEBUG_QUERY
  elog(NOTICE, "query: %s", d_query_start);
#endif /* TABLE_LOG_DEBUG_QUERY */

  ret = SPI_exec(d_query_start, 0);
  if (ret != SPI_OK_DELETE) {
    elog(ERROR, "could not delete data from: %s", table_restore);
  }
  /* done */
}








/*
 * MULTIBYTE dependant internal functions follow
 *
 */
/* from src/backend/utils/adt/quote.c and slightly modified */

#ifndef MULTIBYTE

/* Return a properly quoted identifier */
static char * do_quote_ident(char *iptr) {
  char    *result;
  char    *result_return;
  char    *cp1;
  char    *cp2;
  int     len;

  len = strlen(iptr);
  result = (char *) palloc(len * 2 + 3);
  result_return = result;

  cp1 = VARDATA(iptr);
  cp2 = VARDATA(result);

  *result++ = '"';
  while (len-- > 0) {
    if (*iptr == '"') {
      *result++ = '"';
    }
    if (*iptr == '\\') {
      /* just add a backslash, the ' will be follow */
      *result++ = '\\';
    }
    *result++ = *iptr++;
  }
  *result++ = '"';
  *result++ = '\0';

  return result_return;
}

/* Return a properly quoted literal value */
static char * do_quote_literal(char *lptr) {
  char    *result;
  char    *result_return;
  int     len;

  len = strlen(lptr);
  result = (char *) palloc(len * 2 + 3);
  result_return = result;

  *result++ = '\'';
  while (len-- > 0) {
    if (*lptr == '\'') {
      *result++ = '\\';
    }
    if (*lptr == '\\') {
      /* just add a backslash, the ' will be follow */
      *result++ = '\\';
    }
    *result++ = *lptr++;
  }
  *result++ = '\'';
  *result++ = '\0';

  return result_return;
}

#else

/* Return a properly quoted identifier (MULTIBYTE version) */
static char * do_quote_ident(char *iptr) {
  char    *result;
  char    *result_return;
  int     len;
  int     wl;

  len = strlen(iptr);
  result = (char *) palloc(len * 2 + 3);
  result_return = result;

  *result++ = '"';
  while (len > 0) {
    if ((wl = pg_mblen(iptr)) != 1) {
      len -= wl;

      while (wl-- > 0) {
        *result++ = *iptr++;
      }
      continue;
    }

    if (*iptr == '"') {
      *result++ = '"';
    }
    if (*iptr == '\\') {
      /* just add a backslash, the ' will be follow */
      *result++ = '\\';
    }
    *result++ = *iptr++;

    len--;
  }
  *result++ = '"';
  *result++ = '\0';

  return result_return;
}

/* Return a properly quoted literal value (MULTIBYTE version) */
static char * do_quote_literal(char *lptr) {
  char    *result;
  char    *result_return;
  int     len;
  int     wl;

  len = strlen(lptr);
  result = (char *) palloc(len * 2 + 3);
  result_return = result;

  *result++ = '\'';
  while (len > 0) {
    if ((wl = pg_mblen(lptr)) != 1) {
      len -= wl;

      while (wl-- > 0) {
        *result++ = *lptr++;
      }
      continue;
    }

    if (*lptr == '\'') {
      *result++ = '\\';
    }
    if (*lptr == '\\') {
      /* just add a backslash, the ' will be follow */
      *result++ = '\\';
    }
    *result++ = *lptr++;

    len--;
  }
  *result++ = '\'';
  *result++ = '\0';

  return result_return;
}

#endif /* MULTIBYTE */

char * __table_log_varcharout(VarChar *s) {
  char	    *result;
  int32     len;

  /* copy and add null term */
  len = VARSIZE(s) - VARHDRSZ;
  result = palloc(len + 1);
  memcpy(result, VARDATA(s), len);
  result[len] = '\0';

#ifdef CYR_RECODE
  convertstr(result, len, 1);
#endif

  return result;
}
