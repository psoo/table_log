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

#include <ctype.h>		/* tolower () */
#include <string.h>		/* strlen() */

#include "postgres.h"
#include "fmgr.h"
#include "executor/spi.h"	/* this is what you need to work with SPI */
#include "commands/trigger.h"	/* -"- and triggers */
#include "mb/pg_wchar.h"	/* support for the quoting functions */
#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "utils/formatting.h"
#include "utils/builtins.h"
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/timestamp.h>
#include "funcapi.h"

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
int count_columns (TupleDesc tupleDesc)
{
	int count = 0;
	int i;

	for (i = 0; i < tupleDesc->natts; ++i)
	{
		if (!tupleDesc->attrs[i]->attisdropped)
		{
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
Datum table_log(PG_FUNCTION_ARGS)
{
	TriggerData    *trigdata = (TriggerData *) fcinfo->context;
	int            ret;
	StringInfo     query;
	int            number_columns = 0;		/* counts the number columns in the table */
	int            number_columns_log = 0;	/* counts the number columns in the table */
	char           *orig_schema;
	char           *log_schema;
	char           *log_table;
	int            use_session_user = 0;    /* should we write the current (session) user to the log table? */

	/*
	 * Some checks first...
	 */

	elog(DEBUG2, "start table_log()");

	/* called by trigger manager? */
	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		elog(ERROR, "table_log: not fired by trigger manager");
	}

	/* must only be called for ROW trigger */
	if (TRIGGER_FIRED_FOR_STATEMENT(trigdata->tg_event))
	{
		elog(ERROR, "table_log: can't process STATEMENT events");
	}

	/* must only be called AFTER */
	if (TRIGGER_FIRED_BEFORE(trigdata->tg_event))
	{
		elog(ERROR, "table_log: must be fired after event");
	}

	/* now connect to SPI manager */
	ret = SPI_connect();

	if (ret != SPI_OK_CONNECT)
	{
		elog(ERROR, "table_log: SPI_connect returned %d", ret);
	}

	/* get schema name for the table, in case we need it later */
	orig_schema = get_namespace_name(RelationGetNamespace(trigdata->tg_relation));

	elog(DEBUG2, "prechecks done, now getting original table attributes");

	number_columns = count_columns(trigdata->tg_relation->rd_att);
	if (number_columns < 1)
	{
		elog(ERROR, "table_log: number of columns in table is < 1, can this happen?");
	}

	elog(DEBUG2, "number columns in orig table: %i", number_columns);

	if (trigdata->tg_trigger->tgnargs > 3)
	{
		elog(ERROR, "table_log: too many arguments to trigger");
	}

  /* name of the log schema */
	if (trigdata->tg_trigger->tgnargs > 2)
	{
		/* check if a log schema argument is given, if yes, use it */
		log_schema = trigdata->tg_trigger->tgargs[2];
	}
	else
	{
		/* if no, use orig_schema */
		log_schema = orig_schema;
	}

	/* should we write the current user? */
	if (trigdata->tg_trigger->tgnargs > 1)
	{
		/*
		 * check if a second argument is given
		 * if yes, use it, if it is true
		 */
		if (atoi(trigdata->tg_trigger->tgargs[1]) == 1)
		{
			use_session_user = 1;
			elog(DEBUG2, "will write session user to 'trigger_user'");
		}
	}

	/* name of the log table */
	if (trigdata->tg_trigger->tgnargs > 0)
	{
		/*
		 * check if a logtable argument is given
		 * if yes, use it
		 */
		log_table = pstrdup(trigdata->tg_trigger->tgargs[0]);
		sprintf(log_table, "%s", trigdata->tg_trigger->tgargs[0]);
	}
	else
	{
		/* if no, use 'table name' + '_log' */
		log_table = (char *) palloc((strlen(do_quote_ident(SPI_getrelname(trigdata->tg_relation))) + 5)
									* sizeof(char));
		sprintf(log_table, "%s_log", SPI_getrelname(trigdata->tg_relation));
	}

	elog(DEBUG2, "log table: %s", log_table);
	elog(DEBUG2, "now check, if log table exists");

	/* get the number columns in the table */
	query = makeStringInfo();
	appendStringInfo(query, "%s.%s", do_quote_ident(log_schema), do_quote_ident(log_table));
	number_columns_log = count_columns(RelationNameGetTupleDesc(query->data));

	if (number_columns_log < 1)
	{
		elog(ERROR, "could not get number columns in relation %s", log_table);
	}

    elog(DEBUG2, "number columns in log table: %i", number_columns_log);

	/*
	 * check if the logtable has 3 (or now 4) columns more than our table
	 * +1 if we should write the session user
	 */

	if (use_session_user == 0)
	{
		/* without session user */
		if (number_columns_log != number_columns + 3 && number_columns_log != number_columns + 4)
		{
			elog(ERROR, "number colums in relation %s(%d) does not match columns in %s(%d)",
				 SPI_getrelname(trigdata->tg_relation), number_columns,
				 log_table, number_columns_log);
		}
	}
	else
	{
		/* with session user */
		if (number_columns_log != number_columns + 3 + 1 && number_columns_log != number_columns + 4 + 1)
		{
			elog(ERROR, "number colums in relation %s does not match columns in %s",
				 SPI_getrelname(trigdata->tg_relation), log_table);
		}
	}

	elog(DEBUG2, "log table OK");
	/* For each column in key ... */
	elog(DEBUG2, "copy data ...");

	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
	{
		/* trigger called from INSERT */
		elog(DEBUG2, "mode: INSERT -> new");

		__table_log(trigdata, "INSERT", "new", trigdata->tg_trigtuple, number_columns, log_table, use_session_user, log_schema);
	}
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		/* trigger called from UPDATE */
		elog(DEBUG2, "mode: UPDATE -> old");

		__table_log(trigdata, "UPDATE", "old", trigdata->tg_trigtuple, number_columns, log_table, use_session_user, log_schema);

		elog(DEBUG2, "mode: UPDATE -> new");

		__table_log(trigdata, "UPDATE", "new", trigdata->tg_newtuple, number_columns, log_table, use_session_user, log_schema);
	}
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
	{
		/* trigger called from DELETE */
		elog(DEBUG2, "mode: DELETE -> old");

		__table_log(trigdata, "DELETE", "old", trigdata->tg_trigtuple, number_columns, log_table, use_session_user, log_schema);
	}
	else
	{
		elog(ERROR, "trigger fired by unknown event");
	}

	elog(DEBUG2, "cleanup, trigger done");

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
static void __table_log (TriggerData *trigdata, char *changed_mode,
						 char *changed_tuple, HeapTuple tuple,
						 int number_columns, char *log_table,
						 int use_session_user, char *log_schema)
{
	StringInfo query;
	char      *before_char;
	int        i;
	int        col_nr;
	int        found_col;
	int        ret;

	elog(DEBUG2, "build query");

	/* allocate memory */
	query = makeStringInfo();

	/* build query */
	appendStringInfo(query, "INSERT INTO %s.%s (",
					 do_quote_ident(log_schema), do_quote_ident(log_table));

	/* add colum names */
	col_nr = 0;

	for (i = 1; i <= number_columns; i++)
	{
		col_nr++;
		found_col = 0;

		do
		{
			if (trigdata->tg_relation->rd_att->attrs[col_nr - 1]->attisdropped)
			{
				/* this column is dropped, skip it */
				col_nr++;
				continue;
			}
			else
			{
				found_col++;
			}
		}
		while (found_col == 0);

		appendStringInfo(query,
						 "%s, ",
						 do_quote_ident(SPI_fname(trigdata->tg_relation->rd_att, col_nr)));
	}

	/* add session user */
	if (use_session_user == 1)
		appendStringInfo(query, "trigger_user, ");

	/* add the 3 extra colum names */
	appendStringInfo(query, "trigger_mode, trigger_tuple, trigger_changed) VALUES (");

	/* add values */
	col_nr = 0;
	for (i = 1; i <= number_columns; i++)
	{
		col_nr++;
		found_col = 0;

		do
		{
			if (trigdata->tg_relation->rd_att->attrs[col_nr - 1]->attisdropped)
			{
				/* this column is dropped, skip it */
				col_nr++;
				continue;
			}
			else
			{
				found_col++;
			}
		}
		while (found_col == 0);

		before_char = SPI_getvalue(tuple, trigdata->tg_relation->rd_att, col_nr);
		if (before_char == NULL)
		{
			appendStringInfo(query, "NULL, ");
		}
		else
		{
			appendStringInfo(query, "%s, ",
							 do_quote_literal(before_char));
		}
	}

	/* add session user */
	if (use_session_user == 1)
		appendStringInfo(query, "SESSION_USER, ");

	/* add the 3 extra values */
	appendStringInfo(query, "%s, %s, NOW());",
					 do_quote_literal(changed_mode), do_quote_literal(changed_tuple));

	elog(DEBUG3, "query: %s", query->data);
	elog(DEBUG2, "execute query");

	/* execute insert */
	ret = SPI_exec(query->data, 0);
	if (ret != SPI_OK_INSERT)
	{
		elog(ERROR, "could not insert log information into relation %s (error: %d)", log_table, ret);
	}

	/* clean up */
	pfree(query->data);
	pfree(query);

	elog(DEBUG2, "done");
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
Datum table_log_show_column(PG_FUNCTION_ARGS)
{
	TriggerData    *trigdata = (TriggerData *) fcinfo->context;
	int            ret;

	/*
	 * Some checks first...
	 */
	elog(DEBUG2, "start table_log_show_column()");

	/* Connect to SPI manager */
	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
	{
		elog(ERROR, "table_log_show_column: SPI_connect returned %d", ret);
	}

	elog(DEBUG2, "this function isnt available yet");

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
Datum table_log_restore_table(PG_FUNCTION_ARGS)
{
	/* the original table name */
	char  *table_orig;
	/* the primary key in the original table */
	char  *table_orig_pkey;
	/* number columns in original table */
	int  table_orig_columns = 0;
	/* the log table name */
	char  *table_log;
	/* the primary key in the log table (usually trigger_id) */
	/* cannot be the same then then the pkey in the original table */
	char  *table_log_pkey;
	/* number columns in log table */
	int  table_log_columns = 0;
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

    /*
	 * for getting table infos
	 */
	StringInfo     query;

	int            need_search_pkey = 0;          /* does we have a single key to restore? */
	char           *tmp, *timestamp_string, *old_pkey_string = "";
	char           *trigger_mode;
	char           *trigger_tuple;
	char           *trigger_changed;
	SPITupleTable  *spi_tuptable = NULL;          /* for saving query results */
	VarChar        *return_name;

	/* memory for dynamic query */
	StringInfo      d_query;

	/* memory for column names */
	StringInfo      col_query;

	int      col_pkey = 0;

	/*
	 * Some checks first...
	 */
	elog(DEBUG2, "start table_log_restore_table()");

  /* does we have all arguments? */
	if (PG_ARGISNULL(0))
	{
		elog(ERROR, "table_log_restore_table: missing original table name");
	}
	if (PG_ARGISNULL(1))
	{
		elog(ERROR, "table_log_restore_table: missing primary key name for original table");
	}
	if (PG_ARGISNULL(2))
	{
		elog(ERROR, "table_log_restore_table: missing log table name");
	}
	if (PG_ARGISNULL(3))
	{
		elog(ERROR, "table_log_restore_table: missing primary key name for log table");
	}
	if (PG_ARGISNULL(4))
	{
		elog(ERROR, "table_log_restore_table: missing copy table name");
	}
	if (PG_ARGISNULL(5))
	{
		elog(ERROR, "table_log_restore_table: missing timestamp");
	}

	/* first check number arguments to avoid an segfault */
	if (PG_NARGS() >= 7)
	{
		/* if argument is given, check if not null */
		if (!PG_ARGISNULL(6))
		{
			/* yes, fetch it */
			search_pkey = __table_log_varcharout((VarChar *)PG_GETARG_VARCHAR_P(6));

			/* and check, if we have an argument */
			if (strlen(search_pkey) > 0)
			{
				need_search_pkey = 1;
				elog(DEBUG2, "table_log_restore_table: will restore a single key");
			}
		}
	} /* nargs >= 7 */

	/* same procedere here */
	if (PG_NARGS() >= 8)
	{
		if (!PG_ARGISNULL(7))
		{
			method = PG_GETARG_INT32(7);

			if (method > 0)
			{
				method = 1;
			}
			else
			{
				method = 0;
			}
		}
	} /* nargs >= 8 */

	if (method == 1)
		elog(DEBUG2, "table_log_restore_table: will restore from actual state backwards");
	else
		elog(DEBUG2, "table_log_restore_table: will restore from begin forward");

	if (PG_NARGS() >= 9)
	{
		if (!PG_ARGISNULL(8))
		{
			not_temporarly = PG_GETARG_INT32(8);

			if (not_temporarly > 0)
			{
				not_temporarly = 1;
				elog(DEBUG2, "table_log_restore_table: dont create restore table temporarly");
			}
			else
			{
				not_temporarly = 0;
			}
		}
 	} /* nargs >= 9 */

	/* get parameter */
	table_orig = __table_log_varcharout((VarChar *)PG_GETARG_VARCHAR_P(0));
	table_orig_pkey = __table_log_varcharout((VarChar *)PG_GETARG_VARCHAR_P(1));
	table_log = __table_log_varcharout((VarChar *)PG_GETARG_VARCHAR_P(2));
	table_log_pkey = __table_log_varcharout((VarChar *)PG_GETARG_VARCHAR_P(3));
	table_restore = __table_log_varcharout((VarChar *)PG_GETARG_VARCHAR_P(4));

	/* pkey of original table cannot be the same as of log table */
	if (strcmp((const char *)table_orig_pkey, (const char *)table_log_pkey) == 0)
	{
		elog(ERROR, "pkey of logging table cannot be the pkey of the original table: %s <-> %s",
			 table_orig_pkey, table_log_pkey);
	}

	/* Connect to SPI manager */
	ret = SPI_connect();

	if (ret != SPI_OK_CONNECT)
	{
		elog(ERROR, "table_log_restore_table: SPI_connect returned %d", ret);
	}

	/* check original table */
	query = makeStringInfo();
	appendStringInfo(query,
					 "SELECT a.attname FROM pg_class c, pg_attribute a WHERE c.relname = %s AND a.attnum > 0 AND a.attrelid = c.oid ORDER BY a.attnum",
					 do_quote_literal(table_orig));

	elog(DEBUG3, "query: %s", query->data);

	ret = SPI_exec(query->data, 0);

	if (ret != SPI_OK_SELECT)
	{
		elog(ERROR, "could not check relation: %s", table_orig);
	}

	if (SPI_processed > 0)
	{
		table_orig_columns = SPI_processed;
	} else {
		elog(ERROR, "could not check relation: %s", table_orig);
	}

	/* check pkey in original table */
	resetStringInfo(query);
	appendStringInfo(query,
					 "SELECT a.attname FROM pg_class c, pg_attribute a WHERE c.relname=%s AND c.relkind='r' AND a.attname=%s AND a.attnum > 0 AND a.attrelid = c.oid",
					 do_quote_literal(table_orig), do_quote_literal(table_orig_pkey));

	elog(DEBUG3, "query: %s", query->data);

	ret = SPI_exec(query->data, 0);

	if (ret != SPI_OK_SELECT)
	{
		elog(ERROR, "could not check relation: %s", table_orig);
	}

	if (SPI_processed == 0)
	{
		elog(ERROR, "could not check relation: %s", table_orig);
	}

	elog(DEBUG2, "original table: OK (%i columns)", table_orig_columns);

	/* check log table */
	resetStringInfo(query);
	appendStringInfo(query,
					 "SELECT a.attname FROM pg_class c, pg_attribute a WHERE c.relname = %s AND a.attnum > 0 AND a.attrelid = c.oid ORDER BY a.attnum",
					 do_quote_literal(table_log));

	elog(DEBUG3, "query: %s", query->data);

	ret = SPI_exec(query->data, 0);

	if (ret != SPI_OK_SELECT)
	{
		elog(ERROR, "could not check relation [1]: %s", table_log);
	}

	if (SPI_processed > 0)
	{
		table_log_columns = SPI_processed;
	} else {
		elog(ERROR, "could not check relation [2]: %s", table_log);
	}

	/* check pkey in log table */
	resetStringInfo(query);
	appendStringInfo(query,
					 "SELECT a.attname FROM pg_class c, pg_attribute a WHERE c.relname=%s AND c.relkind='r' AND a.attname=%s AND a.attnum > 0 AND a.attrelid = c.oid",
					 do_quote_literal(table_log), do_quote_literal(table_log_pkey));

	elog(DEBUG3, "query: %s", query->data);

	ret = SPI_exec(query->data, 0);

	if (ret != SPI_OK_SELECT)
	{
		elog(ERROR, "could not check relation [3]: %s", table_log);
	}

	if (SPI_processed == 0)
	{
		elog(ERROR, "could not check relation [4]: %s", table_log);
	}

	elog(DEBUG3, "log table: OK (%i columns)", table_log_columns);

	/* check restore table */
	resetStringInfo(query);
	appendStringInfo(query,
					 "SELECT pg_attribute.attname AS a FROM pg_class, pg_attribute WHERE pg_class.relname=%s AND pg_attribute.attnum > 0 AND pg_attribute.attrelid=pg_class.oid",
					 do_quote_literal(table_restore));

	elog(DEBUG3, "query: %s", query->data);

	ret = SPI_exec(query->data, 0);

	if (ret != SPI_OK_SELECT)
	{
		elog(ERROR, "could not check relation: %s", table_restore);
	}

	if (SPI_processed > 0)
	{
		elog(ERROR, "restore table already exists: %s", table_restore);
	}

	elog(DEBUG2, "restore table: OK (doesnt exists)");

	/* now get all columns from original table */
	resetStringInfo(query);
	appendStringInfo(query,
					 "SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnum FROM pg_class c, pg_attribute a WHERE c.relname = %s AND a.attnum > 0 AND a.attrelid = c.oid ORDER BY a.attnum",
					 do_quote_literal(table_orig));

	elog(DEBUG3, "query: %s", query->data);

	ret = SPI_exec(query->data, 0);

	if (ret != SPI_OK_SELECT)
	{
		elog(ERROR, "could not get columns from relation: %s", table_orig);
	}

	if (SPI_processed == 0)
	{
		elog(ERROR, "could not check relation: %s", table_orig);
	}

	results = SPI_processed;

	/* store number columns for later */
	number_columns = SPI_processed;

	elog(DEBUG2, "number columns: %i", results);

	for (i = 0; i < results; i++)
	{
		/* the column name */
		tmp = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);

		/* now check, if this is the pkey */
		if (strcmp((const char *)tmp, (const char *)table_orig_pkey) == 0)
		{
			/* remember the (real) number */
			col_pkey = i + 1;
		}
	}

	/* check if we have found the pkey */
	if (col_pkey == 0)
	{
		elog(ERROR, "cannot find pkey (%s) in table %s", table_orig_pkey, table_orig);
	}

	/* allocate memory for string */
	col_query = makeStringInfo();

	for (i = 0; i < results; i++)
	{
		if (i > 0)
			appendStringInfo(col_query, ", ");

		appendStringInfo(col_query, "%s",
						 do_quote_ident(SPI_getvalue(SPI_tuptable->vals[i],
													 SPI_tuptable->tupdesc, 1)));
	}

	/* create restore table */
	elog(DEBUG2, "string for columns: %s", col_query->data);
	elog(DEBUG2, "create restore table: %s", table_restore);
	resetStringInfo(query);
	appendStringInfo(query, "SELECT * INTO ");

	/* per default create a temporary table */
	if (not_temporarly == 0)
	{
		appendStringInfo(query, "TEMPORARY ");
	}

	/* from which table? */
	appendStringInfo(query, "TABLE %s FROM %s ", table_restore, table_orig);

	if (need_search_pkey == 1)
	{
		/* only extract a specific key */
		appendStringInfo(query, "WHERE %s = %s ",
						 do_quote_ident(table_orig_pkey),
						 do_quote_literal(search_pkey));
	}

	if (method == 0)
	{
		/* restore from begin (blank table) */
		appendStringInfo(query, "LIMIT 0");
	}

	elog(DEBUG3, "query: %s", query->data);

	ret = SPI_exec(query->data, 0);

	if (ret != SPI_OK_SELINTO)
	{
		elog(ERROR, "could not check relation: %s", table_restore);
	}

	if (method == 1)
		elog(DEBUG2, "%i rows copied", SPI_processed);

	/* get timestamp as string */
	timestamp_string = DatumGetCString(DirectFunctionCall1(timestamptz_out, timestamp));

	if (method == 0)
		elog(DEBUG2, "need logs from start to timestamp: %s", timestamp_string);
	else
		elog(DEBUG2, "need logs from end to timestamp: %s", timestamp_string);

	/* now build query for getting logs */
	elog(DEBUG2, "build query for getting logs");

	/* allocate memory for string and build query */
	d_query = makeStringInfo();
	appendStringInfo(d_query,
					 "SELECT %s, trigger_mode, trigger_tuple, trigger_changed FROM %s WHERE ",
					 col_query->data, do_quote_ident(table_log));

	if (method == 0)
	{
		/* from start to timestamp */
		appendStringInfo(d_query, "trigger_changed <= %s",
						 do_quote_literal(timestamp_string));
	}
	else
	{
		/* from now() backwards to timestamp */
		appendStringInfo(d_query, "trigger_changed >= %s ",
						 do_quote_literal(timestamp_string));
	}

	if (need_search_pkey == 1)
	{
		appendStringInfo(d_query, "AND %s = %s ",
						 do_quote_ident(table_orig_pkey),
						 do_quote_literal(search_pkey));
	}

	if (method == 0)
	{
		appendStringInfo(d_query, "ORDER BY %s ASC",
						 do_quote_ident(table_log_pkey));
	}
	else
	{
		appendStringInfo(d_query, "ORDER BY %s DESC",
						 do_quote_ident(table_log_pkey));
	}

	elog(DEBUG3, "query: %s", d_query->data);

	ret = SPI_exec(d_query->data, 0);

	if (ret != SPI_OK_SELECT)
	{
		elog(ERROR, "could not get log data from table: %s", table_log);
	}

	elog(DEBUG2, "number log entries to restore: %i", SPI_processed);

	results = SPI_processed;
	/* save results */
	spi_tuptable = SPI_tuptable;

	/* go through all results */
	for (i = 0; i < results; i++)
	{

		/* get tuple data */
		trigger_mode = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, number_columns + 1);
		trigger_tuple = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, number_columns + 2);
		trigger_changed = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, number_columns + 3);

		/* check for update tuples we doesnt need */
		if (strcmp((const char *)trigger_mode, (const char *)"UPDATE") == 0)
		{
			if (method == 0 && strcmp((const char *)trigger_tuple, (const char *)"old") == 0)
			{
				/* we need the old value of the pkey for the update */
				old_pkey_string = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, col_pkey);
				elog(DEBUG2, "tuple old pkey: %s", old_pkey_string);

				/* then skip this tuple */
				continue;
			}

			if (method == 1 && strcmp((const char *)trigger_tuple, (const char *)"new") == 0)
			{
				/* we need the old value of the pkey for the update */
				old_pkey_string = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, col_pkey);
				elog(DEBUG2, "tuple: old pkey: %s", old_pkey_string);

				/* then skip this tuple */
				continue;
			}
		}

		if (method == 0)
		{
			/* roll forward */
			elog(DEBUG2, "tuple: %s  %s  %s", trigger_mode, trigger_tuple, trigger_changed);

			if (strcmp((const char *)trigger_mode, (const char *)"INSERT") == 0)
			{
				__table_log_restore_table_insert(spi_tuptable, table_restore, table_orig_pkey, col_query->data, col_pkey, number_columns, i);
			}
			else if (strcmp((const char *)trigger_mode, (const char *)"UPDATE") == 0)
			{
				__table_log_restore_table_update(spi_tuptable, table_restore, table_orig_pkey, col_query->data, col_pkey, number_columns, i, old_pkey_string);
			}
			else if (strcmp((const char *)trigger_mode, (const char *)"DELETE") == 0)
			{
				__table_log_restore_table_delete(spi_tuptable, table_restore, table_orig_pkey, col_query->data, col_pkey, number_columns, i);
			}
			else
			{
				elog(ERROR, "unknown trigger_mode: %s", trigger_mode);
			}

		}
		else
		{
			/* roll back */
			char rb_mode[10]; /* reverse the method */

			if (strcmp((const char *)trigger_mode, (const char *)"INSERT") == 0)
			{
				sprintf(rb_mode, "DELETE");
			}
			else if (strcmp((const char *)trigger_mode, (const char *)"UPDATE") == 0)
			{
				sprintf(rb_mode, "UPDATE");
			}
			else if (strcmp((const char *)trigger_mode, (const char *)"DELETE") == 0)
			{
				sprintf(rb_mode, "INSERT");
			}
			else
			{
				elog(ERROR, "unknown trigger_mode: %s", trigger_mode);
			}

			elog(DEBUG2, "tuple: %s  %s  %s", rb_mode, trigger_tuple, trigger_changed);

			if (strcmp((const char *)trigger_mode, (const char *)"INSERT") == 0)
			{
				__table_log_restore_table_delete(spi_tuptable, table_restore, table_orig_pkey, col_query->data, col_pkey, number_columns, i);
			}
			else if (strcmp((const char *)trigger_mode, (const char *)"UPDATE") == 0)
			{
				__table_log_restore_table_update(spi_tuptable, table_restore, table_orig_pkey, col_query->data, col_pkey, number_columns, i, old_pkey_string);
			}
			else if (strcmp((const char *)trigger_mode, (const char *)"DELETE") == 0)
			{
				__table_log_restore_table_insert(spi_tuptable, table_restore, table_orig_pkey, col_query->data, col_pkey, number_columns, i);
			}
		}
	}

	/* close SPI connection */
	SPI_finish();

	elog(DEBUG2, "table_log_restore_table() done, results in: %s", table_restore);

	/* convert string to VarChar for result */
	return_name = DatumGetVarCharP(DirectFunctionCall2(varcharin, CStringGetDatum(table_restore), Int32GetDatum(strlen(table_restore) + VARHDRSZ)));

	/* and return the name of the restore table */
	PG_RETURN_VARCHAR_P(return_name);
}

void __table_log_restore_table_insert(SPITupleTable *spi_tuptable, char *table_restore,
									  char *table_orig_pkey, char *col_query_start,
									  int col_pkey, int number_columns, int i) {
	int            j;
	int            ret;
	char          *tmp;

	/* memory for dynamic query */
	StringInfo     d_query;

	d_query = makeStringInfo();

	/* build query */
	appendStringInfo(d_query, "INSERT INTO %s (%s) VALUES (",
					 do_quote_ident(table_restore),
					 col_query_start);

	for (j = 1; j <= number_columns; j++)
	{
		if (j > 1)
		{
			appendStringInfoString(d_query, ", ");
		}

		tmp = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, j);

		if (tmp == NULL)
		{
			appendStringInfoString(d_query, "NULL");
		}
		else
		{
			appendStringInfoString(d_query, do_quote_literal(tmp));
		}
	}

	appendStringInfoString(d_query, ")");
	elog(DEBUG3, "query: %s", d_query->data);

	ret = SPI_exec(d_query->data, 0);

	if (ret != SPI_OK_INSERT) {
		elog(ERROR, "could not insert data into: %s", table_restore);
	}

	/* done */
}

void __table_log_restore_table_update(SPITupleTable *spi_tuptable, char *table_restore,
									  char *table_orig_pkey, char *col_query_start,
									  int col_pkey, int number_columns,
									  int i, char *old_pkey_string) {
	int   j;
	int   ret;
	char *tmp;
	char *tmp2;

	/* memory for dynamic query */
	StringInfo d_query;

	d_query = makeStringInfo();

	/* build query */
	appendStringInfo(d_query, "UPDATE %s SET ",
					 do_quote_ident(table_restore));

	for (j = 1; j <= number_columns; j++)
	{
		if (j > 1)
		{
			appendStringInfoString(d_query, ", ");
		}

		tmp = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, j);
		tmp2 = SPI_fname(spi_tuptable->tupdesc, j);

		if (tmp == NULL)
		{
			appendStringInfo(d_query, "%s=NULL", do_quote_ident(tmp2));
		}
		else
		{
			appendStringInfo(d_query, "%s=%s",
							 do_quote_ident(tmp2), do_quote_literal(tmp));
		}
	}

	appendStringInfo(d_query,
			 " WHERE %s=%s",
			 do_quote_ident(table_orig_pkey),
			 do_quote_literal(old_pkey_string));

	elog(DEBUG3, "query: %s", d_query->data);

	ret = SPI_exec(d_query->data, 0);

  if (ret != SPI_OK_UPDATE)
  {
	  elog(ERROR, "could not update data in: %s", table_restore);
  }

  /* done */
}

void __table_log_restore_table_delete(SPITupleTable *spi_tuptable, char *table_restore,
									  char *table_orig_pkey, char *col_query_start,
									  int col_pkey, int number_columns, int i) {
	int   ret;
	char *tmp;

	/* memory for dynamic query */
	StringInfo d_query;

	/* get the size of value */
	tmp = SPI_getvalue(spi_tuptable->vals[i], spi_tuptable->tupdesc, col_pkey);

	if (tmp == NULL)
	{
		elog(ERROR, "pkey cannot be NULL");
	}

	/* initalize StringInfo structure */
	d_query = makeStringInfo();

	/* build query */
	appendStringInfo(d_query,
					 "DELETE FROM %s WHERE %s=%s",
					 do_quote_ident(table_restore),
					 do_quote_ident(table_orig_pkey),
					 do_quote_literal(tmp));

	elog(DEBUG3, "query: %s", d_query->data);

	ret = SPI_exec(d_query->data, 0);

	if (ret != SPI_OK_DELETE)
	{
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
static char * do_quote_ident(char *iptr)
{
	char    *result;
	char    *result_return;
	int     len;

	len           = strlen(iptr);
	result        = (char *) palloc(len * 2 + 3);
	result_return = result;
	*result++     = '"';

	while (len-- > 0)
	{
		if (*iptr == '"')
		{
			*result++ = '"';
		}

		if (*iptr == '\\')
		{
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
static char * do_quote_literal(char *lptr)
{
	char    *result;
	char    *result_return;
	int     len;

	len           = strlen(lptr);
	result        = (char *) palloc(len * 2 + 3);
	result_return = result;
	*result++     = '\'';

	while (len-- > 0)
	{
		if (*lptr == '\'')
		{
			*result++ = '\\';
		}

		if (*lptr == '\\')
		{
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
static char * do_quote_ident(char *iptr)
{
	char    *result;
	char    *result_return;
	int     len;
	int     wl;

	len           = strlen(iptr);
	result        = (char *) palloc(len * 2 + 3);
	result_return = result;
	*result++     = '"';

	while (len > 0)
	{
		if ((wl = pg_mblen(iptr)) != 1)
		{
			len -= wl;

			while (wl-- > 0)
			{
				*result++ = *iptr++;
			}
			continue;
		}

		if (*iptr == '"')
		{
			*result++ = '"';
		}

		if (*iptr == '\\')
		{
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
static char * do_quote_literal(char *lptr)
{
	char    *result;
	char    *result_return;
	int     len;
	int     wl;

	len           = strlen(lptr);
	result        = (char *) palloc(len * 2 + 3);
	result_return = result;
	*result++     = '\'';

	while (len > 0)
	{
		if ((wl = pg_mblen(lptr)) != 1)
		{
			len -= wl;

			while (wl-- > 0)
			{
				*result++ = *lptr++;
			}
			continue;
		}

		if (*lptr == '\'')
		{
			*result++ = '\\';
		}

		if (*lptr == '\\')
		{
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

char * __table_log_varcharout(VarChar *s)
{
	char *result;
	int32 len;

	/* copy and add null term */
	len    = VARSIZE(s) - VARHDRSZ;
	result = palloc(len + 1);
	memcpy(result, VARDATA(s), len);
	result[len] = '\0';

#ifdef CYR_RECODE
	convertstr(result, len, 1);
#endif

	return result;
}
