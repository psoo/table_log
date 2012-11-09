MODULES = table_log
EXTENSION = table_log
DATA = table_log--0.5.sql table_log_init.sql table_log--unpackaged--0.5.sql
## keep it for non-EXTENSION installations
DATA_built = table_log.sql uninstall_table_log.sql
DOCS = README.table_log
REGRESS=table_log

PGXS := $(shell pg_config --pgxs)
include $(PGXS)
