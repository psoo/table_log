MODULES = table_log
DATA_built = table_log.sql
DOCS = README.table_log

ifdef USE_PGXS
  PGXS := $(shell pg_config --pgxs)
  include $(PGXS)
else
  subdir = contrib/table_log
  top_builddir = ../..
  include $(top_builddir)/src/Makefile.global
  include $(top_srcdir)/contrib/contrib-global.mk
endif
