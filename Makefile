MODULE_big = argm
OBJS = argm.o

EXTENSION = argm
DATA = argm--1.1.1.sql argm--1.0--1.0.2.sql argm--1.0.2--1.0.3.sql argm--1.0.3--1.1.0.sql argm--1.1.0--1.1.1.sql
REGRESS = argm anyold

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
