from importlib.resources import files

SQL_CREATE   = files("sql.sql_create")
SQL_INDEXES  = files("sql.sql_indexes")
SQL_TRIGGERS = files("sql.sql_triggers")
SQL_VIEWS    = files("sql.sql_views")
SQL_CLEANUP  = files("sql.sql_cleanup")
SQL_LOAD     = files("sql.sql_load")