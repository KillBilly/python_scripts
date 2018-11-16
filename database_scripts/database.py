import sqlalchemy as sql
import pandas as pd
import numpy as np


class redshift_sql:

    def __init__(self, connection_string):
        self.engine = sql.create_engine(connection_string)
        self.connection = self.engine.connect()
        self.inspect = sql.inspect(self.engine)

    """
    get schema names:
    inspect.get_schema_names()

    get table names in a schema:
    inspect.get_table_names(schema='schema_name')

    get columns in a table:
    df_lst = get_columns(table_name, schema=None, **kw)
    pd.DataFrame(df_lst)
    """

    def get_table_attributes(self, schema, table):
        sql_code = "set search_path to " + schema + ";" + \
            "select \"column\", distkey, sortkey, type, \"notnull\" from \
            pg_table_def where tablename = '" + table + "';"
        df = pd.read_sql_query(sql_code, self.engine)
        pk = pd.DataFrame({'PK': self.inspect.get_pk_constraint(
            table, schema)['constrained_columns']})

        return df.merge(pk, how='left', left_on='column', right_on='PK')\
            .sort_values(by=['PK', 'distkey', 'sortkey']).\
            reset_index(inplace=True)

    def read_sql(self, sql_file_path):
        sql_file = open(sql_file_path, 'r')
        sql_file.seek(0) # Go back to the starting position
        sql_code = sql_file.read()
        sql_file.close()
        return pd.read_sql_query(sql.text(sql_code), self.engine)

    def run_sql(self, sql_file_path):
        sql_file = open(sql_file_path, 'r')
        sql_file.seek(0) # Go back to the starting position
        sql_code = sql_file.read()
        sql_file.close()
        self.connection.execute(sql.text(sql_code).
                                execution_options(autommit=True))
