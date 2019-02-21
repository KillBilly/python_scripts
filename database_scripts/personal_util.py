import sqlalchemy as sql
import pandas as pd
from pympler import summary, muppy
import psutil
from sys import intern

# redshift connection string
"""
'postgresql://{username}:{pwd}\
@gsco-prod-dw.coicvnfbc4te.us-east-1.redshift.amazonaws.com:8192/gscoprod'.format(username='',pwd='')
"""


def log_progress(sequence, every=None, size=None, name='Items'):
    from ipywidgets import IntProgress, HTML, VBox
    from IPython.display import display

    is_iterator = False
    if size is None:
        try:
            size = len(sequence)
        except TypeError:
            is_iterator = True
    if size is not None:
        if every is None:
            if size <= 200:
                every = 1
            else:
                every = int(size / 200)     # every 0.5%
    else:
        assert every is not None, 'sequence is iterator, set every'

    if is_iterator:
        progress = IntProgress(min=0, max=1, value=1)
        progress.bar_style = 'info'
    else:
        progress = IntProgress(min=0, max=size, value=0)
    label = HTML()
    box = VBox(children=[label, progress])
    display(box)

    index = 0
    try:
        for index, record in enumerate(sequence, 1):
            if index == 1 or index % every == 0:
                if is_iterator:
                    label.value = '{name}: {index} / ?'.format(
                        name=name,
                        index=index
                    )
                else:
                    progress.value = index
                    label.value = u'{name}: {index} / {size}'.format(
                        name=name,
                        index=index,
                        size=size
                    )
            yield record
    except:
        progress.bar_style = 'danger'
        raise
    else:
        progress.bar_style = 'success'
        progress.value = index
        label.value = "{name}: {index}".format(
            name=name,
            index=str(index or '?')
        )


def get_virtual_memory_usage_kb():
    """
    The process's current virtual memory size in Kb, as a float.

    """
    return float(psutil.Process().memory_info_ex().vms) / 1024.0


def memory_usage(where):
    """
    Print out a basic summary of memory usage.

    """
    mem_summary = summary.summarize(muppy.get_objects())
    print("Memory summary:", where)
    summary.print_(mem_summary, limit=2)
    print("VM: %.2fMb" % (get_virtual_memory_usage_kb() / 1024.0))


class redshift_sql:

    def __init__(self, connection_string):
        self.engine = sql.create_engine(connection_string)
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
            .sort_values(by=['PK', 'distkey', 'sortkey'],
                         ascending=[True, True, False]).\
            reset_index()


class StringFolder(object):
    """
    Class that will fold strings. See 'fold_string'.
    This object may be safely deleted or go out of scope when
    strings have been folded.
    """

    def __init__(self):
        self.unicode_map = {}

    def fold_string(self, s):
        """
        Given a string (or unicode) parameter s, return a string object
        that has the same value as s (and may be s). For all objects
        with a given value, the same object will be returned. For unicode
        objects that can be coerced to a string with the same value, a
        string object will be returned.
        If s is not a string or unicode object, it is returned unchanged.
        :param s: a string or unicode object.
        :return: a string or unicode object.
        """
        # If s is not a string or unicode object, return it unchanged
        if not isinstance(s, str):
            return s

        # If s is already a string, then str() has no effect.
        # If s is Unicode, try and encode as a string and use intern.
        # If s is Unicode and can't be encoded as a string, this try
        # will raise a UnicodeEncodeError.
        try:
            return intern(str(s))
        except UnicodeEncodeError:
            # Fall through and handle s as Unicode
            pass

        # Look up the unicode value in the map and return
        # the object from the map. If there is no matching entry,
        # store this unicode object in the map and return it.
        return self.unicode_map.setdefault(s, s)


def string_folding_wrapper(results):
    """
    This generator yields rows from the results as tuples,
    with all string values folded.
    """
    # Get the list of keys so that we build tuples with all
    # the values in key order.
    keys = results.keys()
    folder = StringFolder()
    for row in results:
        yield tuple(
            folder.fold_string(row[key])
            for key in keys
        )
