import numpy as np
import pandas as pd
import sqlalchemy as sql
import IPython.display as dp
import csv
import sys
sys.path.append('/Users/liubill/Documents/GitHub/python_scripts/database_scripts')
import database


pd.set_option('display.max_columns',100)
pd.set_option('display.max_rows',100)
pd.set_option('display.max_colwidth',-1)

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


data = pd.HDFStore('/Users/liubill/desktop/data_1.h5')

min_itemsize = {'chargeback_code': 96, 'vendor_code': 50, 'po': 50, 'ASIN': 50, 'Units': 100, 'vendor_amount': 100,
                'first_vendor_dispute_msg': 300, 'last_vendor_dispute_msg': 300,
                'isd_id': 50, 'po_code': 50, 'freight_terms': 50, 'infraction_subtype_code': 50,
                'carrier_name': 100, 'scac': 50, 'dock_door': 50, 'vamp_event_id': 100, 'window_start': 50,
                'window_end': 50, 'actual_date': 50,
                'actual_date_source': 50, 'inbound_ship_delivery_id': 50, 'ship_mode': 50,
                'dm_pro': 50, 'dm_isa_id': 50, 'carrier_req_del_date': 50}
for chunker in log_progress(iter(pd.read_csv('/Users/liubill/desktop/results.txt', sep='\t', chunksize=10000, dtype='str')),
                            every=1):
    chunker['first_vendor_dispute_msg'] = chunker['first_vendor_dispute_msg'].str.slice(
        0, 298)
    chunker['last_vendor_dispute_msg'] = chunker['last_vendor_dispute_msg'].str.slice(
        0, 298)
    data.append('df', chunker, data_columns=[
                'chargeback_code'], min_itemsize=min_itemsize)
