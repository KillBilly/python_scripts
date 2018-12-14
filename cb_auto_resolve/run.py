#!flask/bin/python
from flask import Flask, render_template, request, send_file
import pandas as pd
import numpy as np
import os
import sys
import requests
import sqlalchemy
import datetime
from time import time
import json

from selenium import webdriver
import getpass

# pyinstaller MAC code- run in cmd to generate package add -F for ONE FILE
# activate virenv first
# source activate liubill1
# pyinstaller --add-data './templates/*.html:./templates' --add-data './static/css/*.css:./static/css' --add-data './src/chromedriver:./src' --add-data './download/results.csv:./download' -F run.py

# pyinstaller Windows code- run in cmd to generate package add -F for ONE FILE
# activate virenv first
# conda activate liubill1
# pyinstaller --add-data ".\templates\*.html;.\templates" --add-data ".\static\css\*.css;.\static\css" --add-data ".\src\chromedriver.exe;.\src" --add-data ".\download\results.csv;.\download" -F run.py


# code version of generating ONE FOLDER package in pyinstaller
# if getattr(sys, 'frozen', False):
#     root_dir = os.path.dirname(os.path.join(sys.executable))
#     template_folder = os.path.join(root_dir, 'templates')
#     static_folder = os.path.join(root_dir, 'static')
#     app = Flask(__name__, template_folder=template_folder,
#                 static_folder=static_folder)

# code version of generating ONE FILE package in pyinstaller
if getattr(sys, 'frozen', False):
    root_dir = os.path.join(sys._MEIPASS)
    template_folder = os.path.join(root_dir, 'templates')
    static_folder = os.path.join(root_dir, 'static')
    app = Flask(__name__, template_folder=template_folder,
                static_folder=static_folder)

# for normal unpackaged usage
else:
    root_dir = os.path.dirname(os.path.abspath(__file__))
    app = Flask(__name__)

pd.set_option('display.max_colwidth', -1)

# global variables
username = ''
pwd = ''
input = ''
engine = ''
df1 = ''
df2 = ''
link_code = ''


username = getpass.getuser()

if os.name == 'posix':
    chrome_executable_path = os.path.join(root_dir, 'src', 'chromedriver')
    chrome_profile_path = os.path.join('user-data-dir=', 'Users', username,
                                       'Library', 'Application Support',
                                       'Google', 'Chrome', 'Default')

elif os.name == 'nt':
    chrome_executable_path = os.path.join(root_dir, 'src', 'chromedriver.exe')
    chrome_profile_path = os.path.join('user-data-dir=C:', 'Users',
                                       username, 'AppData', 'Local',
                                       'Google', 'Chrome', 'Userdata',
                                       'Default')

options = webdriver.ChromeOptions()
options.add_argument(chrome_profile_path)  # Path to your chrome profile
options.add_argument('disable-infobars')  # disable inforbar
w = ''
url = "https://vcbs.amazon.com/vcbs/search/chargebacks"


# ------------- flask app section ---------------------------------------------
def post_request(region, query, x_code):
    """
    This will make the post request and
    write to this the response, with the name
    of the query used.
    """

    # ELASTIC SEARCH CLUSTERS
    search = {"na":
              {"vcbs":
               "http://vcbs-search-ess-na.iad.proxy.amazon.com/vcbs/_search",
               "rsp":
               "http://vcbs-search-ess-na.iad.proxy.amazon.com/rsp/_search"
               },
              "eu":
              {"vcbs":
               'http://vcbs-ess-eu-dub.dub.proxy.amazon.com/vcbs/_search',
               "rsp":
               "http://vcbs-ess-eu-dub.dub.proxy.amazon.com/rsp/_search"}}

    search_queries = {
        'rsp': '{"_source": \
        ["context.chargeback_code","context.has_evidence"],\
        "query":{"query_string":\
        {"query":"chargeback_code:' + "(" + ",".join(x_code) + ')"' + "}}}"
    }

    print('START Elastic Search for evidence tag')
    start = time()
    response = requests.get(search[region][query], data=search_queries[query])
    end = time()
    print("TIME ELAPSED: " + str(datetime.timedelta(seconds=end - start)))

    response_dictionary = response.json()

    return response_dictionary


def resolve(cb_string):

    try:
        engine.connect()
    except Exception as e:
        return ['Bad login', 'Bad login', 'Bad login']

    cb_code = [x for x in cb_string.strip().split('\n') if len(x) > 0]

    for i in range(len(cb_code)):
        cb_code[i] = "'" + cb_code[i] + "'"

    # Input of Chargeback Codes
    df_input = pd.DataFrame({"seq": range(len(cb_code)),
                             "chargeback_code": cb_code})
    df_input['chargeback_code'] = df_input['chargeback_code'].str.strip("'")

    # retrieve cb information records from redshift
    sql_query = 'select * from volt_ddl.d_cb_dispute_data \
    where chargeback_code in (' + ','.join(cb_code) + ')'

    raw_df = pd.read_sql_query(sql_query, engine)

    raw_df['checker'] = raw_df['chargeback_code']
    f_df = df_input.merge(raw_df, how='left',
                          on='chargeback_code')

    # elastic search to get has_evidence
    region = 'na'
    rsp = post_request(region, "rsp", df_input['chargeback_code'].tolist())
    evidence_dict = {}

    for each_hit in rsp["hits"]["hits"]:
        evidence_dict[each_hit["_source"]["context"]["chargeback_code"]] = \
            each_hit["_source"]["context"]["has_evidence"]

    df_evi = pd.Series(evidence_dict).reset_index()
    df_evi.columns = ['chargeback_code', 'has_evidence']

    print(df_evi)

    f_df = f_df.merge(df_evi, how='left',
                      on='chargeback_code').sort_values(by='seq')

    def has_evidence_tag(row):
        if pd.isnull(row['has_evidence']):
            return 'unknown'
        elif row['has_evidence'].lower() == 'true':
            return 'y'
        elif row['has_evidence'].lower() == 'false':
            return 'n'
        else:
            return 'unknown'

    f_df['has_evidence'] = f_df.apply(has_evidence_tag, axis=1)

    # add evidence link
    # here in lambda, x is a row. Using axis=1 to prevent errors
    f_df['evidence_link'] = f_df.\
        apply(lambda x:
              '<button id="ev_button" name="{cbc}" \
              onClick="doWork(this.name)">link</button>'.
              format(cbc=x['chargeback_code']), axis=1)

    f_df['evidence_link_excel'] = f_df['has_evidence'].\
        apply(lambda x: '=HYPERLINK("{link}","vcbs site")'.format(link=url)
              if x in ['y', 'unknown'] else np.nan)

    # formatting
    f_df['quantity'] = f_df['quantity'].astype(float)
    f_df['vendor_amount'] = f_df['vendor_amount'].astype(float)

    # Logic Processing
    f_df['Dispute Suggestion'] = ''
    f_df['Dispute Blurb'] = ''
    col_len = len(f_df.columns)

    for i in range(len(f_df)):
        if pd.isnull(f_df['checker'][i]):
            f_df.iloc[i, col_len - 2] = 'Cannot find Chargeback Record'

        elif pd.isnull(f_df['dispute_status'][i]):
            f_df.iloc[i, col_len - 2] = 'Cannot find Dispute Record'

        elif f_df['dispute_status'][i] not in \
                ['WIP', 'PENDING', 'REQUEST_MORE_INFO']:
            f_df.iloc[i, col_len - 2] = 'No Suggestion because decision \
                had been made: ' + \
                f_df['dispute_status'][i] + ''
            f_df.iloc[i, col_len - 1] = f_df['vm_resolution_code'][i]

        elif not pd.isnull(f_df['dm_pro'][i])\
                and 'csx' in str(f_df['dm_pro'][i]).lower():

            f_df.iloc[i, col_len - 2] = 'Approve Dispute'
            f_df.iloc[i, col_len - 1] = "Amazon Internal Error. POACNA94 \
            Dockmaster's entry appointment was not recorded \
            appropriately showing Csx label as PRO"

        elif (not pd.isnull(f_df['carrier_name'][i]) and 'no carrier scac list'
              in str(f_df['carrier_name'][i]).lower()) or \
            (not pd.isnull(f_df['scac'][i])
             and str(f_df['scac'][i]).lower() == 'ncsl') or \
                (not pd.isnull(f_df['dock_door'][i])
                 and str(f_df['dock_door'][i]).lower() == '9999'):

            f_df.iloc[i, col_len - 2] = 'Approve Dispute'
            f_df.iloc[i, col_len - 1] = "Amazon Internal Error. POACNA95 \
            Dockmaster's entry appointment was not recorded \
            appropriately showing NCSL and Dock Door = 9999"

        elif not pd.isnull(f_df['vamp_event_id'][i]):
            if not pd.isnull(f_df['actual_date'][i]) and \
                    not pd.isnull(f_df['window_end'][i]) and \
                    f_df['actual_date'][i] <= f_df['window_end'][i]:
                f_df.iloc[i, col_len - 2] = 'Approve Dispute'
                f_df.iloc[i, col_len - 1] = "Amazon Internal Error. POACNA11 \
                The Carrier requested delivery date (CRDD) chosen \
                during the CARP appointment, \
                falls within the window assigned to the Vendor"

            elif f_df['has_evidence'][i] in ['y', 'unknown']:
                f_df.iloc[i, col_len - 2] = 'Review Evidence'

            else:
                f_df.iloc[i, col_len - 2] = 'Deny Dispute'

        elif not pd.isnull(f_df['carrier_req_del_date'][i]) and \
                not pd.isnull(f_df['window_end'][i]) and \
                f_df['carrier_req_del_date'][i] <= f_df['window_end'][i]:
            f_df.iloc[i, col_len - 2] = 'Approve Dispute'
            f_df.iloc[i, col_len - 1] = "Amazon Internal Error.POACNA12 \
                 The Carrier requested delivery date (CRDD) chosen, \
                falls within the window assigned to the Vendor"

        elif f_df['has_evidence'][i] in ['y', 'unknown']:
            f_df.iloc[i, col_len - 2] = 'Review Evidence'

        else:
            f_df.iloc[i, col_len - 2] = 'Deny Dispute'

    results = f_df[['chargeback_code', 'Dispute Suggestion',
                    'Dispute Blurb', 'has_evidence', 'evidence_link',
                    'quantity', 'vendor_amount', 'first_vendor_dispute_msg',
                    'last_vendor_dispute_msg',
                    'isd_id', 'dm_isa_id', 'ship_mode',
                    'vendor_code', 'po', 'freight_terms',
                    'infraction_subtype_code', 'dm_pro',
                    'carrier_name', 'scac', 'dock_door', 'vamp_event_id',
                    'window_start', 'window_end', 'actual_date',
                    'actual_date_source', 'carrier_req_del_date']]

    results_excel = f_df[['chargeback_code', 'Dispute Suggestion',
                          'Dispute Blurb', 'has_evidence',
                          'evidence_link_excel',
                          'quantity', 'vendor_amount',
                          'first_vendor_dispute_msg',
                          'last_vendor_dispute_msg',
                          'isd_id', 'dm_isa_id', 'ship_mode',
                          'vendor_code', 'po', 'freight_terms',
                          'infraction_subtype_code', 'dm_pro',
                          'carrier_name', 'scac', 'dock_door', 'vamp_event_id',
                          'window_start', 'window_end', 'actual_date',
                          'actual_date_source', 'carrier_req_del_date']]

    # change column names
    results.columns = ['chargeback_code', 'dispute_suggestion',
                       'dispute_blurb', 'has_evidence', 'evidence_link',
                       'quantity',
                       'vendor_amount', 'first_vendor_dispute_msg',
                       'last_vendor_dispute_msg',
                       'isd_id', 'dm_isa_id', 'ship_mode',
                       'vendor_code', 'po', 'freight_terms',
                       'infraction_subtype_code', 'dm_pro',
                       'carrier_name', 'scac', 'dock_door',
                       'vamp_event_id',
                       'window_start', 'window_end', 'actual_date',
                       'actual_date_source', 'carrier_req_del_date']
    # change column names
    results_excel.columns = ['chargeback_code', 'dispute_suggestion',
                             'dispute_blurb', 'has_evidence', 'evidence_link',
                             'quantity',
                             'vendor_amount', 'first_vendor_dispute_msg',
                             'last_vendor_dispute_msg',
                             'isd_id', 'dm_isa_id', 'ship_mode',
                             'vendor_code', 'po', 'freight_terms',
                             'infraction_subtype_code', 'dm_pro',
                             'carrier_name', 'scac', 'dock_door',
                             'vamp_event_id',
                             'window_start', 'window_end', 'actual_date',
                             'actual_date_source', 'carrier_req_del_date']
    # Output
    return [results, results_excel, 'Succeed']


def get_status():
    global w, link_code
    if w == '':
        w = webdriver.Chrome(executable_path=chrome_executable_path,
                             options=options)
    try:
        w.current_url == "https://vcbs.amazon.com/vcbs/search/chargebacks"
    except Exception as e:
        w.quit()  # when encounter exception, must quit() first then re-launch
        # or you get an NoSuchWindow/WindowAlreadyClosed error!
        w = webdriver.Chrome(executable_path=chrome_executable_path,
                             options=options)

    if w.current_url != "https://vcbs.amazon.com/vcbs/search/chargebacks":
            w.get(url)
    else:
        pass

    try:
        enter_field = w.find_element_by_name("identifierCodes")
        enter_field.clear()
        enter_field.send_keys(link_code)

        search_button = w.find_element_by_xpath("//input[@type='submit']")
        search_button.click()

        supporting_bar = w.find_element_by_xpath(
            "//div[contains(text(), 'Supporting Documentation')]")
        supporting_bar.click()

    except Exception as e:
        w.quit()  # when encounter exception, must quit() first then re-launch
        # or you get an NoSuchWindow/WindowAlreadyClosed error!
        w = ''


@app.route('/')
def login():
    return render_template('login.html')


@app.route('/search')
def index():
    return render_template('search.html')


@app.route('/tables')
def tables():
    return render_template('tables.html',
                           data_table=df1.to_html(classes='t1',
                                                  index=False,
                                                  justify='center',
                                                  na_rep='N/A',
                                                  escape=False,
                                                  table_id='table',
                                                  float_format=lambda x:
                                                  '%10.2f' % x))


@app.route('/receiver', methods=['POST'])
def worker():
    global input, df1, df2
    input = request.form['data']
    results = resolve(input)
    df1 = results[0]
    df2 = results[1]
    if results[2] == 'Bad login':
        return json.dumps({'success': False}), 404, \
            {'ContentType': 'application/json'}
    df2.to_csv(os.path.join(root_dir, 'download', 'results.csv'), index=False)
    return json.dumps({'success': True}), 200, \
        {'ContentType': 'application/json'}


@app.route('/login', methods=['POST'])
def logger():
    global username
    global pwd
    global engine
    username = request.form['username']
    pwd = request.form['password']
    engine = sqlalchemy.\
        create_engine('postgresql://'
                      + username + ":" + pwd
                      + '@gsco-prod-dw.coicvnfbc4te.us-east-1.'
                      + 'redshift.amazonaws.com:8192/gscoprod')
    try:
        engine.connect()
        return json.dumps({'success': True}), 200, \
            {'ContentType': 'application/json'}
    except Exception as e:
        return json.dumps({'success': False}), 404, \
            {'ContentType': 'application/json'}


@app.route('/evidencelink', methods=['POST'])
def go_to_link():
    global w, link_code
    link_code = request.form['link_code']

    get_status()

    return json.dumps({'success': True}), \
        200, {'ContentType': 'application/json'}


@app.route("/download")
def get_csv():
    return send_file(
        os.path.join(root_dir, 'download', 'results.csv'),
        mimetype="text/csv",
        attachment_filename='results.csv',
        as_attachment=True)


# ----------- main function ---------------------------------------------------
if __name__ == '__main__':
    app.run()
