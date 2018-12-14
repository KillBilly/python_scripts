import requests
from time import time
#import datetime
import json
import os
from pprint import pprint
# from file_manager import *
import datetime
import pandas as pd
import numpy as np

def post_request_completion(epoch_start, epoch_end, region, query):
    # ELASTIC SEARCH CLUSTERS
    count = {"na":{"vcbs":"http://vcbs-search-ess-na.iad.proxy.amazon.com/vcbs/_count",
                   "rsp":"http://vcbs-search-ess-na.iad.proxy.amazon.com/rsp/_count"},
            "eu":{
                "vcbs":'http://vcbs-ess-eu-dub.dub.proxy.amazon.com/vcbs/_count',
                "rsp":'http://vcbs-ess-eu-dub.dub.proxy.amazon.com/rsp/_count'
               }}
    search = {"na":{"vcbs":"http://vcbs-search-ess-na.iad.proxy.amazon.com/vcbs/_search",
                    "rsp":"http://vcbs-search-ess-na.iad.proxy.amazon.com/rsp/_search"
                    },
                 "eu":{"vcbs":'http://vcbs-ess-eu-dub.dub.proxy.amazon.com/vcbs/_search',
                       "rsp":"http://vcbs-ess-eu-dub.dub.proxy.amazon.com/rsp/_search"}}

    count_queries = {
    'vcbs':'{"query": {"query_string": {"query": "chargeback.dispute.status:(NOT WIP) AND cacheTimestamp:['+str(epoch_start)+' '+str(epoch_end)+']"}}}',
    'rsp':'{"query":{"query_string":{"query":"context.dispute_status:(NOT WIP) AND cacheTimestamp:['+str(epoch_start)+' '+str(epoch_end)+' ]"}}}'
        }

    size = requests.get(count[region][query], data=count_queries[query]).json()["count"]

    search_queries = {
    'vcbs':'{"size": '+str(size)+',"_source": [],"query": {"query_string": {"query": "chargeback.dispute.status:(NOT WIP) AND cacheTimestamp:['+str(epoch_start)+' '+str(epoch_end)+']"}}}',
    'rsp':'{"size": '+str(size)+', "_source": ["context.chargeback_code","context.approver", "context.hold", "context.comment*"], "query":{"query_string":{"query":"context.dispute_status:(NOT WIP) AND cacheTimestamp:['+str(epoch_start)+' '+str(epoch_end)+' ]"}}}'
    }

    start = time()
    response = requests.get(search[region][query], data=search_queries[query])
    end = time()

    print("TIME ELAPSED: "+str(datetime.timedelta(seconds=end-start)))

    response_dictionary = response.json()

    return response_dictionary

def get_completion(timestamp):
    """
    From the timestamp to today,
    this should return a resolved query for EU and NA
    """

    EPOCH_INIT = datetime.datetime(1970, 1, 1)

    start_date = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
    end_date = datetime.datetime.utcnow()

    epoch_start = int((start_date - EPOCH_INIT).total_seconds())*1000
    epoch_end = int((end_date - EPOCH_INIT).total_seconds())*1000

    print(start_date)

    na = post_request_completion(epoch_start, epoch_end, 'na', 'rsp')
    eu = post_request_completion(epoch_start, epoch_end, 'eu', 'rsp')

    for hit in na['hits']['hits']:
        eu['hits']['hits'].append(hit)

    # Note that EU has the info from NA too.
    return eu

def generate_window(DAYS_SLA):
    """
    Generates window for downloading only disputes that are within the 90 days of SLA.

    """
    EPOCH_INIT = datetime.datetime(1970, 1, 1)

    start_date = datetime.datetime.utcnow() + datetime.timedelta(DAYS_SLA)
    end_date = datetime.datetime.utcnow()

    epoch_start = (start_date - EPOCH_INIT).total_seconds()
    epoch_end = (end_date - EPOCH_INIT).total_seconds()

    return [int(epoch_start)*1000, int(epoch_end)*1000]

def post_request(region, query):
    """
    This will make the post request and
    write to this the response, with the name
    of the query used.
    """

    dates = generate_window(-1)

    # ELASTIC SEARCH CLUSTERS
    count = {"na":{"vcbs":"http://vcbs-search-ess-na.iad.proxy.amazon.com/vcbs/_count",
                   "rsp":"http://vcbs-search-ess-na.iad.proxy.amazon.com/rsp/_count"},
            "eu":{
                "vcbs":'http://vcbs-ess-eu-dub.dub.proxy.amazon.com/vcbs/_count',
                "rsp":'http://vcbs-ess-eu-dub.dub.proxy.amazon.com/rsp/_count'
                }}
    search = {"na":{"vcbs":"http://vcbs-search-ess-na.iad.proxy.amazon.com/vcbs/_search",
                    "rsp":"http://vcbs-search-ess-na.iad.proxy.amazon.com/rsp/_search"
                    },
                 "eu":{"vcbs":'http://vcbs-ess-eu-dub.dub.proxy.amazon.com/vcbs/_search',
                       "rsp":"http://vcbs-ess-eu-dub.dub.proxy.amazon.com/rsp/_search"}}

    count_queries = {
    'vcbs':'{"query": {"query_string": {"query": "chargeback.dispute.status:(WIP) AND chargeback.dispute.last_updated_date:['+str(dates[0])+' '+str(dates[1])+']"}}}',
    'rsp':'{"query":{"query_string":{"query":"context.dispute_status:(WIP) AND context.dispute_last_updated_date:['+str(dates[0])+' '+str(dates[1])+' ]"}}}'
        }

    size = requests.get(count[region][query], data=count_queries[query]).json()["count"]


    search_queries = {
    'vcbs':'{"size": '+str(size)+',"_source": [],"query": {"query_string": {"query": "chargeback.dispute.status:(WIP) AND chargeback.dispute.last_updated_date:['+str(dates[0])+' '+str(dates[1])+']"}}}',
    'rsp':'{"size": '+str(size)+', "_source": ["context.chargeback_code","context.approver", "context.hold", "context.comment*", "context.has_evidence"], "query":{"query_string":{"query":"context.dispute_status:(WIP) AND context.dispute_last_updated_date:['+str(dates[0])+' '+str(dates[1])+' ]"}}}'
    }


    print("STARTING DOWNLOAD, MONITOR VIA TASK PANEL")

    start = time()
    response = requests.get(search[region][query], data=search_queries[query])
    end = time()

    print("TIME ELAPSED: "+str(datetime.timedelta(seconds=end-start)))

    response_dictionary = response.json()

    return response_dictionary


def download_query():
    """

    Test method to get response from ElasticSearch cluster by POST request.
    Searching specific assigned chargeback

    Test by now downloads all info

    """

    region = 'na'

    # vcbs = post_request(region, "vcbs")
    rsp = post_request(region, "rsp")

    evidence_dict = {}
    is_hold_dict = {}
    comment_dict = {}

    print("TEST BEGIN")

    for each_hit in rsp["hits"]["hits"]:
        evidence_dict[each_hit["_source"]["context"]["chargeback_code"]] = each_hit["_source"]["context"]["has_evidence"]
        # is_hold_dict[each_hit["_source"]["context"]["chargeback_code"]] = each_hit["_source"]["context"]["hold"]
        # comment_dict[each_hit["_source"]["context"]["chargeback_code"]] = []
        # for each_comment_key in each_hit["_source"]["context"]:
        #     if 'comment' in each_comment_key:
        #         #print each_hit["_source"]["context"][each_comment_key]
        #         comment_dict[each_hit["_source"]["context"]["chargeback_code"]].append(each_hit["_source"]["context"][each_comment_key])

    # for each_hit in vcbs["hits"]["hits"]:
    #     # EVIDENCE
    #     try:
    #         each_hit["_source"]["attributes"]["has_evidence"] = evidence_dict[each_hit["_source"]["chargeback"]["chargeback_code"]]
    #     except:
    #         each_hit["_source"]["attributes"]["has_evidence"] = "?"
    #
    #     # HOLD
    #     try:
    #         each_hit["_source"]["attributes"]["hold"] = is_hold_dict[each_hit["_source"]["chargeback"]["chargeback_code"]]
    #     except:
    #         each_hit["_source"]["attributes"]["hold"] = "?"
    #
    #     # HOLD COMMENT
    #     try:
    #         each_hit["_source"]["attributes"]["comment_log"] = comment_dict[each_hit["_source"]["chargeback"]["chargeback_code"]]
    #     except:
    #         each_hit["_source"]["attributes"]["comment_log"] = "?"


    # APPEND ALL DATA BASE
    # all_database = vcbs

    # region = 'eu'
    #
    # vcbs = post_request(region, "vcbs")
    # rsp = post_request(region, "rsp")
    #
    # evidence_dict = {}
    # is_hold_dict = {}
    # comment_dict = {}
    #
    # #print "TEST BEGIN"
    #
    # for each_hit in rsp["hits"]["hits"]:
    #     evidence_dict[each_hit["_source"]["context"]["chargeback_code"]] = each_hit["_source"]["context"]["has_evidence"]
    #     is_hold_dict[each_hit["_source"]["context"]["chargeback_code"]] = each_hit["_source"]["context"]["hold"]
    #     comment_dict[each_hit["_source"]["context"]["chargeback_code"]] = []
    #     for each_comment_key in each_hit["_source"]["context"]:
    #         if 'comment' in each_comment_key:
    #             #print each_hit["_source"]["context"][each_comment_key]
    #             comment_dict[each_hit["_source"]["context"]["chargeback_code"]].append(each_hit["_source"]["context"][each_comment_key])
    #
    #     #print comment_dict
    #     #print is_hold_dict
    #
    # for each_hit in vcbs["hits"]["hits"]:
    #     # EVIDENCE
    #     try:
    #         each_hit["_source"]["attributes"]["has_evidence"] = evidence_dict[each_hit["_source"]["chargeback"]["chargeback_code"]]
    #     except:
    #         each_hit["_source"]["attributes"]["has_evidence"] = "?"
    #
    #     # HOLD
    #     try:
    #         each_hit["_source"]["attributes"]["hold"] = is_hold_dict[each_hit["_source"]["chargeback"]["chargeback_code"]]
    #     except:
    #         each_hit["_source"]["attributes"]["hold"] = "?"
    #
    #     # HOLD COMMENT
    #     try:
    #         each_hit["_source"]["attributes"]["comment_log"] = comment_dict[each_hit["_source"]["chargeback"]["chargeback_code"]]
    #     except:
    #         each_hit["_source"]["attributes"]["comment_log"] = "?"

    # MERGIN ALL DATABASE

    # print("Mergin REGION info...")
    #
    #
    # for each_hit in vcbs["hits"]["hits"]:
    #     all_database["hits"]["hits"].append(each_hit)


    # print("Writing response to disk")
    #
    # with open('database-'+str(datetime.date.today())+'.txt' , 'w') as outfile:
    #     json.dump(all_database, outfile, indent=4)

# def automatic_download():
#     """
#     Pretty much as downloadquery, except, that it creates the folders
#     and navigates automatically.
#     """
#
#     pylon_settings = read_pylon_settings()
#
#     os.chdir(pylon_settings['CONFIG_FOLDER'])
#     download_query()

if __name__ ==  "__main__":
    download_query()
