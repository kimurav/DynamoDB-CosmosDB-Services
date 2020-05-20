import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constants
import azure.cosmos.documents as documents

import os
import json
import decimal
import csv
import pprint
""" 
Only allow the user to input partition key value either single or range.
Dont worry about filtering by attributes.
Allow an ability to sort by
Allow ability to Project certain values
"""

url = os.environ['ACCOUNT_URI']
key = os.environ['ACCOUNT_KEY']
client = cosmos_client.CosmosClient(url, {'masterKey': key})

database_name="movies"
container_name='movies'
database_loc = 'dbs/'+database_name+'/colls/'+container_name

def query_builder(key):
    if len(key) == 0: 
        return 
    key_list = key.split('-')
    if len(key_list) > 1:
        # range
        lower_bound = key_list[0]
        upper_bound = key_list[1]
        q = "SELECT * FROM " + container_name + " r WHERE r.year >= "+ lower_bound + " AND r.year <= " + upper_bound
        return q
    else:
        q = "SELECT * FROM " + container_name + " r WHERE r.year = " + key
        return q

def execute_query(q):
    try:
        items = client.QueryItems(database_loc, q,{'enableCrossPartitionQuery': True})
        return items
    except Exception as e:
        print("Failed to execute query: " + q + "\n" + e)


def parse_sort(sort_input):
    tokens = sort_input.split(" ")
    if len(tokens) != 2:
        print("Could not identify sort value")
        return
    attribute = tokens[0]
    asc_or_desc = tokens[1]

def get_attribute_filter():
    filter_out = input(">>> Enter a comma seperated list of attributes to show. eg: title,year or title,info or year,info: ")
    if len(filter_out) == 0:
        return -1
    return filter_out.split(',')

def main():
    #for item in client.QueryItems(database_loc, "SELECT * FROM " + container_name+" r WHERE r.info.rating > 6"):
        #print(json.dumps(item, indent=True))
    print(">>> Welcome to the CosmosDB client\nPlease follow the prompts to query CosmosDB")
    partition_key = input(">>> Enter the partition key to search for. Can be a single value or a range seperated by a \'-\'\n    eg:2003 or 2003-2010: ")
    sort_attribute = input(">>> Enter a attribute to sort on followed by \"DESC\" or \"ASC\" for Decending sort or Ascending sort. eg year DESC or title ASC: ")
    filter_attr = get_attribute_filter()
    res = execute_query(query_builder(partition_key))
    sorted_items = list(res)
    if len(sort_attribute) > 0:
        sort_q = sort_attribute.split(" ")
        desc = False
        if sort_q[1] == 'DESC':
            desc = True
        sorted_items = sorted(res, key = lambda i: i[sort_q[0]], reverse=desc)
    if filter_attr != -1:
        unwanted = set(['year', 'title', 'info','id','_rid', '_self', '_etag', '_attachments', '_ts']) - set(filter_attr)
        for i in sorted_items:
            for unwanted_key in unwanted:
                del i[unwanted_key]
    with open("cosmos_query_results.csv", 'w', newline='') as output_file:
        writer = csv.DictWriter(output_file, sorted_items[0].keys())
        writer.writeheader()
        for i in sorted_items:
            #writer.writerow({'title': i['title'], 'year': i['year'], 'info': i['info']})
            writer.writerow(dict(i))
    pprint.pprint(sorted_items)

main()