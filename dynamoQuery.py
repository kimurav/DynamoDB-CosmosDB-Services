import boto3
import pprint
import csv
from boto3.dynamodb.conditions import Key, Attr



db = boto3.resource('dynamodb')
table = db.Table('manav_movies')
pp = pprint.PrettyPrinter(indent=4)

def generateAttr(filter_val):
    if len(filter_val) > 1:
        to_search = ''
        for i in range(2, len(filter_val)):
                if i == 2:
                    to_search += filter_val[i]
                else:
                    to_search += ' ' + filter_val[i]
        attr_filter = Attr(filter_val[0])
        if filter_val[1] == 'eq':
            attr_filter = attr_filter.eq(to_search)
        elif filter_val[1] == 'contains':
            attr_filter = attr_filter.contains(to_search)
        elif filter_val[1] == 'lt':
            attr_filter = attr_filter.lt(int(filter_val[2]))
        elif filter_val[1] == 'gt':
            attr_filter =  attr_filter.gt(int(filter_val[2]))
        return attr_filter
    else:
        return -1
#return a comma formatted string of attributes 
def genProjAttr(attr):
    attr =  attr.split(',')
    if attr[0] == '':
        return '#yr, title, info'
    elif len(attr) > 0:
        return '#yr, ' + ', '.join(attr)

def output_to_csv(items):
    with open("dynamo_query_results.csv", 'w', newline='') as output_file:
            writer = csv.DictWriter(output_file, items[0].keys())
            writer.writeheader()
            for i in items:
                writer.writerow(dict(i))
    

#TODO adding the ability to sort and filter attributes copy the azure example
def main():
    key_to_search = input(">>> Please enter a year or a range of year seperated by '-'\n\teg(2012 or 2010-2012): ")
    title_to_search = ''
    filter_value = ''
    if len(key_to_search) == 0:
        title_to_search = input(">>> Please enter a title to search for or press enter and leave blank: ")
    key_to_search = key_to_search.split('-')
    if len(title_to_search) == 0:
        filter_value = input(">>> Please enter a filter\n\teg(info.rating gt 5 or info.genres contains Drama): ")
    proj_attr = input(">>> Please enter a comma seperated list of attributes to view not including the year: eg(title, info.rating)")
    result_set=[]
    if len(filter_value) > 0:
        filter_value = filter_value.split(' ')
    resp = {}
    # Case when the user enters one year 
    if len(key_to_search) == 1 and key_to_search[0] != '':
        key_expr = Key('year').eq(int(key_to_search[0]))
        filt = generateAttr(filter_value)
        pe = genProjAttr(proj_attr)
        if filt == -1:
            resp = table.query(
                KeyConditionExpression=key_expr,
                ProjectionExpression=pe,
                ExpressionAttributeNames={'#yr': 'year'}
            )
            result_set += resp['Items']
            if 'LastEvaluatedKey' in resp:
                while 'LastEvaluatedKey' in resp:
                    start_key=resp['LastEvaluatedKey']
                    resp = table.query(
                        KeyConditionExpression=key_expr,
                        ProjectionExpression=pe,
                        ExpressionAttributeNames={'#yr': 'year'},
                        ExclusiveStartKey=start_key
                    )
                    result_set += resp['Items']
        else:
            resp = table.scan(
                FilterExpression=filt & Attr('year').eq(int(key_to_search[0])),
                ProjectionExpression=pe,
                ExpressionAttributeNames={'#yr': "year"}
            )
            result_set += resp['Items']
            if 'LastEvaluatedKey' in resp:
                while 'LastEvaluatedKey' in resp:
                    start_key=resp['LastEvaluatedKey']
                    resp = table.scan(
                        FilterExpression=filt & Attr('year').eq(int(key_to_search[0])),
                        ProjectionExpression=pe,
                        ExpressionAttributeNames={'#yr': "year"},
                        ExclusiveStartKey=start_key
                    )
                    result_set += resp['Items']
       
    #Case when the user doesnt enter a year but a title
    elif key_to_search[0] == '' and len(title_to_search) != 0:
        pe = genProjAttr(proj_attr)
        resp = table.scan(
            FilterExpression=Attr('title').eq(title_to_search),
            ProjectionExpression=pe,
            ExpressionAttributeNames={'#yr': 'year'}
        )
        result_set = resp['Items']
        if "LastEvaluatedKey" in resp and resp['Items'][0]['title'] != title_to_search:
            while "LastEvaluatedKey" in resp:
                start_key = resp['LastEvaluatedKey']
                resp = table.scan(
                    FilterExpression=Attr('title').eq(title_to_search), 
                    ExclusiveStartKey=start_key,
                    ProjectionExpression=pe,
                    ExpressionAttributeNames={'#yr': 'year'}
                    )
                result_set += resp["Items"]
        
    elif len(key_to_search) == 2:
        
        key_expr = Key('year').between(int(key_to_search[0]), int(key_to_search[1]))
        pe = genProjAttr(proj_attr)
        resp = table.scan(
            FilterExpression=key_expr,
            ProjectionExpression=pe,
            ExpressionAttributeNames={'#yr': "year"}
        )
        result_set += resp['Items']
        if "LastEvaluatedKey" in resp:
            while "LastEvaluatedKey" in resp:
                start_key = resp['LastEvaluatedKey']
                resp = table.scan(FilterExpression=key_expr, ProjectionExpression=pe, ExclusiveStartKey=start_key, ExpressionAttributeNames={'#yr': 'year'})
                result_set += resp['Items']



    sort_on = input(">>> Please enter an attribute to sort on followed by ASC or DESC for ascendin and decending sort.\neg(title ASC, year DESC): ")
    if len(sort_on) == 0:
       output_to_csv(result_set)
       pp.pprint(result_set)
    else:
        sort_q = sort_on.split(" ")
        desc = False
        if sort_q[1] == 'DESC':
            desc = True
        sorted_items = sorted(result_set, key = lambda i: i[sort_q[0]], reverse=desc)
        output_to_csv(sorted_items)
        pp.pprint(sorted_items)

        


main()