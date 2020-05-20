import boto3
import json
import decimal
import time

dynamodb = boto3.resource('dynamodb')
db_client = boto3.client('dynamodb')

resp = db_client.list_tables()
table_name = 'manav_movies'
table_exists = False
for name in resp['TableNames']:
    if name == table_name:
        print('table already exists skipping creation')
        table_exists = True

if table_exists == False:
    print('Creating table...')
    db_client.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'year',
                'KeyType': 'HASH'  #Partition key
            },
            {
                'AttributeName': 'title',
                'KeyType': 'RANGE'  #Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'year',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'title',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    time.sleep(10)

table = dynamodb.Table(table_name)
with open("moviedata.json") as json_file:
    movies = json.load(json_file, parse_float = decimal.Decimal)
    for movie in movies:
        year = int(movie['year'])
        title = movie['title']
        info = movie['info']

        print("Adding movie:", year, title)

        table.put_item(
           Item={
               'year': year,
               'title': title,
               'info': info,
            }
        )