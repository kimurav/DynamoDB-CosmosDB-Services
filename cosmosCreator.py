import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constants
import azure.cosmos.documents as documents

import os
import json
import decimal

# Connect with the database using the environment vars
url = os.environ['ACCOUNT_URI']
key = os.environ['ACCOUNT_KEY']
client = cosmos_client.CosmosClient(url, {'masterKey': key})

database_name="movies"
try:
    database = client.CreateDatabase({'id': database_name})
except errors.HTTPFailure:
    database = client.ReadDatabase("dbs/" + database_name)


container_definition = {'id': 'movies',
                        'partitionKey':
                                    {
                                        'paths': ['/year'],
                                        'kind': documents.PartitionKind.Hash
                                    }
                        }
try:
    container = client.CreateContainer("dbs/" + database['id'], container_definition, {'offerThroughput': 400})
except errors.HTTPFailure as e:
    if e.status_code == http_constants.StatusCodes.CONFLICT:
        container = client.ReadContainer("dbs/" + database['id'] + "/colls/" + container_definition['id'])
    else:
        raise e

with open("moviedata.json") as json_file:
    movies = json.load(json_file)
    for movie in movies:
        print("Uploading movie: " + movie['title'])
        client.UpsertItem("dbs/"+database['id']+"/colls/"+container['id'], movie)