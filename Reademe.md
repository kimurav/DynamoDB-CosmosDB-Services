# DynamoDB and CosmosDB Client Scripts

# DynamoDB
DynamoDB is a cloud NoSQL service provided by Amazon Web Services which is widly accepted by many companies. 
The `dynamoCreator.py` script will upload the `moviedata.json` file contents into a DynamoDB instance and the
`dynamoQuery.py` script provides a interface to build queries into the DynamoDB instance for movie data. Both scripts make use of the
`boto3` sdk provided by Amazon for python.

# CosmosDB
CosmosDB is a cloud database provided by Azure with millisecond response times and high availability. 
The `cosmosCreator.py` script is used to make  the cloud database and upload the `moviedata.json` contents.
The `cosmosQuery.py` script is used to interact with the created CosmosDB instance.

for sample results look at the .csv