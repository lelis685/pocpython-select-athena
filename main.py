import json
import time
import boto3

client = boto3.client('athena')

def lambda_handler(event, context):
    
    print('START LAMBDA')
    
    print('QUERYING FROM "water_data_database"."lelis_athena"')
    
    queryStart = client.start_query_execution(
    QueryString = 'SELECT "reservoir_name" FROM "water_data_database"."lelis_athena" limit 10',
    QueryExecutionContext = {'Database': 'water_data_database'}, 
    ResultConfiguration = { 'OutputLocation': 's3://lelis-result-athena/result-lambda'},
    WorkGroup = 'primary'
    )
    
    queryExecutionId = queryStart['QueryExecutionId']
    
    print(f'WAITING FOR RESULT TO BE AVAILABLE QueryExecutionId={queryExecutionId}')
    time.sleep(4)
    
    queryExecResp = client.get_query_execution(QueryExecutionId=queryExecutionId)
    
    print(f'STATUS AND STATISTICS QueryExecutionId={queryExecutionId}')
    print (json.dumps(queryExecResp['QueryExecution']['Status'], indent=2, default=str))
    print (json.dumps(queryExecResp['QueryExecution']['Statistics'], indent=2, default=str))

    results = client.get_query_results(QueryExecutionId=queryStart['QueryExecutionId'])
    
    print(f'RESULT OF QueryExecutionId={queryExecutionId}')
    
    
    for row in results['ResultSet']['Rows']:
        print(row)
