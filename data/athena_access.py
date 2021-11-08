import boto3
import re
import time
from entity.params import Params

class AthenaAccess:
    def __init__(self):
        self.params = Params()
        self.session = boto3.Session()
        self.client = self.session.client('athena', region_name=self.params.get_param("region"))
        
    def athena_query(self, client, sqlQuery):
        response = client.start_query_execution(
            QueryString=sqlQuery,
            QueryExecutionContext={
                'Database': self.params.get_param('database')
            },
            ResultConfiguration={
                'OutputLocation': 's3://' + self.params.get_param('bucket') + '/' + self.params.get_param('path')
            }
        )
        return response
        
    def athena_to_s3(self, sqlQuery,  max_execution = 5):
        execution = self.athena_query(self.client, sqlQuery)
        
        execution_id = execution['QueryExecutionId']
        state = 'RUNNING'
        while (max_execution > 0 and state in ['RUNNING', 'QUEUED']):
            max_execution = max_execution - 1
            response = self.client.get_query_execution(QueryExecutionId = execution_id)
            if 'QueryExecution' in response and \
                    'Status' in response['QueryExecution'] and \
                    'State' in response['QueryExecution']['Status']:
                state = response['QueryExecution']['Status']['State']
                if state == 'FAILED':
                    return False
                elif state == 'SUCCEEDED':
                    s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                    filename = re.findall('.*\/(.*)', s3_path)[0]
                    return filename
            #print('execution id: ' + str(execution_id) +' state:' + state)
            time.sleep(1)
        return False