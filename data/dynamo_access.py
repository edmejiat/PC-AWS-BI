import boto3

class DynamoAccess:
    def __init__(self):
        self._dynamodb = boto3.resource('dynamodb')
        
    def set_table(self, name_table):
        self._table = self._dynamodb.Table(name_table)
        
    def get_item(self, column_name, column_value):
        response = self._table.get_item(
            Key={
                column_name: column_value
            }
        )
        item = response['Item']
        
        return item
        
    def get_report(self):
        return
    
    def get_all_items(self):
        response = self._table.scan()
        return response['Items']