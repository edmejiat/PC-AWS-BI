import boto3
import pandas as pd
import io
from entity.params import Params2

class S3Access:
    def __init__(self):
        self.session = boto3.Session()
        self.s3client = boto3.client('s3')
        self.params = Params2() 
        
    def s3_to_pandas(self, s3_filename):    
        #s3client = self.session.client('s3')
        obj = self.s3client.get_object(Bucket=self.params.get_param('bucket'),
                                  Key=self.params.get_param('path') + '/' + s3_filename)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        return df
    
    # Deletes all files in your path so use carefully!
    def cleanup(self, params):
        s3 = self.session.resource('s3')
        my_bucket = s3.Bucketself.params.get_param('bucket')
        for item in my_bucket.objects.filter(Prefix=self.params.get_param('path')):
            item.delete()
            
    def readS3File(self, filename):
        print(self.params.get_param('bucket') + '/' + self.params.get_param('path') + filename)
        result = self.s3client.get_object(Bucket=self.params.get_param('bucket'), Key=self.params.get_param('path')+filename) 
        return result