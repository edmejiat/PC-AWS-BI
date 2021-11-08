from data.dynamo_access import DynamoAccess
from data.athena_access import AthenaAccess
from data.s3_access import S3Access
from io import StringIO
import boto3
import pandas as pd
from entity.params import Params
from components.dates import Dates

class Orquestration:
    def __init__(self):
        #dynamo connect for partner report
        self._dynamo_access = DynamoAccess()
        self.params = Params()

    def prueba_athena(self,campaign,start_date,end_date):
        #Setup dynamo table
        self._dynamo_access.set_table("puntos_colombia_bi_report_automation_querys")
        
        #Get dynamo record based on key name_report.
        report = self._dynamo_access.get_item("name_report", self.params.get_param('name_report'))
        bucket_destination = str(report["bucket_destination"])
        file_path_destination = str(report["file_path_destination"])
        
        
        #initialize clasess to acces S3 and Athena and dates for query execution.
        athena = AthenaAccess()
        s3Access = S3Access()

        
        #Get query string
        queryToExecute= str(report["query"])
        queryToExecute = queryToExecute.replace('$_START_DATE_$', start_date)
        queryToExecute = queryToExecute.replace('$_END_DATE_$', end_date)
        queryToExecute = queryToExecute.replace('$_CAMPAIGN_DATE_$', str(campaign))
        
        
        #Si quiero quemar un codigo aqui, seria con la siguiente linea: (comento las de dynamo y descomento la de mas abajo)
        #queryToExecute='select * from "puntoscolombia_datalake_prod_datagov_db"."v_cmt_addresses" limit 10'
        
        file_name = athena.athena_to_s3(queryToExecute,300)
        print('FileName: ' + str(file_name))
        if file_name != False:
            obj = s3Access.readS3File(file_name)
            data_frame_athena_result = pd.read_csv(obj['Body'])
            file_name = 'acumulacion'+str(campaign)+'.csv'
            self.dataframe_to_s3(bucket_destination, file_path_destination, file_name, data_frame_athena_result)
           #self.dataframe_to_s3(self.params.get_param('bucket'), self.params.get_param('path_result'), file_name, data_frame_athena_result)
        else:
            print("Error reading s3 file with query results.")
            return False
        
        
        
    def dataframe_to_s3(self, bucket_destination, file_path_destination, file_name, df):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding='UTF-8',sep=';')
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket_destination, file_path_destination + file_name).put(Body=csv_buffer.getvalue())