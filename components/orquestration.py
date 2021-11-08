from data.dynamo_access import DynamoAccess
from data.athena_access import AthenaAccess
from data.s3_access import S3Access
from io import StringIO
import boto3
import pandas as pd
from entity.params import Params
from components.dates import Dates
from datetime import datetime

class Orquestration:
    def __init__(self):
        #dynamo connect for partner report
        self._dynamo_access = DynamoAccess()
        self.params = Params()

    def prueba_athena(self):
        #Setup dynamo table
        self._dynamo_access.set_table("bi_report_automation_querys")
        
        #Get dynamo record based on key name_report.
        report = self._dynamo_access.get_item("name_report", self.params.get_param('name_report'))

        acumulacion_c = self._dynamo_access.get_item("name_report", self.params.get_param('name_report1'))
        bucket_destination_acum = str(acumulacion_c["bucket_destination"])
        file_path_destination_acum = str(acumulacion_c["file_path_destination"])
        t_acumulacion=pd.DataFrame()
        
        redencion_c = self._dynamo_access.get_item("name_report", self.params.get_param('name_report2'))
        bucket_destination_rede = str(redencion_c["bucket_destination"])
        file_path_destination_rede = str(redencion_c["file_path_destination"])
        t_redencion=pd.DataFrame()
        
        metricas_comunicacion = self._dynamo_access.get_item("name_report", self.params.get_param('name_report3'))
        bucket_destination_mc = str(metricas_comunicacion["bucket_destination"])
        file_path_destination_mc = str(metricas_comunicacion["file_path_destination"])
        metricas_com=pd.DataFrame()
        
        #initialize clasess to acces S3 and Athena and dates for query execution.
        athena = AthenaAccess()
        s3Access = S3Access()
        
        
        #### Lectura de la informacion de campa;as
        queryToExecute= str(report["query"])
        file_name = athena.athena_to_s3(queryToExecute,300)
        print('FileName: ' + str(file_name))
        if file_name != False:
            obj = s3Access.readS3File(file_name)
            data_frame_athena_result = pd.read_csv(obj['Body'])
            df = pd.DataFrame(data_frame_athena_result)
        else:
            print("Error reading informacion de campaigns")
            return False
            
        campaigns = df['id'].tolist()
        
        for campaign in campaigns:
            my_campaign=df[df.id==campaign]
        
            start_date=str(my_campaign['fecha inicio'].values[0])
            start_date = datetime.strptime(start_date, '%d/%m/%Y')
            start_date=str(start_date.strftime("%Y-%m-%d"))
        
            end_date=str(my_campaign['fecha fin'].values[0])
            end_date = datetime.strptime(end_date, '%d/%m/%Y')
            end_date=str(end_date.strftime("%Y-%m-%d"))
    
        
        # #Acumulaciones
            queryToExecute= str(acumulacion_c["query"])
            queryToExecute = queryToExecute.replace('$_START_DATE_$', start_date)
            queryToExecute = queryToExecute.replace('$_END_DATE_$', end_date)
            queryToExecute = queryToExecute.replace('$_CAMPAIGN_DATE_$', str(campaign))
        
            file_name = athena.athena_to_s3(queryToExecute,300)
            print('FileName: ' + str(file_name))
            if file_name != False:
                obj = s3Access.readS3File(file_name)
                data_frame_athena_result = pd.read_csv(obj['Body'],low_memory=False)
                file_name = 'acumulacion'+str(campaign)+'.csv'
                self.dataframe_to_s3(bucket_destination_acum, file_path_destination_acum, file_name, data_frame_athena_result)
                t_acumulacion=pd.concat([t_acumulacion, data_frame_athena_result])
            else:
                print("Error reading s3 file with query results en acumulacion.")
                return False
                
        #Redenciones
            queryToExecute= str(redencion_c["query"])
            queryToExecute = queryToExecute.replace('$_START_DATE_$', start_date)
            queryToExecute = queryToExecute.replace('$_END_DATE_$', end_date)
            queryToExecute = queryToExecute.replace('$_CAMPAIGN_DATE_$', str(campaign))
        
            file_name = athena.athena_to_s3(queryToExecute,300)
            print('FileName: ' + str(file_name))
            if file_name != False:
                obj = s3Access.readS3File(file_name)
                data_frame_athena_result = pd.read_csv(obj['Body'],low_memory=False)
                file_name = 'acumulacion'+str(campaign)+'.csv'
                self.dataframe_to_s3(bucket_destination_rede, file_path_destination_rede, file_name, data_frame_athena_result)
                t_redencion=pd.concat([t_redencion, data_frame_athena_result])
            else:
                print("Error reading s3 file with query results. en redencion")
                return False
        
        #Metricas de comunicacion
            queryToExecute= str(metricas_comunicacion["query"])
            queryToExecute = queryToExecute.replace('$_CAMPAIGN_DATE_$', str(campaign))
            file_name = athena.athena_to_s3(queryToExecute,300)
            print('FileName: ' + str(file_name))
            if file_name != False:
                obj = s3Access.readS3File(file_name)
                data_frame_athena_result = pd.read_csv(obj['Body'])
                df2 = pd.DataFrame(data_frame_athena_result)
                metricas_com=pd.concat([metricas_com, data_frame_athena_result])
            else:
                print("Error reading metricas de comunicacion")
                return False
            
        
        self.dataframe_to_s3(bucket_destination_rede, file_path_destination_acum, "t_acumulacion.csv", t_acumulacion)
        self.dataframe_to_s3(bucket_destination_rede, file_path_destination_rede, "t_redencion.csv", t_redencion)
        self.dataframe_to_s3(bucket_destination_mc, file_path_destination_mc, "metricas_comunicacion.csv", metricas_com)
        
        ## Resumenes sin nivel de cliente
        
        t_acumulacion['dia'] = t_acumulacion['fecha_trx'].str[0:11]
        a=t_acumulacion.groupby(['campaign_id','dia','tipo_trx','id_aliado','nombre_aliado','id_establecimiento','nombre_establecimiento']).agg({'puntos':'sum','valor_trx':'sum','valor_trx2':'sum'})
        a = a.reset_index()
        a.columns = ["".join(pair) for pair in a.columns]
        
        self.dataframe_to_s3(bucket_destination_rede, file_path_destination_acum, "t_acumulacion_sc.csv", a)
        
        t_redencion['dia'] = t_redencion['fecha_trx'].str[0:11]
        r=t_redencion.groupby(['campaign_id','dia','tipo_trx','id_aliado','nombre_aliado','id_establecimiento','nombre_establecimiento']).agg({'puntos':'sum','valor_trx':'sum','valor_trx2':'sum'})
        r = r.reset_index()
        r.columns = ["".join(pair) for pair in r.columns]
        
        self.dataframe_to_s3(bucket_destination_rede, file_path_destination_rede, "t_redencion_sc.csv", r)
        
    def dataframe_to_s3(self, bucket_destination, file_path_destination, file_name, df):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding='UTF-8',sep=';')
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket_destination, file_path_destination + file_name).put(Body=csv_buffer.getvalue())