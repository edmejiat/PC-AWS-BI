class Params:
    def __init__(self):
        self._params = {
            'region': 'us-east-1',
            'database': 'puntoscolombia_datalake_dev_datagov_db',
            'bucket': 'puntoscolombia-datalake-dev-us-east-1-383560356682-analytics',
            'path': 'athena-datascientist/danilo.mejia/python_query_result/',
            'name_report' : 'INFO_CAMPAIGN',
            'name_report1' : 'ACUMULACIONES_CAMPAIGN',
            'name_report2' : 'REDENCIONES_CAMPAIGN',
            'name_report3' : 'METRICAS_COMUNICACION_CAMPAIGN'
        }

    def get_param(self, param):
        return self._params[param]
        
