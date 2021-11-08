from datetime import date
import datetime
from dateutil import relativedelta

class Dates:
    def __init__(self):
        #Calculo de las fechas actual, inicio de mes y fin de mes anterior y actual
        self.today = datetime.date.today() # fecha actual.
        self.yesterday = datetime.date.today() - datetime.timedelta(days=1) # ayer.
        self.first_day_current_month = self.today.replace(day=1) # cambia el dia por el 1er dia del mes.
        self.last_day_last_month = self.first_day_current_month - datetime.timedelta(days=1) # resta 1 dia a fecha 1ero de mes.
        self.first_day_last_month = self.last_day_last_month.replace(day=1) #Al ultimo dia del mes anterior le cambia a 1ero de mes
        self.first_day_next_month = self.first_day_current_month + relativedelta.relativedelta(months=1) # calcula el 1ero del mes siguiente
        self.last_day_current_month = self.first_day_next_month - datetime.timedelta(days=1) # resta 1 dia a fecha 1ero de mes siguiente.

    def set_dates(self):
        self.response = {'start_date': '', 'end_date': ''}
        #Si es 1ero  se calcula con base en el mes anterior. sino,la base es el mes actual.
        self.response['start_date'] = self.first_day_last_month if (self.today.strftime("%d") == "01") else self.first_day_current_month
        self.response['end_date'] = self.yesterday
        self.response['today'] = self.today

    def get_dates(self):
        return self.response