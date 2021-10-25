#importaciones necesarias
import  boto3, time, wget
from datetime import datetime, timedelta 

#definir el S3 usando Boto3
s3 = boto3.resource('s3')

#se extrae el tiempo del día que se ejecute y se separa en variables día, mes, año
today = datetime.today()
day_actual = today.day
month_actual = today.month
year_actual = today.year

today.weekday()

#se extraen las keys para descargar la información de Avianca, Ecopetrol, Grupo Aval y Cemento Argos
array_keys =['AVHOQ', 'EC','AVAL','CMTOY']

#función principal 
def handler(event, context):    
        if today.weekday() in [0, 1, 2, 3, 4, 5]:
            get_data()
            for i in range(len(array_keys)):
                upload_path = f'stocks/company={array_keys[i]}/year={year_actual}/month={month_actual}/day={day_actual}/{array_keys[i]}.csv'
                s3.meta.client.upload_file(f'/tmp/{array_keys[i]}.csv', "parcialpunto1download" , upload_path)
        return{'status':200}
        
#Esta función permite convertir el formato de fecha a un entero el cual será utilizado para descargar los archivos de la página de yahoo
#tambien descarga el archivo
def get_data():
        today = datetime.today()-timedelta(days=1)
        today = today.replace(hour=13)
        today = int(time.mktime(today.timetuple()))

        for symbol in array_keys:
            url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={today}&period2={today}&interval=1d&events=history&includeAdjustedClose=true"
            wget.download(url, out = f'/tmp/{symbol}.csv')

#para actualizar el athena
def uploadathena(event, context):
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString='msck repair table stocks',
        QueryExecutionContext={
            'Database': 'stocks',
        },
        ResultConfiguration={
            'OutputLocation': 's3://parcialpunto1upload/trash/'
        },
        WorkGroup='primary'
    )

