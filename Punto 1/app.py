import  boto3, time, wget, urllib
from datetime import datetime, timedelta 

s3 = boto3.resource('s3')

today = datetime.today()
day_actual = today.day
month_actual = today.month
year_actual = today.year

today.weekday()

my_stocks =['AVHOQ', 'EC','AVAL','CMTOY']

def get_data():
        today = datetime.today()-timedelta(days=3)
        today = today.replace(hour=13)
        today = int(time.mktime(today.timetuple()))

        #Prueba día 20 octubre 1634688000 1634774400
        for symbol in my_stocks:
            try:
                url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1=1634688000&period2=1634774400&interval=1d&events=history&includeAdjustedClose=true"
                #url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={today}&period2={today}&interval=1d&events=history&includeAdjustedClose=true"
                wget.download(url, out = f'/tmp/{symbol}.csv')
            except urllib.error.HTTPError as e:
                en_ceros = open(f'/tmp/{symbol}.csv','w+')
                en_ceros.write(f'Date,Open,High,Low,Close,Adj Close,Volume \n{datetime.today().replace(hour=13).strftime("%Y-%m-%d")},0,0,0,0,0,0')
                en_ceros.close()
                print('error')

def handler(event, context):    
    if today.weekday() in [0, 1, 2, 3, 4, 5]:
        get_data()
        for i in range(len(my_stocks)):
            upload_path = f'stocks/company={my_stocks[i]}/year={year_actual}/month={month_actual}/day={day_actual}/{my_stocks[i]}.csv'
            s3.meta.client.upload_file(f'/tmp/{my_stocks[i]}.csv', "parcialpunto1download" , upload_path)
    return{'status':200}

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