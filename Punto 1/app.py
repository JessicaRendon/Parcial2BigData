import  boto3, time, wget 
from datetime import datetime, timedelta 
s3 = boto3.resource('s3')

today = datetime.today()
day_actual = today.day
month_actual = today.month
year_actual = today.year

today.weekday()

my_stocks =['AVHOQ', 'EC','AVAL','CMTOY']

def handler(event, context):
	if today.weekday() in [0, 1, 2, 3, 4, 5]:

		using_get_data()

		for i in range(len(my_stocks)):
			upload_path = f'stocks/company={my_stocks[i]}/year={year_actual}/month={month_actual}/day={day_actual}/{my_stocks[i]}.csv'
			s3.meta.client.upload_file(f'{my_stocks[i]}.csv', "parcialpunto1download" , upload_path)

		print("ENTRA hello from zappa")



def get_data(symbol):
	today = datetime.today()-timedelta(days=1)
	
	today = today.replace(hour=13)

	today = int(time.mktime(today.timetuple()))


	url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={today}&period2={today}&interval=1d&events=history&includeAdjustedClose=true"
	wget.download(url, f'{symbol}.csv')

def using_get_data():
    
    for item in my_stocks:
        get_data(item)

