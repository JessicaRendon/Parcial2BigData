Python 3.8.10 (tags/v3.8.10:3d8993a, May  3 2021, 11:48:03) [MSC v.1928 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> 
import requests
import boto3
import time

bucket="zappa-parcial2-descargas"
def handler(event,context):

	localtime=time.localtime()
	
	s3 = boto3.resource('s3')
	getHTMLFinances("Avianca","https://query1.finance.yahoo.com/v7/finance/download/AVHOQ?period1=1634601600&period2=1634688000&interval=1d&events=history&includeAdjustedClose=true",localtime,bucket,s3)
	getHTMLFinances("Ecopetrol","https://query1.finance.yahoo.com/v7/finance/download/EC?period1=1634774400&period2=1634860800&interval=1d&events=history&includeAdjustedClose=true",localtime,bucket,s3)
	getHTMLFinances("GrupoAval","https://query1.finance.yahoo.com/v7/finance/download/AVAL?period1=1634774400&period2=1634860800&interval=1d&events=history&includeAdjustedClose=true",localtime,bucket,s3)
	getHTMLFinances("CementosArgos","https://query1.finance.yahoo.com/v7/finance/download/CMTOY?period1=1634774400&period2=1634860800&interval=1d&events=history&includeAdjustedClose=true",localtime,bucket,s3)

	return {
			"status_code":200
		}

def getHTMLFinances(name, url,localtime,bucketname,s3):	
	headers = {'User-Agent': 'Mozilla'}
	r = requests.get(url, headers=headers)
	filepath="/tmp/"+name+".csv"
	f = open(filepath,"w")
	print("Saving file from "+name)
	f.write(r.text)
	f.close()
	path = 'stocks/company='+name+'/year='+str(localtime.tm_year)+'/month='+str(localtime.tm_mon)+'/day='+str(localtime.tm_mday)+'/'+str(localtime.tm_hour)+str(localtime.tm_min)+str(localtime.tm_sec)+'page.csv'
	s3.meta.client.upload_file(filepath, bucketname, path)