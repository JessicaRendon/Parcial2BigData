import json
import os
import boto3
from bs4 import BeautifulSoup
from urllib.parse import unquote_plus
s3 = boto3.client('s3')
import datetime as dt

def handler(event, context):
 #Nombre de las paginas
 name_eltiempo = 'eltiempo'
 name_elespectador = 'elespectador'
 #Fecha
 today = dt.datetime.today()
 day_actual = today.day
 month_actual = today.month
 year_actual = today.year
 
 #Path
 download_path_eltiempo = 'headlines/raw/periodico=' + name_eltiempo + '/year=' + str(year_actual) + '/month=' + str(month_actual) + '/day=' + str(day_actual) + '/' + name_eltiempo + '.html'
 download_path_elespectador = 'headlines/raw/periodico=' + name_elespectador + '/year=' + str(year_actual) + '/month=' + str(month_actual) + '/day=' + str(day_actual) + '/' + name_elespectador + '.html'

 #Direccion
 key_eltiempo = download_path_eltiempo
 key_elespectador = download_path_elespectador

 #Nombre del bucket
 bucketName = 'parcialpunto2download'#'bucketparcialbd'
 
 #Nombre del archivo a descargar
 download_path_eltiempo = '/tmp/{}.'.format(key_eltiempo.split('/')[-1])
 download_path_elespectador = '/tmp/{}.'.format(key_elespectador.split('/')[-1])

 #Descarga del archivo
 s3.download_file(bucketName, key_eltiempo, download_path_eltiempo)
 s3.download_file(bucketName, key_elespectador, download_path_elespectador)

 #scraping el tiempo
 with open(download_path_eltiempo) as file:
   content = file.read()
   soupET = BeautifulSoup(content,'html.parser')

 articleET = soupET.find_all('div', attrs={'class': 'article-details'})
 
 eltiempoCSV='{}; {}; {} \n'.format('categoria','titulo','link')

 for row in articleET:
   try:
    a = str(str(str(str(row.find_all('a', attrs={'class':'category'})).split('<')).split('>')).split(',')[2]).replace('"','').replace("'","")
    b = str(str(str(row.find_all('a', attrs={'class':'title'})).split('<')).split('>')[1]).replace('"','').replace("'","").replace(', /a','')
    c = 'https://www.eltiempo.com'+str(row.find_all('a', attrs={'class':'title'})).split('"')[3]
    eltiempoCSV = eltiempoCSV+'{}; {}; {} \n'.format(a,b,c)
   except:
    pass

 #Archivo resultante del scaping del tiempo
 archivo=open('/tmp/eltiempo.txt','w', encoding='utf-8') 
 archivo.write(''+eltiempoCSV)
 archivo.close()

 #Scraping el espectador
 with open(download_path_elespectador) as file:
   content = file.read()
   soupES = BeautifulSoup(content,'html.parser')

 articleES = soupES.find_all('div', attrs={'class': 'Card-Container'})
 
 elespectadorCSV='{}; {}; {} \n'.format('categoria','titulo','link')
 
 for row in articleES:
   try:
    a = str(str(str(str(row.find('h4')).split('<')).split('>')).split(',')[4]).replace('"','').replace("'","")
    b = str(str(str(row.find('h2')).split('<')).split('>')[4]).replace('"','').replace("'","").replace(', /a','')
    if str(b) == ", /path":
     b = str(str(str(row.find('h2')).split('<')).split('>')[14]).replace('"','').replace("'","").replace(', /a','')
    c = "https://www.elespectador.com"+str(str(str(row.find('h2')).split('href=')[1]).split('rel=')[0]).replace('"','')
    elespectadorCSV = elespectadorCSV+'{}; {}; {} \n'.format(a,b,c)
   except:
    pass

 #Archivo resultante del espectador 
 archivo=open('/tmp/elespectador.txt','w', encoding='utf-8') 
 archivo.write(''+elespectadorCSV)
 archivo.close()

 bucketName='parcialpunto2scrapped'#'scrappingbd'
 #Path de subida
 upload_path_eltiempo = 'headlines/final/periodico='+name_eltiempo+'/year='+str(year_actual)+'/month='+str(month_actual)+'/day='+str(day_actual)+'/'+name_eltiempo+'.csv'
 upload_path_elespectador = 'headlines/final/periodico='+name_elespectador+'/year='+str(year_actual)+'/month='+str(month_actual)+'/day='+str(day_actual)+'/'+name_elespectador+'.csv'
 #Subida del archivo 
 s3.upload_file('/tmp/eltiempo.txt', bucketName, upload_path_eltiempo)
 s3.upload_file('/tmp/elespectador.txt', bucketName, upload_path_elespectador)

#Actualizacion de las particiones de Athena
def athenaupdate(event, context):
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString='msck repair table final',
        QueryExecutionContext={
            'Database': 'news',
        },
         ResultConfiguration={
        'OutputLocation': 's3://parcialpunto2logs/',
        
        },
      WorkGroup='primary'
    )