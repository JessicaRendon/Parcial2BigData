
import json
import requests
from bs4 import BeautifulSoup
import os
import boto3
import datetime as dt
s3 = boto3.client('s3')

def handler(event, context):

 newBucket='parcialpunto2download'#'bucketparcialbd'
 #Descarga la pagina "el tiempo"
 t = requests.get('https://www.eltiempo.com/')
 soup_t = BeautifulSoup(t.text, 'lxml')
 name_eltiempo='eltiempo'
 #Descarga la pagina "el espectador"
 e = requests.get('https://www.elespectador.com')
 soup_e = BeautifulSoup(e.text, 'lxml')
 name_elespectador='elespectador'
 #Crea el archivo el tiempo
 archivo=open('/tmp/'+name_eltiempo+'.html','w', encoding='utf-8')
 archivo.write(str(soup_t))
 archivo.close()
 #Crea el archivo el espectador
 archivo=open('/tmp/'+name_elespectador+'.html','w', encoding='utf-8')
 archivo.write(str(soup_e))
 archivo.close()
 #Fecha
 today = dt.datetime.today()
 day_actual = today.day
 month_actual = today.month
 year_actual = today.year 

 #Path de subida
 upload_path_eltiempo = 'headlines/raw/periodico='+name_eltiempo+'/year='+str(year_actual)+'/month='+str(month_actual)+'/day='+str(day_actual)+'/'+name_eltiempo+'.html'
 upload_path_elespectador = 'headlines/raw/periodico='+name_elespectador+'/year='+str(year_actual)+'/month='+str(month_actual)+'/day='+str(day_actual)+'/'+name_elespectador+'.html'

 #Subida del archivo
 s3.upload_file('/tmp/'+name_eltiempo+'.html', newBucket, upload_path_eltiempo)
 s3.upload_file('/tmp/'+name_elespectador+'.html', newBucket, upload_path_elespectador)

 
 return {
  'status': 200
 }


