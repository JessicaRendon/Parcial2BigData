import urllib, urllib.request
from urllib import request
import requests
import json
import boto3
import time

bucket="parcialpunto2download"

nametiempo = 'El Tiempo'
urltiempo = 'https://www.eltiempo.com/'

nameespectador = 'El Espectador'
urlespectador = 'https://www.elespectador.com/'
def handler(event,context):
    localtime=time.localtime()
    s3 = boto3.resource('s3')
    
    #tiempo
    headers = {'User-Agent': 'Mozilla'}
    r = requests.get(urltiempo, headers=headers)
    filepath="/tmp/"+nametiempo+".txt"
    f = open(filepath,"w")
    print("Saving file from "+nametiempo)
    f.write(r.text)
    f.close()
    path = 'headlines/raw/periodico='+nametiempo+'/year='+str(localtime.tm_year)+'/month='+str(localtime.tm_mon)+'/day='+str(localtime.tm_mday)+'/'+nametiempo+'.txt'
    s3.meta.client.upload_file(filepath, bucket, path)

    #Espectador
    headers = {'User-Agent': 'Mozilla'}
    r = requests.get(urlespectador, headers=headers)
    filepath="/tmp/"+nameespectador+".txt"
    f = open(filepath,"w")
    print("Saving file from "+nameespectador)
    f.write(r.text)
    f.close()
    path = 'headlines/raw/periodico='+nameespectador+'/year='+str(localtime.tm_year)+'/month='+str(localtime.tm_mon)+'/day='+str(localtime.tm_mday)+'/'+nameespectador+'.txt'
    s3.meta.client.upload_file(filepath, bucket, path)

        
    return {
        "status_code":200
        }
