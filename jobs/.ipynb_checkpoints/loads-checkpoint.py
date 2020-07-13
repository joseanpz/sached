from datetime import datetime
import pandas as pd
import numpy as np
from itertools import chain, zip_longest
from functools import reduce
import multiprocessing
import io
import boto3
import gzip
import json
import logging

import redis


redis_connection = redis.Redis(host='127.0.0.1', port=6379, password='r3d15p4s5w0rd')


logger = logging.getLogger('apscheduler')


# ruta de trabajo en s3
# bucket = 'metricas-pintpoint'
bucket = 'metricas-pinpoint-prod'
# bucket = 'metricas-pintpoint'


# s3 resource
s3_bucket_resource = boto3.resource('s3').Bucket(bucket)

meses = ['00', '01', '02', '03', '04',
         '05', '06', '07', '08',
         '09', '10', '11', '12']
dias = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09',
        '10', '11', '12', '13', '14', '15', '16', '17', '18', '19',
        '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30','31']
horas = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09',
         '10', '11', '12', '13', '14', '15', '16', '17', '18', '19',
         '20', '21', '22', '23']


def load_events(prefix, events=[]):
    for obj in s3_bucket_resource.objects.filter(Prefix=prefix):  # Iteramos sobre las particiones de las tablas
        if obj.key.find('$folder$') == -1:
            # print(obj.key)
            obj_paso = s3_bucket_resource.Object(obj.key).get()

            # io stream
            byte_io_ziped = io.BytesIO(obj_paso['Body'].read())

            # unzip
            byte_io = gzip.GzipFile(fileobj=byte_io_ziped)

            # read lines and convert to dictionary
            for event in byte_io.readlines():
                events.append(json.loads(event))


def load_events_io(anio, mes, dia_inicial, hora_inicial, cantidad_dias, cantidad_horas):
    events = []
    # print(anio, mes, dia_inicial, hora_inicial, cantidad_dias, cantidad_horas)
    for dia in dias[dia_inicial:dia_inicial+cantidad_dias]:
        for hora in horas[hora_inicial:hora_inicial+cantidad_horas]:
            prefix = f'HEY_PROD_{anio}/{mes}/{dia}/{hora}'
            # print(f'Prefijo {prefix}')
            logger.info(f'Prefijo {prefix}')
            load_events(prefix, events)
    return events


def load_events_job():
    now = datetime.now()
    dia_inicial = now.day
    hora_inicial = now.hour
    
    logger.info((str(now.year), meses[now.month], dia_inicial, hora_inicial))
    
    events = load_events_io(str(now.year), meses[now.month], dia_inicial, hora_inicial, 1, 1)
    
    # add to stream
    redis_connection.xadd('events', {'count': len(events)})
    
    logger.info(f'Longitud de eventos {len(events)}, dia inicial {dia_inicial} mes {now.month}, epoch {now}',)
    # print(len(events), dia_inicial, now.month, now)
    
    