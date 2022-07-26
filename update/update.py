#!/usr/bin/env python3

import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
import os

def update_cookies(response):
    cookies['PHPSESSID'] = response.cookies.get('PHPSESSID')

def update_data(response):
    html = BeautifulSoup(response.text, 'html.parser')
    for name in ['B903A6B7', 'varSesionCli']:
        i = html.select('input[name={}]'.format(name))
        if len(i) > 0:
            data[name] = i[0]['value']

def get_session():
    global data
    global cookies

    url = 'https://www.sicoes.gob.bo/portal/contrataciones/busqueda/convocatorias.php?tipo=convNacional'
    
    response = requests.get(url, cookies=cookies)
    update_cookies(response)
    
    response = requests.get(url, cookies=cookies)
    update_data(response)

def parse_field(field, encoding):
    if '%' in field:
        return bytes.fromhex(field.replace('%', '')).decode(encoding)
    else:
        return field

def parse_results(response_json, encoding='iso-8859-1'):
    return [{field: parse_field(item[field], encoding) for field in item.keys()} for item in response_json['data']]

def search():
    global total_results
    
    response = requests.post('https://www.sicoes.gob.bo/portal/contrataciones/operacion.php', cookies=cookies, data=data)
    
    if 'error' not in response.json().keys():
        update_cookies(response)
        update_data(response)
        r = response.json()
        total_results = r['recordsTotal']
        return parse_results(r)
    
    else:
        return 'error'

def search_all():
    
    while True:
        results = search()
        if results == 'error':

            get_session()
        else:

            print('{}/{}'.format((int(data['draw']) - 1) * 10, total_results))
            all_results.extend(results)
            data['draw'] = str(int(data['draw']) + 1)

            if len(results) < 10:
                break

def format_results(results):
    names = [
        'CUCE',
        'Entidad',
        'Tipo de Contratación',
        'Modalidad',
        'Objeto de Contratación',
        'Estado',
        'Subasta',
        'Fecha Presentación',
        'Fecha Publicación',
        'Archivos',
        'Formularios',
        'Ficha del proceso',
        'Persona contacto',
        'Garantía',
        'CostoPliego',
        'ARPC',
        'Reunión aclaración',
        'Fecha Adjudicación / Desierta',
        'Departamento',
        'Normativa'
    ]
    
    df = pd.DataFrame(results)
    df.columns = names
    
    for col in ['Fecha Presentación', 'Fecha Publicación', 'Fecha Adjudicación / Desierta']:
        df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')
        
    df['Subasta'] = df['Subasta'].map({'Si': True, 'No': False})
        
    df.drop(columns=['Garantía', 'CostoPliego', 'ARPC', 'Reunión aclaración', 'Ficha del proceso'], inplace=True)
    
    for col in ['Archivos', 'Formularios']:
        df[col] = df[col].apply(lambda x: [a.get_text() for a in BeautifulSoup(x, 'html.parser').select('a')])
    
    return df


cookies = {
    'cpttxhHQrES2eWopmC6e+yrKFa1G': 'v1RbeGSQSDF6E'
}

data = {
    'entidad': '',
    'codigoDpto': '',
    'cuce1': '',
    'cuce2': '',
    'cuce3': '',
    'cuce4': '',
    'cuce5': '',
    'cuce6': '',
    'objetoContrato': '',
    'codigoModalidad': '',
    'r1': '',
    'codigoContrato': '',
    'nroContrato': '',
    'codigoNormativa': '',
    'montoDesde': '',
    'montoHasta': '',
    'publicacionDesde': '',
    'publicacionHasta': '',
    'presentacionPropuestasDesde': '',
    'presentacionPropuestasHasta': '',
    'desiertaDesde': '',
    'desiertaHasta': '',
    'subasta': '',
    'personaContDespliegue': 'on',
    'nomtoGarDespliegue': 'option2',
    'costoPlieDespliegue': 'option3',
    'arpcDespliegue': 'option3',
    'fechaReunionDespliegue': 'option1',
    'fechaAdjudicacionDespliegue': 'option2',
    'dptoDespliegue': 'option3',
    'normativaDespliegue': 'option3',
    'tipo': 'Avanzada',
    'operacion': 'convNacional',
    'autocorrector': '',
    'nroRegistros': '10',
    'draw': '1',
    'start': '0',
    'length': '10',
    'captcha': '',
}

get_session()

ayer = (dt.datetime.now() - dt.timedelta(days=1))
for fecha in ['publicacionDesde', 'publicacionHasta']:
    data[fecha] = ayer.strftime('%d/%m/%Y')
all_results = []
total_results = 0

search_all()
df = format_results(all_results)

filename = 'data/{}.csv'.format(ayer.strftime('%Y%m'))
if os.path.exists(filename):
    old = pd.read_csv(filename, parse_dates=['Fecha Presentación', 'Fecha Publicación', 'Fecha Adjudicación / Desierta'])
    df = pd.concat([old, df])
df.to_csv(filename, index=False)
