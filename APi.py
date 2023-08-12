import requests
import time
import json
import pandas as pd


base_url = 'https://api.datos.gob.mx/v1/condiciones-atmosfericas'

def call_API(base_url):
    df_final = pd.DataFrame(columns=['_id', 'cityid', 'validdateutc', 'winddirectioncardinal', 'probabilityofprecip', 'relativehumidity',
                                 'name', 'date-insert', 'longitude', 'state', 'lastreporttime', 'skydescriptionlong',
                                 'stateabbr', 'tempc', 'latitude', 'iconcode', 'windspeedkm'])
    for pagina in range(1, 100):
        URL = base_url + '?page=' + str(pagina)
        g_url = requests.get(URL)
        response = g_url.json()
        df_aux = pd.json_normalize(response["results"])
        df_final = pd.concat([df_final, df_aux])
        time.sleep(1)
        #df_final = df_final.rename(columns={"_id": "id_clima", "name": "city_name", "date-insert": "date_insert"})
        print(URL)
    return df_final

def cambio_nombres(data):
    df_final = data.rename(columns={"_id": "id_clima", "name": "city_name", "date-insert": "date_insert"})
    return df_final