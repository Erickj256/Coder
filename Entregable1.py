# importamos las librerias que usaremos
import requests
import time 
import json 
import pandas as pd

# La API seleccionada es sobre las condiciones admosfericas y es facilitada por el gobierno de la Ciudad de México
base_url = 'https://api.datos.gob.mx/v1/condiciones-atmosfericas'

# Creamos un diccionario y crearemos las keys con los nombres en columns_name y ademas a estas keys se le asignara una lista vacia 
clima = {} 
columns_name = ["id","cityid","validdateutc","winddirectioncardinal","probabilityofprecip","relativehumidity","name","date_insert",
                "longitude","state","lastreporttime","skydescriptionlong","stateabbr","tempc","latitude","iconcode","windspeedkm"]
for col_name in columns_name:
    clima[col_name] = []

# este for ira realizando el get a cada una de las paginas de la APi y lo ira vaciando en un Data Frame al final.
for pagina in range(1,13106056):
    URL = base_url + '?page=' + str(pagina)
    answer = requests.get(URL)
    text = answer.text
    response = json.loads(text)
    time.sleep(2)
    
    #vamos agregando a cada una de la lista los valores de la API
    for item in range(len(response["results"]) - 1):
        clima["id"].append(response["results"][item]["_id"])
        clima["cityid"].append(response["results"][item]["cityid"])
        clima["validdateutc"].append(response["results"][item]["validdateutc"])
        clima["winddirectioncardinal"].append(response["results"][item]["winddirectioncardinal"])
        clima["probabilityofprecip"].append(response["results"][item]["probabilityofprecip"])
        clima["relativehumidity"].append(response["results"][item]["relativehumidity"])
        clima["name"].append(response["results"][item]["name"])
        clima["date_insert"].append(response["results"][item]["date-insert"])
        clima["longitude"].append(response["results"][item]["longitude"])
        clima["state"].append(response["results"][item]["state"])
        clima["lastreporttime"].append(response["results"][item]["lastreporttime"])
        clima["skydescriptionlong"].append(response["results"][item]["skydescriptionlong"])
        clima["stateabbr"].append(response["results"][item]["stateabbr"])
        clima["tempc"].append(response["results"][item]["tempc"])
        clima["latitude"].append(response["results"][item]["latitude"])
        clima["iconcode"].append(response["results"][item]["iconcode"])
        clima["windspeedkm"].append(response["results"][item]["windspeedkm"])
    print(URL)

data = pd.DataFrame(clima)
print(data)