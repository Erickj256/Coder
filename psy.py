import psycopg2 as ps
import pandas as pd
from Entregable1 import data

#credenciales: 
HOST_NAME = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
DBNAME = 'data-engineer-database'
PORT = '5439'
USERNAME = 'josafat_math_coderhouse'
PASSWORD = '0ZJqN12xni'

#funcion para realizar la conexion a RS

def conexion_rs(host_name, dbname, port, username, password):
    try: 
        conn = ps.connect(host=HOST_NAME, dbname=DBNAME, user=USERNAME, password=PASSWORD, port=PORT)
    except ps.OperationalError as e:
        raise e
    else:
        print('conexion exitosa')
        return conn

    
#funcion para insertar datos en la tabla
def insertar_registros(curr, idclima,cityid,validdateutc,winddirectioncardinal,probabilityofprecip, relativehumidity, name, date_insert,
                       longitude, state, lastreporttime, skydescriptionlong,stateabbr, tempc, latitude, iconcode, windspeedkm):
    
    insertar_climacdmx = ("""INSERT INTO josafat_math_coderhouse.clima_cdmx (
                            idclima,cityid,validdateutc,winddirectioncardinal,
                            probabilityofprecip, relativehumidity, name, date_insert,
                            longitude, state, lastreporttime, skydescriptionlong,stateabbr, tempc, latitude, iconcode, windspeedkm)
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""")
    
    insertando_row = (idclima,cityid,validdateutc,winddirectioncardinal,probabilityofprecip, relativehumidity, name, date_insert,
                       longitude, state, lastreporttime, skydescriptionlong,stateabbr, tempc, latitude, iconcode, windspeedkm)
    
    curr.execute(insertar_climacdmx, insertando_row)
                            

#funcion update
def update_row(curr,idclima,cityid,validdateutc,winddirectioncardinal,probabilityofprecip, relativehumidity, name, date_insert,
                       longitude, state, lastreporttime, skydescriptionlong,stateabbr, tempc, latitude, iconcode, windspeedkm):
    query = ( """UPDATE josafat_math_coderhouse.clima_cdmx
                SET cityid= %s, 
                    validdateutc=%s,
                    winddirectioncardinal=%s,
                    probabilityofprecip=%s,
                    relativehumidity=%s, 
                    name=%s,
                    date_insert=%s,
                    longitude=%s,
                    state=%s, 
                    lastreporttime=%s, 
                    skydescriptionlong=%s,
                    stateabbr=%s,
                    tempc=%s,
                    latitude=%s, 
                    iconcode=%s,
                    windspeedkm=%s
                WHERE idclima = %s; 
                """)
    columns_to_update = ('cityid', 'validdateutc', 'winddirectioncardinal',
                        'probabilityofprecip', 'relativehumidity', 'name', 'date_insert',
                        'longitude', 'state', 'lastreporttime', 'skydescriptionlong',
                        'stateabbr', 'tempc', 'latitude', 'iconcode', 'windspeedkm','idclima')
    curr.execute(query,columns_to_update)

#funcion que nos indica si existe el registro
def existe_registro(curr, idclima):
    query = ("""SELECT idclima FROM josafat_math_coderhouse.clima_cdmx WHERE idclima = %s;""")
    curr.execute(query, (idclima,))
    return curr.fetchone() is not None

#agrega del dataframe a RS
def agregar_a_rs(curr, data):
    for i, row in data.iterrows():
        insertar_registros(curr,row['idclima'], row['cityid'], row['validdateutc'], row['winddirectioncardinal'],
                            row['probabilityofprecip'], row['relativehumidity'], row['name'],row['date_insert'],
                            row['longitude'], row['state'], row['lastreporttime'], row['skydescriptionlong'],
                            row['stateabbr'], row['tempc'], row['latitude'], row['iconcode'], row['windspeedkm'])

#funcion para revisar si la informacon ya se encuentra en la DB
def actualizar_db(curr, data):
    for i, row in data.iterrows():
        if existe_registro(curr, row["idclima"]):
            update_row(curr,row['idclima'], row['cityid'], row['validdateutc'], row['winddirectioncardinal'],
                            row['probabilityofprecip'], row['relativehumidity'], row['name'],row['date_insert'],
                            row['longitude'], row['state'], row['lastreporttime'], row['skydescriptionlong'],
                            row['stateabbr'], row['tempc'], row['latitude'], row['iconcode'], row['windspeedkm']) #En caso de que el registro este, simplemente se actualizara 
        else:
            data = data.append(row) #si no esta, el registro del clima, lo agregaremos a la tabla

    return data

conn = conexion_rs(HOST_NAME, DBNAME, PORT, USERNAME, PASSWORD)
curr = conn.cursor()


print(data)

#new_data_df = actualizar_db(curr,data)
#conn.commit()

agregar_a_rs(curr,data)
conn.commit()

curr.execute("SELECT * FROM josafat_math_coderhouse.clima_cdmx;")
print(curr.fetchall())
