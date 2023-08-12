import psycopg2 as ps
import pandas as pd
import APi

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

def crear_tabla(curr):
    tabla = (
        """ CREATE TABLE IF NOT EXISTS josafat_math_coderhouse.clima_cdmx (
	        id_clima varchar (50) PRIMARY KEY,
            cityid varchar(50) NOT NULL,
            validdateutc varchar(50) NOT NULL,
            winddirectioncardinal varchar (25) NOT NULL,
            probabilityofprecip varchar(25) NOT NULL,
            relativehumidity varchar(25) NOT NULL, 
            city_name varchar(50) NOT NULL,
            date_insert varchar(60) NOT NULL,
            longitude varchar(25) NOT NULL,
            state varchar(50) NOT NULL,
            lastreporttime varchar(50) NOT NULL,
            skydescriptionlong varchar(50) NOT NULL,
            stateabbr varchar(25) NOT NULL,
            tempc varchar(25) NOT NULL,
            latitude varchar(25) NOT NULL,
            iconcode varchar(25) NOT NULL,
            windspeedkm varchar(25) NOT NULL 
            )""")
    curr.execute(tabla)

def insertar_registros(curr, id_clima,cityid,validdateutc,winddirectioncardinal,probabilityofprecip, relativehumidity, city_name, date_insert,
                       longitude, state, lastreporttime, skydescriptionlong,stateabbr, tempc, latitude, iconcode, windspeedkm):
    
    insertar_climacdmx = ("""INSERT INTO josafat_math_coderhouse.clima_cdmx (
                            id_clima,cityid,validdateutc,winddirectioncardinal,
                            probabilityofprecip, relativehumidity, city_name, date_insert,
                            longitude, state, lastreporttime, skydescriptionlong,stateabbr, tempc, latitude, iconcode, windspeedkm)
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""")
    
    insertando_row = (id_clima,cityid,validdateutc,winddirectioncardinal,probabilityofprecip, relativehumidity, city_name, date_insert,
                       longitude, state, lastreporttime, skydescriptionlong,stateabbr, tempc, latitude, iconcode, windspeedkm)
    
    curr.execute(insertar_climacdmx, insertando_row)

def actualizar_fila(curr, id_clima, cityid, validdateutc, winddirectioncardinal , probabilityofprecip , relativehumidity, 
                    city_name, date_insert, longitude, state, lastreporttime, skydescriptionlong,stateabbr, tempc,
                    latitude, iconcode, windspeedkm):
    query = (
                """
                UPDATE josafat_math_coderhouse.clima_cdmx
                    SET cityid = %s,
                    validdateutc = %s,
                    winddirectioncardinal = %s,
                    probabilityofprecip = %s,
                    relativehumidity = %s, 
                    city_name = %s,
                    date_insert = %s,
                    longitude = %s,
                    state = %s,
                    lastreporttime = %s,
                    skydescriptionlong = %s,
                    stateabbr = %s,
                    tempc = %s,
                    latitude = %s,
                    iconcode = %s,
                    windspeedkm = %s 
                WHERE id_clima = %s;
                """
                )
    variables_update = (cityid, validdateutc, winddirectioncardinal , probabilityofprecip , relativehumidity, 
                        city_name, date_insert, longitude, state, lastreporttime, skydescriptionlong,
                        stateabbr, tempc, latitude, iconcode, windspeedkm,id_clima)

    curr.execute(query,variables_update)

def existen_videos(curr, id_clima):
    query = (""" SELECT id_clima FROM josafat_math_coderhouse.clima_cdmx WHERE id_clima = %s """)
    curr.execute(query, (id_clima,))
    return curr.fetchone() is not None

def agregar_a_rs(curr, df):
    for i, row in df.iterrows():
        insertar_registros(curr,row['id_clima'], row['cityid'], row['validdateutc'], row['winddirectioncardinal'],
                            row['probabilityofprecip'], row['relativehumidity'], row['city_name'],row['date_insert'],
                            row['longitude'], row['state'], row['lastreporttime'], row['skydescriptionlong'],
                            row['stateabbr'], row['tempc'], row['latitude'], row['iconcode'], row['windspeedkm'])

def actualizar_db(curr,df): 

    temp_row  = []

    for i, row in df.iterrows():
        if existen_videos(curr,row["id_clima"]):
            actualizar_fila(curr,row["id_clima"], row["cityid"], row["validdateutc"], row["winddirectioncardinal"], row["probabilityofprecip"], 
                            row["relativehumidity"], row["city_name"], row["date_insert"], row["longitude"], row["state"], 
                            row["lastreporttime"], row["skydescriptionlong"],row["stateabbr"], row["tempc"], row["latitude"], 
                            row["iconcode"], row["windspeedkm"])
        else:
            fila = row
            temp_row.append(fila)
            temp_df = pd.DataFrame(temp_row, columns=["id_clima","cityid", "validdateutc", "winddirectioncardinal", "probabilityofprecip", "relativehumidity", 
                                                     "city_name", "date_insert", "longitude", "state", "lastreporttime", "skydescriptionlong",
                                                     "stateabbr", "tempc", "latitude", "iconcode", "windspeedkm"])
    return temp_df

