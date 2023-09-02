from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
#from dags.climacdmx import clima_cdmx

def clima_cdmx():
    import time
    import requests
    import psycopg2 as ps
    import pandas as pd

    def call_api():
        base_url = 'https://api.datos.gob.mx/v1/condiciones-atmosfericas'
        df_final = pd.DataFrame(columns=['_id', 'cityid', 'validdateutc', 'winddirectioncardinal', 'probabilityofprecip', 'relativehumidity',
                                    'name', 'date-insert', 'longitude', 'state', 'lastreporttime', 'skydescriptionlong',
                                    'stateabbr', 'tempc', 'latitude', 'iconcode', 'windspeedkm'])
        for pagina in range(107, 108):
            URL = base_url + '?page=' + str(pagina)
            g_url = requests.get(URL, timeout=20)
            response = g_url.json()
            df_aux = pd.json_normalize(response["results"])
            df_final = pd.concat([df_final, df_aux])
            time.sleep(1)
            print(URL)
        
        df_final = df_final.rename(columns={"_id": "id_clima", "name": "city_name", "date-insert": "date_insert"})
        
        return df_final

    def conexion_rs(): 
        from airflow.models import Variable

        HOST_NAME = Variable.get("Host")
        DBNAME = Variable.get("Dbname")
        PORT = Variable.get("Port")
        USERNAME = Variable.get("Username")
        PASSWORD = Variable.get("Pass")

        try: 
            conn = ps.connect(host=HOST_NAME, dbname=DBNAME, user=USERNAME, password=PASSWORD, port=PORT)
        except ps.OperationalError as e:
            raise e
        else:
            print('conexion exitosa')
            return conn

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


    df = call_api()
    print(df)

    conn = conexion_rs()
    curr = conn.cursor()

    new_df = actualizar_db(curr,df)
    conn.commit()

    agregar_a_rs(curr, new_df)
    conn.commit()

    print("Carga Realizada")

def Alerta_termino():
    from airflow.models import Variable
    from email.mime.text import MIMEText
    import smtplib

    sender = Variable.get("Correo")
    Pmail = Variable.get("Pcorreo")
    destination = Variable.get("Dcorreo")

    mensaje = MIMEText("La carga de datos se ha realizado con exito")
    mensaje["Subject"] = "Carga con exito"
    mensaje["From"] = sender
    mensaje["To"] = destination

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp_server:
        smtp_server.login(sender,Pmail)
        smtp_server.sendmail(sender, destination, mensaje.as_string())

    print("Alerta Enviada")


default_args={
    'owner': 'Erick',
    'retries':5,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id='CargaDatos',
    description= 'Se carga la información a RS',
    start_date=datetime(2023,8,24),
    schedule_interval=None
    ) as dag:


    task1 = PythonOperator(
        task_id='Tarea',
        execution_timeout=timedelta(minutes=40),
        python_callable=clima_cdmx,
        dag = dag,
    )

    task2 = PythonOperator(
        task_id='Correo',
        python_callable=Alerta_termino,
        dag = dag,
    )

task1 >> task2