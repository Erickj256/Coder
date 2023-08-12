import APi
import Entregable2 as ent

#credenciales
HOST_NAME = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
DBNAME = 'data-engineer-database'
PORT = '5439'
USERNAME = 'josafat_math_coderhouse'
PASSWORD = '0ZJqN12xni'

#llamamos la informacion de la API
df_raw = APi.call_API(base_url='https://api.datos.gob.mx/v1/condiciones-atmosfericas')

# renombramos algunas columnas para no tener problemas
df = APi.cambio_nombres(df_raw)

#realizamos la conexion a RS
conn = ent.conexion_rs(HOST_NAME, DBNAME, USERNAME, PASSWORD, PORT)
curr = conn.cursor()

# creamos la tabla, solo se ejecuta una vez
#crear_tabla(curr)

#actualizamos la tabla
new_df = ent.actualizar_db(curr,df)
conn.commit()

#agregamos la info a RS
ent.agregar_a_rs(curr, new_df)
conn.commit()

#realizamos un select para confirmar que se cargo la informacion
curr.execute("SELECT * FROM josafat_math_coderhouse.clima_cdmx")
print(curr.fetchall())

