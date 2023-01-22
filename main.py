import numpy as np
import pandas as pd
import sqlalchemy as db
from sklearn.preprocessing import StandardScaler


SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:12345678@localhost/DB_Futbol_Tesis'
engine = db.create_engine(SQLALCHEMY_DATABASE_URI)
connection = engine.connect()

dataset_jugadores = pd.read_sql("SELECT * from Jugadores", con=connection)
df_jugadores = dataset_jugadores[["jugador_id","posicion"]]

dataset_partidos_jugador = pd.read_sql("SELECT * from PartidosJugador", con=connection)
columnas_no_incluidas = ['partido_jugador_id','jugador_id','equipo_id','partido','competicion','fecha','posicion','minutos_jugados']
df_partidos_jugador = dataset_partidos_jugador.drop(columnas_no_incluidas,axis=1)

lista_por_columna = list()
for columna in df_partidos_jugador.columns:
    for i in df_jugadores.index:
        sql_query = "select avg(" + columna + ") from PartidosJugador where jugador_id = " + str(df_jugadores.loc[i]["jugador_id"])
        result = connection.execute(sql_query).fetchone()[0]
        lista_por_columna.append(result)
    

    df_jugadores.insert(len(df_jugadores.columns), columna, lista_por_columna)
    lista_por_columna.clear()


std_scaler = StandardScaler()
df_estadisticas = df_jugadores.iloc[:, 2:] #Del dataset utilizo como variables a analizar desde la columna acciones_totales en adelante
df_jugadores_normalizado = std_scaler.fit_transform(df_estadisticas.values)
df_jugadores_normalizado = pd.DataFrame(df_jugadores_normalizado, columns=df_estadisticas.columns)
df_jugadores_normalizado
