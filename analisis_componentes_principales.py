import sqlalchemy as db
import pandas as pd
from sqlalchemy import sql
from pca import pca

def reducir_dimensionalidad_dataset():
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:12345678@localhost/db_tesis'
    engine = db.create_engine(SQLALCHEMY_DATABASE_URI)
    connection = engine.connect()

    dataset_jugadores = pd.read_sql("SELECT * from Jugadores", con=connection)
    df_jugadores = dataset_jugadores[["jugador_id","posicion"]]

    dataset_partidos_jugador = pd.read_sql("SELECT * from PartidosJugador", con=connection)
    columnas_no_incluidas = ['partido_jugador_id','jugador_id','partido','competicion','fecha','posicion','minutos_jugados']
    df_partidos_jugador = dataset_partidos_jugador.drop(columnas_no_incluidas,axis=1)

    lista_por_columna = list()
    for columna in df_partidos_jugador.columns:
        for i in df_jugadores.index:
            sql_query = "select avg(" + columna + ") from PartidosJugador where jugador_id = " + str(df_jugadores.loc[i]["jugador_id"])
            result = connection.execute(sql.text(sql_query)).fetchone()[0]
            lista_por_columna.append(float(result))
        

        df_jugadores.insert(len(df_jugadores.columns), columna, lista_por_columna)
        lista_por_columna.clear()
    
    df_estadisticas = df_jugadores.iloc[:, 2:]
    modelo = pca(n_components=11, normalize=True)
    modelo.fit_transform(df_estadisticas)
    df_reducido = modelo.transform(df_estadisticas)
    jugador_id = df_jugadores.iloc[:,0]
    posicion = df_jugadores.iloc[:,1]
    df_reducido.insert(0,"jugador_id",jugador_id)
    df_reducido.insert(1,"posicion",posicion)

    return df_reducido