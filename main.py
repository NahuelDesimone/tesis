import numpy as np
import pandas as pd
import sqlalchemy as db

SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:12345678@localhost/DB_Futbol_Tesis'
engine = db.create_engine(SQLALCHEMY_DATABASE_URI)
connection = engine.connect()

dataset_jugadores = pd.read_sql("SELECT * from Jugadores", con=connection)

print(dataset_jugadores)