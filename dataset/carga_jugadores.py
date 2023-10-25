from calendar import c
from tkinter.messagebox import NO
import numpy
import pandas
import mysql.connector
from datetime import datetime
from os import scandir

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="12345678",
    database="tesis_dataset"
)


def crearTablas():

    try:
        dbCursor = mydb.cursor()

        sql_crear_tabla_equipos = """
        CREATE TABLE Equipos (
        equipo_id int NOT NULL AUTO_INCREMENT,
        nombre varchar(255) NOT NULL,
        pais varchar(255) NOT NULL,
        provincia varchar(255) NOT NULL,
        ciudad varchar(255) NOT NULL,
        PRIMARY KEY (equipo_id)
        );
        """
        sql_crear_tabla_jugadores = """
        CREATE TABLE Jugadores (
        jugador_id int NOT NULL AUTO_INCREMENT,
        nombre varchar(255) NOT NULL,
        posicion varchar(255) NOT NULL,
        fecha_nacimiento datetime NOT NULL,
        nacionalidad varchar(255) NOT NULL,
        estatura int,
        peso int,
        equipo_id int NOT NULL,
        PRIMARY KEY (jugador_id),
        FOREIGN KEY (equipo_id) REFERENCES Equipos(equipo_id)
        );
        """

        sql_crear_tabla_partido_equipo = """
        CREATE TABLE PartidosEquipo (
        partido_equipo_id int NOT NULL AUTO_INCREMENT,
        equipo_id int NOT NULL,
        fecha datetime NOT NULL,
        partido varchar(255) NOT NULL,
        competicion varchar(255) NOT NULL,
        duracion int NOT NULL,
        esquema varchar(255) NOT NULL,
        goles int,
        xG float NOT NULL,
        tiros_totales int,
        tiros_a_la_porteria int,
        efectividad_de_tiros float,
        pases_totales int,
        pases_logrados int,
        efectividad_de_pases float,
        posesion_del_balon float,
        balones_perdidos_totales int,
        balones_perdidos_bajos int,
        balones_perdidos_medios int,
        balones_perdidos_altos int,
        balones_recuperados_totales int,
        balones_recuperados_bajos int,
        balones_recuperados_medios int,
        balones_recuperados_altos int,
        duelos_totales int,
        duelos_ganados int,
        efectividad_de_duelos float,
        tiros_de_fuera_del_area_totales int,
        tiros_de_fuera_del_area_a_porteria int,
        efectividad_de_tiros_fuera_del_area float,
        ataques_posicionales_totales int,
        ataques_posicionales_con_remate int,
        efectividad_de_ataques_posicionales float,
        contraataques_totales int,
        contraataques_con_remate int,
        efectividad_de_contraataques float,
        jugadas_a_balon_parado_totales int,
        jugadas_a_balon_parado_con_remate int,
        efectividad_de_jugadas_a_balon_parado float,
        corneres_totales int,
        corneres_con_remate int,
        efectividad_de_corneres float,
        tiros_libres_totales int,
        tiros_libres_con_remate int,
        efectividad_de_tiros_libres float,
        penaltis_totales int,
        penaltis_marcados int,
        efectividad_en_penaltis float,
        centros_totales int,
        centros_precisos int,
        efectividad_en_centros float,
        pases_cruzados_en_profundidad_completados int,
        pases_en_profundidad_completados int,
        entradas_al_area_de_penalti_totales int,
        entradas_al_area_de_panalti_en_carrera int,
        entradas_al_area_de_panalti_con_pases_cruzados int,
        toques_en_el_area_de_penalti int,
        duelos_ofensivos_totales int,
        duelos_ofensivos_ganados int,
        efectividad_duelos_ofensivos float,
        fuera_de_juego int,
        goles_recibidos int,
        tiros_en_contra_totales int,
        tiros_en_contra_a_la_porteria int,
        porcentaje_de_tiros_en_contra float,
        duelos_defensivos_totales int,
        duelos_defensivos_ganados int,
        efectividad_de_duelos_defensivos float,
        duelos_aereos_totales int,
        duelos_aereos_ganados int,
        efectividad_de_duelos_aereos float,
        entradas_a_ras_de_suelo_totales int,
        entradas_a_ras_de_suelo_logradas int,
        efectividad_de_entradas_a_ras_de_suelo float,
        interceptaciones int,
        despejes int,
        faltas int,
        tarjetas_amarillas int,
        tarjetas_rojas int,
        pases_hacia_adelante_totales int,
        pases_hacia_adelante_logrados int,
        efectividad_de_pases_hacia_adelante float,
        pases_hacia_atras_totales int,
        pases_hacia_atras_logrados int,
        efectividad_de_pases_hacia_atras float,
        pases_laterales_totales int,
        pases_laterales_logrados int,
        efectividad_de_pases_laterales float,
        pases_largos_totales int,
        pases_largos_logrados int,
        efectividad_de_pases_largos float,
        pases_en_el_ultimo_tercio_totales int,
        pases_en_el_ultimo_tercio_logrados int,
        efectividad_de_pases_en_el_ultimo_tercio float,
        pases_progresivos_totales int,
        pases_progresivos_precisos int,
        efectividad_de_pases_progresivos float,
        desmarques_totales int,
        desmarques_logrados int,
        efectividad_de_desmarques float,
        saques_laterales_totales int,
        saques_laterales_logrados int,
        efectividad_de_saques_laterales float,
        saques_de_meta int,
        intensidad_de_paso float,
        promedio_pases_por_posesion_del_balon float,
        efectividad_de_lanzamiento_largo float,
        distancia_media_de_tiro float,
        longitud_media_pases float,
        ppda float,
        PRIMARY KEY (partido_equipo_id),
        FOREIGN KEY (equipo_id) REFERENCES Equipos(equipo_id)
        )
        """

        sql_crear_tabla_partido_jugador = """
        CREATE TABLE PartidosJugador (
        partido_jugador_id int NOT NULL AUTO_INCREMENT,
        jugador_id int NOT NULL,
        partido varchar(255),
        competicion varchar(255),
        fecha datetime,
        posicion varchar(255),
        minutos_jugados int,
        acciones_totales int,
        acciones_logradas int,
        goles int,
        asistencias int,
        tiros_totales int,
        tiros_logrados int,
        xG float,
        pases_totales int,
        pases_logrados int,
        pases_largos int,
        pases_largos_logrados int,
        centros_totales int,
        centros_precisos int,
        regates_totales int,
        regates_logrados int,
        duelos_totales int,
        duelos_ganados int,
        duelos_aereos_totales int,
        duelos_aereos_ganados int,
        interceptaciones int,
        balones_perdidos_totales int,
        balones_perdidos_propia_mitad int,
        balones_recuperados_totales int,
        balones_recuperados_mitad_adversario int,
        tarjeta_amarilla int,
        tarjeta_roja int,
        duelos_defensivos_totales int,
        duelos_defensivos_ganados int,
        duelos_por_balon_perdido_totales int,
        duelos_por_balon_perdido_ganados int,
        entradas_al_ras_del_suelo_totales int,
        entradas_al_ras_del_suelo_logradas int,
        despejes int,
        faltas int,
        tarjetas_amarillas int,
        tarjetas_rojas int,
        asistencias_a_tiro int,
        duelos_ofensivos_totales int,
        duelos_ofensivos_ganados int,
        toques_en_el_area_de_penalti int,
        fuera_de_juego int,
        carreras_en_profundidad int,
        faltas_recibidas int,
        pases_en_profundidad_totales int,
        pases_en_profundidad_logrados int,
        xA float,
        segundas_asistencias int,
        pases_en_el_ultimo_tercio_totales int,
        pases_en_el_ultimo_tercio_logrados int,
        pases_hacia_el_area_de_penalti_totales int,
        pases_en_el_area_de_penalti_precisos int,
        pases_recibidos int,
        pases_hacia_adelante_totales int,
        pases_hacia_adelante_logrados int,
        pases_hacia_atras_totales int,
        pases_hacia_atras_logrados int,
        goles_recibidos int,
        xCG float,
        remates_en_contra int,
        paradas_totales int,
        paradas_de_relejos int,
        salidas int,
        cesion_al_arquero_totales int,
        cesion_al_arquero_logradas int,
        saques_de_meta int,
        saques_de_meta_cortos int,
        saques_de_meta_largos int,
        PRIMARY KEY (partido_jugador_id),
        FOREIGN KEY (jugador_id) REFERENCES Jugadores(jugador_id)
        );
        """
        dbCursor.execute("drop table if exists PartidosJugador")
        dbCursor.execute("drop table if exists PartidosEquipo")
        dbCursor.execute("drop table if exists Jugadores")
        dbCursor.execute("drop table if exists Equipos")
        dbCursor.execute(sql_crear_tabla_equipos)
        dbCursor.execute(sql_crear_tabla_jugadores)
        dbCursor.execute(sql_crear_tabla_partido_equipo)
        dbCursor.execute(sql_crear_tabla_partido_jugador)
        mydb.commit()
        print("Se crearon las tablas correctamente")

    except Exception as e:
        print("Error al crear las tablas")


def cargarEquipos(datasetEquipos):

    data = pandas.read_csv(datasetEquipos, sep=";", encoding='utf-8')

    dbCursor = mydb.cursor()

    sql_cargar_equipos = "INSERT INTO Equipos (nombre, pais, provincia, ciudad) VALUES (%s,%s,%s,%s)"

    try:
        dic_rows = {}
        for indice in data.index:
            for columna in data.columns:
                dic_rows.update({columna: data.loc[indice, columna]})

            values_to_add = tuple(dic_rows.values())
            dbCursor.execute(sql_cargar_equipos, values_to_add)
            mydb.commit()
            dic_rows.clear()

        print("Se cargaron los equipos correctamente en la base de datos")

    except Exception as err:
        print("Error: {0}".format(err))


def cargarJugadores(carpetaJugadores):

    try:

        for archivoJugador in scandir(carpetaJugadores):

            data = pandas.read_csv(archivoJugador.path,
                                   sep=";", encoding='utf-8')
            
            print(archivoJugador.path)

            dbCursor = mydb.cursor()

            sql_cargar_jugadores = "INSERT INTO Jugadores (nombre, posicion, fecha_nacimiento, nacionalidad, estatura, peso, equipo_id) VALUES (%s,%s,%s,%s,%s,%s,%s)"

            dic_rows = {}
            for indice in data.index:
                for columna in data.columns:
                    dic_rows.update({columna: data.loc[indice, columna]})
                team_name = dic_rows['equipo_actual']
                sql = "SELECT equipo_id from Equipos where nombre = '" + team_name + "'"
                dbCursor.execute(sql)
                team_id = None
                for result in dbCursor:
                    team_id = result[0]
                dic_rows['equipo_actual'] = team_id
                fecha_nacimiento_dt = datetime.strptime(
                    dic_rows['fecha_nacimiento'], '%d/%m/%Y')
                dic_rows['fecha_nacimiento'] = fecha_nacimiento_dt.date()
                for column in dic_rows.keys():
                    if(type(dic_rows[column]) == numpy.int64):
                        dic_rows[column] = numpy.int64(
                            dic_rows[column]).item()

                values_to_add = tuple(dic_rows.values())
                dbCursor.execute(sql_cargar_jugadores, values_to_add)
                mydb.commit()
                dic_rows.clear()

            print("Se cargaron los jugadores de {0} correctamente en la base de datos".format(
                team_name))

    except Exception as err:
        print("Error: {0}".format(err))


def cargarPartidoEquipo(datasetPartidoEquipo):

    try:

        data = pandas.read_csv(datasetPartidoEquipo, sep=";", encoding='utf-8')

        dbCursor = mydb.cursor()

        sql_cargar_partidos_equipo = """INSERT INTO PartidosEquipo (
        equipo_id,
        fecha,
        partido,
        competicion,
        duracion,
        esquema,
        goles,
        xG,
        tiros_totales,
        tiros_a_la_porteria,
        efectividad_de_tiros,
        pases_totales,
        pases_logrados,
        efectividad_de_pases,
        posesion_del_balon,
        balones_perdidos_totales,
        balones_perdidos_bajos,
        balones_perdidos_medios,
        balones_perdidos_altos,
        balones_recuperados_totales,
        balones_recuperados_bajos,
        balones_recuperados_medios,
        balones_recuperados_altos,
        duelos_totales,
        duelos_ganados,
        efectividad_de_duelos,
        tiros_de_fuera_del_area_totales,
        tiros_de_fuera_del_area_a_porteria,
        efectividad_de_tiros_fuera_del_area,
        ataques_posicionales_totales,
        ataques_posicionales_con_remate,
        efectividad_de_ataques_posicionales,
        contraataques_totales,
        contraataques_con_remate,
        efectividad_de_contraataques,
        jugadas_a_balon_parado_totales,
        jugadas_a_balon_parado_con_remate,
        efectividad_de_jugadas_a_balon_parado,
        corneres_totales,
        corneres_con_remate,
        efectividad_de_corneres,
        tiros_libres_totales,
        tiros_libres_con_remate,
        efectividad_de_tiros_libres,
        penaltis_totales,
        penaltis_marcados,
        efectividad_en_penaltis,
        centros_totales,
        centros_precisos,
        efectividad_en_centros,
        pases_cruzados_en_profundidad_completados,
        pases_en_profundidad_completados,
        entradas_al_area_de_penalti_totales,
        entradas_al_area_de_panalti_en_carrera,
        entradas_al_area_de_panalti_con_pases_cruzados,
        toques_en_el_area_de_penalti,
        duelos_ofensivos_totales,
        duelos_ofensivos_ganados,
        efectividad_duelos_ofensivos,
        fuera_de_juego,
        goles_recibidos,
        tiros_en_contra_totales,
        tiros_en_contra_a_la_porteria,
        porcentaje_de_tiros_en_contra,
        duelos_defensivos_totales,
        duelos_defensivos_ganados,
        efectividad_de_duelos_defensivos,
        duelos_aereos_totales,
        duelos_aereos_ganados,
        efectividad_de_duelos_aereos,
        entradas_a_ras_de_suelo_totales,
        entradas_a_ras_de_suelo_logradas,
        efectividad_de_entradas_a_ras_de_suelo,
        interceptaciones,
        despejes,
        faltas,
        tarjetas_amarillas,
        tarjetas_rojas,
        pases_hacia_adelante_totales,
        pases_hacia_adelante_logrados,
        efectividad_de_pases_hacia_adelante,
        pases_hacia_atras_totales,
        pases_hacia_atras_logrados,
        efectividad_de_pases_hacia_atras,
        pases_laterales_totales,
        pases_laterales_logrados,
        efectividad_de_pases_laterales,
        pases_largos_totales,
        pases_largos_logrados,
        efectividad_de_pases_largos,
        pases_en_el_ultimo_tercio_totales,
        pases_en_el_ultimo_tercio_logrados,
        efectividad_de_pases_en_el_ultimo_tercio,
        pases_progresivos_totales,
        pases_progresivos_precisos,
        efectividad_de_pases_progresivos,
        desmarques_totales,
        desmarques_logrados,
        efectividad_de_desmarques,
        saques_laterales_totales,
        saques_laterales_logrados,
        efectividad_de_saques_laterales,
        saques_de_meta,
        intensidad_de_paso,
        promedio_pases_por_posesion_del_balon,
        efectividad_de_lanzamiento_largo,
        distancia_media_de_tiro,
        longitud_media_pases,
        ppda
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        dic_rows = {}

        for indice in data.index:
            for columna in data.columns:
                dic_rows.update({columna: data.loc[indice, columna]})
                if (type(dic_rows[columna]) == numpy.int64):
                    dic_rows[columna] = numpy.int64(
                        dic_rows[columna]).item()
                else:
                    if (type(dic_rows[columna] == str) and (columna != "Equipo") and (len(dic_rows[columna]) <= 5)):
                        dic_rows[columna] = float(
                            dic_rows[columna].replace(",", "."))
            fecha_partido_dt = datetime.strptime(
                dic_rows['Fecha'], '%d/%m/%Y')
            dic_rows['Fecha'] = fecha_partido_dt.date()
            team_name = dic_rows['Equipo']
            sql = "SELECT equipo_id from Equipos where nombre = '" + team_name + "'"
            dbCursor.execute(sql)
            team_id = None
            for result in dbCursor:
                team_id = result[0]
            dic_rows['Equipo'] = team_id
            values_to_add = tuple(dic_rows.values())
            dbCursor.execute(sql_cargar_partidos_equipo, values_to_add)
            mydb.commit()
            dic_rows.clear()

        print("Se cargaron los Partidos Equipo correctamente en la base de datos")

    except Exception as err:
        print("Error: {0}".format(err))


def cargarPartidoJugador(datasetPartidoJugador):

    try:

        for archivo_partido_jugador in scandir(datasetPartidoJugador):

            data = pandas.read_csv(archivo_partido_jugador.path, sep=";", encoding='utf-8')

            dbCursor = mydb.cursor()

            sql_cargar_partido_jugador = """INSERT INTO PartidosJugador (
            jugador_id,
            partido,
            competicion,
            fecha,
            posicion,
            minutos_jugados,
            acciones_totales,
            acciones_logradas,
            goles,
            asistencias,
            tiros_totales,
            tiros_logrados,
            xG,
            pases_totales,
            pases_logrados,
            pases_largos,
            pases_largos_logrados,
            centros_totales,
            centros_precisos,
            regates_totales,
            regates_logrados,
            duelos_totales,
            duelos_ganados,
            duelos_aereos_totales,
            duelos_aereos_ganados,
            interceptaciones,
            balones_perdidos_totales,
            balones_perdidos_propia_mitad,
            balones_recuperados_totales,
            balones_recuperados_mitad_adversario,
            tarjeta_amarilla,
            tarjeta_roja,
            duelos_defensivos_totales,
            duelos_defensivos_ganados,
            duelos_por_balon_perdido_totales,
            duelos_por_balon_perdido_ganados,
            entradas_al_ras_del_suelo_totales,
            entradas_al_ras_del_suelo_logradas,
            despejes,
            faltas,
            tarjetas_amarillas,
            tarjetas_rojas,
            asistencias_a_tiro,
            duelos_ofensivos_totales,
            duelos_ofensivos_ganados,
            toques_en_el_area_de_penalti,
            fuera_de_juego,
            carreras_en_profundidad,
            faltas_recibidas,
            pases_en_profundidad_totales,
            pases_en_profundidad_logrados,
            xA,
            segundas_asistencias,
            pases_en_el_ultimo_tercio_totales,
            pases_en_el_ultimo_tercio_logrados,
            pases_hacia_el_area_de_penalti_totales,
            pases_en_el_area_de_penalti_precisos,
            pases_recibidos,
            pases_hacia_adelante_totales,
            pases_hacia_adelante_logrados,
            pases_hacia_atras_totales,
            pases_hacia_atras_logrados,
            goles_recibidos,
            xCG,
            remates_en_contra,
            paradas_totales,
            paradas_de_relejos,
            salidas,
            cesion_al_arquero_totales,
            cesion_al_arquero_logradas,
            saques_de_meta,
            saques_de_meta_cortos,
            saques_de_meta_largos
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s)"""

            dic_rows = {}

            print("Cargando datos en la tabla PartidosJugador del equipo {} .... ".format(data.loc[0,"Club actual"]))

            for indice in data.index:
                for columna in data.columns:
                    dic_rows.update({columna: data.loc[indice, columna]})
                    if (type(dic_rows[columna]) == numpy.int64):
                        dic_rows[columna] = numpy.int64(dic_rows[columna]).item()
                    else:
                        if (type(dic_rows[columna] == str) and (columna != "Posición específica") and (columna != "Club actual") and (len(dic_rows[columna]) <= 5)):
                            dic_rows[columna] = float(
                                dic_rows[columna].replace(",", "."))

                fecha_partido_dt = datetime.strptime(dic_rows['Date'], '%d/%m/%y')
                dic_rows['Date'] = fecha_partido_dt.date()
                team_name = dic_rows['Club actual']
                sql_obtener_id_equipo = "SELECT equipo_id from Equipos where nombre = '" + team_name + "'"
                dbCursor.execute(sql_obtener_id_equipo)
                team_id = None
                for result in dbCursor:
                    team_id = result[0]
                #dic_rows['Club actual'] = team_id
                player_name = dic_rows["Jugador"]
                sql_obtener_id_jugador = "SELECT jugador_id from Jugadores where nombre = '" + \
                    player_name + "' and equipo_id = " + str(team_id)
                dbCursor.execute(sql_obtener_id_jugador)
                jugador_id = None
                for result in dbCursor:
                    jugador_id = result[0]
                dic_rows['Jugador'] = jugador_id
                del dic_rows["Club actual"]
                values_to_add = tuple(dic_rows.values())
                dbCursor.execute(sql_cargar_partido_jugador, values_to_add)
                mydb.commit()
                dic_rows.clear()
        

    except Exception as err:
        print("Error: {0}".format(err))

crearTablas()


datasetEquipos = "/Users/nahueldesimone/Library/Mobile Documents/com~apple~CloudDocs/Tesis/Dataset_V3/Equipos/Equipos.csv"
cargarEquipos(datasetEquipos)


datasetJugadores = "/Users/nahueldesimone/Library/Mobile Documents/com~apple~CloudDocs/Tesis/Dataset_V3/Jugadores"
cargarJugadores(datasetJugadores)

datasetPartidoEquipo = "/Users/nahueldesimone/Library/Mobile Documents/com~apple~CloudDocs/Tesis/Dataset_V3/Partido_Equipo/Partido_Equipo_Stats.csv"
cargarPartidoEquipo(datasetPartidoEquipo)

datasetPartidoJugador = "/Users/nahueldesimone/Downloads/Partidos_Equipo"
cargarPartidoJugador(datasetPartidoJugador)