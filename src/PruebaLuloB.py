from itertools import count
from pickle import FALSE
import profile
import sqlite3
from typing import Set
import requests as rq
import pandas as pd
from pandas_profiling import ProfileReport
import sqlite3
from sqlite3 import Error
import datetime

#Creando la estructura de los dataframe y Variables
DfTransmission = pd.DataFrame(columns=["id Transmission","url","Nombre","Temporada","Numero",
                                        "Tipo_Transmission","Tiempo al aire","airstamp",
                                        "Tiempo de ejecucion_Transmission","Puntuacion media"])

DfShow = pd.DataFrame(columns=["id_show","id_transmision","Nombre Show","Tipo Show","Lenguaje","Genero",
                                "Estado","Tiempo de ejecucion _Show","Tiempo ejecuccion Promedio",
                                "Horario"])

DfChannel = pd.DataFrame(columns=["id_webChannel","id_show","id_transmision","Nombre Canal",
                                "Pais del Canal"])

SqliteNameFile= "../db/PruebaLulo.sqlite3"
t = 0
w = 0
s = 0
#------------------------------------------------------------------------------------

def Conexion(Db_file,Df,Nombre):
    try:
        con=sqlite3.connect(Db_file)
        Df.to_sql(Nombre,con,if_exists="replace")
        print("Conexion_Creada")

    except Error as ex:
        print(ex) 

    finally:
        if con:
            con.close()

def getfechas():
    fechas = []    
    x = datetime.datetime(2020, 12, 1)
    fechas.append(x.strftime("%Y-%m-%d"))

    while x.month == 12 and x.year== 2020 and x.day<31:
        x += datetime.timedelta(days=1)
        fechas.append(x.strftime("%Y-%m-%d"))
    
    
    return fechas 

def getJson(date):
    # Consumiendo la api del mes de diciembre del año 2022
    url = "http://api.tvmaze.com/schedule/web?date=" + date    

    jsonData = {}
    datos = rq.get(url)
    if datos.status_code == 200:
        
        res = datos.json()

        with open("../json/request_" + date + ".json", "w",encoding="utf-8") as f:
            f.write(datos.text)

        return res
    else:
        return {}

def parseData(datos):            
    
    global t
    global w
    global s

    #Transformando el json en los dataframe 
    for i in range(0, len(datos)) :

        item=datos[i] 

        DfTransmission.loc[t]=[datos[i]["id"],\
                     datos[i]["url"],\
                     datos[i]["name"],\
                     datos[i]["season"],\
                     datos[i]["number"],\
                     datos[i]["type"],\
                     datos[i]["airdate"],\
                     datos[i]["airstamp"],\
                     datos[i]["runtime"],\
                     datos[i]["rating"]["average"]]

        t = t + 1   

        DfShow.loc[s]=[datos[i]["_embedded"]["show"]["id"],
                     datos[i]["id"],
                     datos[i]["_embedded"]["show"]["name"],
                     datos[i]["_embedded"]["show"]["type"],
                     datos[i]["_embedded"]["show"]["language"],
                     datos[i]["_embedded"]["show"]["genres"],
                     datos[i]["_embedded"]["show"]["status"],
                     datos[i]["_embedded"]["show"]["runtime"],
                     datos[i]["_embedded"]["show"]["averageRuntime"],
                     datos[i]["_embedded"]["show"]["schedule"]["days"]]
        s = s + 1    

        if(datos[i]["_embedded"]["show"]["webChannel"] is not None) :

            id_webchannel = datos[i]["_embedded"]["show"]["webChannel"]["id"]
            id_show = datos[i]["_embedded"]["show"]["id"]
            id_transmision = datos[i]["id"]
            name = datos[i]["_embedded"]["show"]["webChannel"]["name"]
            country = datos[i]["_embedded"]["show"]["webChannel"]["country"]
            if(country is None):
                country = ''
            else:
                country = datos[i]["_embedded"]["show"]["webChannel"]["country"]["name"]
            
            DfChannel.loc[w]=[id_webchannel,id_show,id_transmision, name, country]
            w = w + 1
              
def profiling(df,nombre):
    # Realizando los profiling de los dataframe
    prof=ProfileReport(df)
    prof.to_file(output_file=nombre)

def run():
    
    fechas = getfechas()
    for fecha in fechas:        
        json = getJson(fecha)
        parseData(json)

    #Limpiando DfShow   ---------------------------------------------------------
    DfTemp = DfShow['Genero'].tolist()
    Df=pd.DataFrame(DfTemp)
    Df.columns = ["Genero_1","Genero_2","Genero_3","Genero_4"]
    DfTemp2= pd.concat([DfShow,Df],axis=1,sort=False)

    DfAux = DfShow['Horario'].tolist()
    Df1=pd.DataFrame(DfAux)
    Df1.columns = ["dia_1","dia_2","dia_3","dia_4","dia_5","dia_6","dia_7"]
    DfAux2= pd.concat([DfTemp2,Df1],axis=1,sort=False)

    DfShowResult = DfAux2.drop(['Genero','Horario'],axis=1)
    #Limpiando DfShow   ---------------------------------------------------------


    #Creando y exportando los Profiling
    profiling(DfTransmission,"../profiling/output-DfTransmission.html")
    profiling(DfShowResult,"../profiling/output-DfShowResult.html")
    profiling(DfChannel,"../profiling/output-DfChannel.html")

    #Guardando los DataFrame en La BD
    Conexion(SqliteNameFile,DfTransmission,"DfTransmission")
    Conexion(SqliteNameFile,DfChannel,"DfChannel")    
    Conexion(SqliteNameFile,DfShowResult,"DfShow")

if __name__ == "__main__":
    run()