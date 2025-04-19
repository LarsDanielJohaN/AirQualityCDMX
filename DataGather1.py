"""
Last edited date: Wed 29 of May, 2024. 
Author: Lars Daniel Johansson Niño
Purpose: This python script scrapes air quality data for Mexico city. 
This is done by downloading data (in .csv) directly from the www.aire.cdmx.com.mx/estadisticas-consultas/concentraciones
and making cell by cell comparisons with requests made by the method. 

Notes:
For 2023_HORARIOS_co.csv the following parameters were used, 
parametro = 'co'
anio = '2023'
qtipo = 'HORARIOS'

"""
import os
scrpt_path = os.path.abspath(__file__) #Gets absolute path of the current script. 
scrpt_dir = os.path.dirname(scrpt_path) # Get the directory name of the current script
os.chdir(scrpt_dir) #Chenges working direct  ory to the python file´s path. 

import polars as pl
import DataWebScraping as ws
import numpy as np
from datetime import datetime, timedelta
import time 



#station_info = ws.scrape_estaciones(timeProt = 5)
#station_info.write_csv('StationInfo.csv')
#print(station_info)


months = ['0'+str(i) if i<10 else str(i) for i in range(2,13)]

parametro = 'co'
anio = '2020'
qtipo = 'HORARIOS'
save_file = f'{anio}_{qtipo}_{parametro}.csv'


horarios_part = ws.scrape_aire_cdmx(qtipo = qtipo, parametro = parametro, anio =  anio , qmes = '01' )
row, col = horarios_part.shape

convert_hora  = [horarios_part.item(r,0) + ( ' 0'+str(int(horarios_part.item(r,1) -1 ) )+ ':00:00') if  horarios_part.item(r,1)<=10.0 else
                 horarios_part.item(r,0) + ( ' '+str(int(horarios_part.item(r,1) -1 ) )+ ':00:00')  for r in range(0,row)]
convert_hora = pl.Series('Fecha-Hora', [datetime.strptime(date, "%d-%m-%Y %H:%M:%S") for date in convert_hora])


horarios_part = horarios_part.with_columns( convert_hora  )


for month in months:
    horarios_part_curr = ws.scrape_aire_cdmx(qtipo = qtipo, parametro = parametro, anio =  anio , qmes = month )
    row, col = horarios_part_curr.shape

    convert_hora  = [horarios_part_curr.item(r,0) + ( ' 0'+str(int(horarios_part_curr.item(r,1) -1 ) )+ ':00:00') if  horarios_part_curr.item(r,1)<=10.0 else
                 horarios_part_curr.item(r,0) + ( ' '+str(int(horarios_part_curr.item(r,1) -1 ) )+ ':00:00')  for r in range(0,row)]
    convert_hora = pl.Series('Fecha-Hora', [datetime.strptime(date, "%d-%m-%Y %H:%M:%S") for date in convert_hora])
    
    horarios_part_curr = horarios_part_curr.with_columns(convert_hora)
    horarios_part = horarios_part.extend(horarios_part_curr)
    time.sleep(5)

print(horarios_part)
horarios_part.write_csv(save_file)













