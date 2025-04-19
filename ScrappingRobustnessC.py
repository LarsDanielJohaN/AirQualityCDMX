"""
Last edited date: Wed 29 of May, 2024. 
Author: Lars Daniel Johansson Niño
Purpose: This python script makes robustness checks for the scrape_aire_cdmx method on the DataWebScrapping file. 
This is done by downloading data (in .csv) directly from the www.aire.cdmx.com.mx/estadisticas-consultas/concentraciones
and making cell by cell comparisons with requests made by the method. 
"""
import os
scrpt_path = os.path.abspath(__file__) #Gets absolute path of the current script. 
scrpt_dir = os.path.dirname(scrpt_path) # Get the directory name of the current script
os.chdir(scrpt_dir) #Chenges working directory to the python file´s path. 

import polars as pl
import DataWebScraping as ws
import numpy as np


#Recieves two values and returns true if they are the same and false otherwise taking into account the expected data types, str and float. 
def check_vals(vala, valb):
    if isinstance(vala, str) and isinstance(valb, str): #In a given case in which both values are strings, make direct comparison. 
        return vala == valb
    elif isinstance(vala, float) and isinstance(valb,float): #If they are of type float. 
        return ( (vala == valb) or (np.isnan(vala) and np.isnan(valb)) ) #Return true if both values are the same (either as numbers or both being NaN)
    else: #In other case, vala is not valb, or they arent of the expected data type. In either case, it returns False. 
        return False 






#File for maximum NO concentration per day on 2017.
maximos_diarios_no_2017_page_file = f'MaximosDiariosNo2017.csv'
maximos_diarios_no_2017_page = pl.read_csv(maximos_diarios_no_2017_page_file, has_header = True, null_values = ['nr']) #Reads downloaded file, changing al nr values (indicating not recorded ) to null. 
maximos_diarios_no_2017_page = maximos_diarios_no_2017_page.drop('') #Deletes last column created as result of .csv file´s rows ending with ,

for col in maximos_diarios_no_2017_page.columns[1:len(maximos_diarios_no_2017_page)]:#Casts all columns but the date one to float type. 
    maximos_diarios_no_2017_page = maximos_diarios_no_2017_page.with_columns(maximos_diarios_no_2017_page[col].cast(pl.Float64))
maximos_diarios_no_2017_page = maximos_diarios_no_2017_page.fill_null(np.NaN)#Changes null values by NaN

maximos_diarios_no_2017_WS = ws.scrape_aire_cdmx(qtipo = 'MAXIMOS', parametro = 'no', anio = '2017', qmes = '')#Makes web scraping.


row,col = maximos_diarios_no_2017_page.shape

i = 0

print("Check, element by element, if values are equal. Maximos Diarios co for 2017. ")
while i<row:
    j = 0
    while j<col and  check_vals(maximos_diarios_no_2017_page.item(i,j), maximos_diarios_no_2017_WS.item(i,j)) == True:
        j+=1     
    i+=1
print("Done",i,j,row,col)

print(maximos_diarios_no_2017_WS)
print(maximos_diarios_no_2017_page)


#File for minimum CO concentration per day on 2008. 
minimos_diarios_co2_2008_page_file = f'MinimosDiariosCo2008.csv'
minimos_diarios_co2_2008_page = pl.read_csv( minimos_diarios_co2_2008_page_file, has_header = True, null_values = ['nr']) #Reads downloaded file, changing al nr values (indicating not recorded ) to null. 
minimos_diarios_co2_2008_page = minimos_diarios_co2_2008_page.drop('') #Deletes last column created as result of .csv file´s rows ending with ,

for col in minimos_diarios_co2_2008_page.columns[1:len(minimos_diarios_co2_2008_page.columns)]: #Casts all columns but the date one to float type. 
    minimos_diarios_co2_2008_page  = minimos_diarios_co2_2008_page.with_columns(minimos_diarios_co2_2008_page[col].cast(pl.Float64))
minimos_diarios_co2_2008_page  = minimos_diarios_co2_2008_page.fill_null(np.NaN) #Changes null values by NaN
minimos_diarios_co2_2008_WS = ws.scrape_aire_cdmx(qtipo = 'MINIMOS', parametro = 'co', anio = '2008', qmes = '') #Makes web scraping. 



row, col =minimos_diarios_co2_2008_page.shape
i = 0

print("Check, element by element, if values are equal. Minimos Diarios co for 2008. ")
while i<row:
    j = 0
    while j<col and  check_vals(minimos_diarios_co2_2008_page.item(i,j), minimos_diarios_co2_2008_WS.item(i,j)) == True:
        j+=1     
    i+=1
print("Done",i,j,row,col)

print(minimos_diarios_co2_2008_page)
print(minimos_diarios_co2_2008_WS)

#File for avarage so2 concentration data on January of 2023. 
promedio_horarios_so2_jan2023_page_file = f'PromedioHorariosSo2Jan2023.csv'
promedio_horarios_so2_jan2023_page = pl.read_csv(promedio_horarios_so2_jan2023_page_file, has_header=True, null_values = ['nr']) #Reads downloaded file, changing al nr values (indicating not recorded ) to null.  
promedio_horarios_so2_jan2023_page = promedio_horarios_so2_jan2023_page.drop('') #Deletes last column created as result of .csv file´s rows ending with ,


for col in promedio_horarios_so2_jan2023_page.columns[1:len(promedio_horarios_so2_jan2023_page)]: #Casts all columns but the date one to float type. 
    promedio_horarios_so2_jan2023_page = promedio_horarios_so2_jan2023_page.with_columns(promedio_horarios_so2_jan2023_page[col].cast(pl.Float64))
promedio_horarios_so2_jan2023_page = promedio_horarios_so2_jan2023_page.fill_null(np.NaN)#Changes null values by NaN
promedio_horarios_so2_jan2023_page_WS = ws.scrape_aire_cdmx()#Makes web scraping.


row, col =promedio_horarios_so2_jan2023_page.shape
i = 0

print(row,col)
print("Check, element by element, if values are equal. Promedios Horarios so2 January of 2023. ")
while i<row:
    j = 0
    while j<col and check_vals(promedio_horarios_so2_jan2023_page.item(i,j),  promedio_horarios_so2_jan2023_page_WS.item(i,j)) == True:
        j+=1
    i+=1
print("Done.  ",i,j,row,col)


print(promedio_horarios_so2_jan2023_page)
print(promedio_horarios_so2_jan2023_page_WS)









