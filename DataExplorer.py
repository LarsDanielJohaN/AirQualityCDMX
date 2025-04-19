"""
Last edited date: Wed 29 of May, 2024. 
Author: Lars Daniel Johansson Niño
Purpose: This python script scrapes air quality data for Mexico city. 
This is done by downloading data (in .csv) directly from the www.aire.cdmx.com.mx/estadisticas-consultas/concentraciones
and making cell by cell comparisons with requests made by the method. 

https://sig.cdmx.gob.mx/datos/descarga
"""
import os
scrpt_path = os.path.abspath(__file__) #Gets absolute path of the current script. 
scrpt_dir = os.path.dirname(scrpt_path) # Get the directory name of the current script
os.chdir(scrpt_dir) #Chenges working direct  ory to the python file´s path. 



import polars as pl
import DataWebScraping as ws
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
from datetime import datetime
import numpy as np
import pandas as pd
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from shapely import Point 



parametro = 'co'
anio = '2023'
qtipo = 'HORARIOS'
unit_ran = 24
#file_horarios_part = f'{str(unit_ran)}_{anio}_{qtipo}_{parametro}.csv'#Gets file path. 
file_horarios_part = 'horarios_co.csv'

horarios_part = pl.read_csv(file_horarios_part, has_header = True) #Reads data obtained from DataGather1.py

horarios_part = horarios_part.with_columns(horarios_part['Fecha-Hora'].str.to_datetime(format = '%Y-%m-%dT%H:%M:%S.%f')) #Convert 'Fecha-Hora' back to date format. 
horarios_part = horarios_part.with_columns(horarios_part['Fecha'].str.to_date()) #Converts Fecha to date. 

horarios_part = horarios_part.sort(pl.col('Fecha-Hora')) #Sorts by date and time in ascending order. 

data_ex = horarios_part.select(horarios_part.columns[2:len(horarios_part.columns)-1])
data_ex = np.nanmean(data_ex.to_numpy(), axis = 1)
lines = np.linspace(0, len(data_ex), num = 13)

plt.suptitle('Blue: mean co concentration (all valid stations), Red: Approximate month divisors')
plt.plot(np.arange(len(data_ex)), data_ex)
plt.vlines(lines, ymin = 0, ymax = 2.0, colors= 'red')
plt.show()























"""
The following section performs some visual analysis of data in an specified time interval. 
This is accomplished by selecting those stations which have less than 100 missing values later to 
plot their raw evolution. Each station identified by a single element of the markers list. 
"""

"""
#The following section prepares data from January of 2023 to be analyzed in a functional context. In particular, data is prepared 
#as an R Data Set object. 

Observation: This is kept as a separate section as the actions done to data from this point might be different from the ones worked uppon previously. 




stations = pl.read_csv('StationInfo.csv', has_header= True)
print(stations.columns)
print("----------------------------------------------------------------------------------------")
part = 'RNO2'
print()

stations_part = stations.filter(pl.col(part) != '')
stations_part = stations_part.with_columns(stations_part['Latitud'].cast(pl.Float64))
stations_part = stations_part.with_columns(stations_part['Longitud'].cast(pl.Float64))


lat = stations_part['Latitud'].to_list()
lon = stations_part['Longitud'].to_list()

geometry = [Point( lon[i],lat[i]) for i in range(0, len(lat))]
stations_part = stations_part.with_columns(pl.Series('geometry', geometry))


cdmx = gpd.read_file('Data/MarcoGeoesta_Censo2020_INEGI/CDMX/conjunto_de_datos/09mun.shp')
cdmx = cdmx.to_crs("EPSG:4326")

edomex = gpd.read_file('Data/MarcoGeoesta_Censo2020_INEGI/EDOMEX/conjunto_de_datos/15mun.shp')
edomex = edomex.to_crs("EPSG:4326")


#
cdmx_edomex = pd.concat([cdmx, edomex])
#base = cdmx_edomex.plot(column = 'CVE_ENT')
color_map = {
    '09': '#c16767',    # Example color for CDMX
    '15': '#6c4675'    # Example color for EDOMEX
}

# Plot each municipality with the appropriate color
fig, ax = plt.subplots(1, 1, figsize=(10, 10))
for key, group in cdmx_edomex.groupby('CVE_ENT'):
    group.plot(ax=ax, color=color_map.get(key, 'gray'))  # Default to 'gray' if the key is not in the color_map




stations_part = gpd.GeoDataFrame(stations_part.to_pandas(), crs = 4326 )

print(stations_part.columns)
print(stations_part[part].unique())
print(stations_part[part])
stations_part_proj = stations_part.to_crs(epsg=32614)

stations_part_proj_4 = stations_part_proj[stations_part_proj[part] == '4']
stations_part_proj_4['geom_stations'] = stations_part_proj_4['geometry']
stations_part_proj_4['geometry'] = stations_part_proj_4.buffer(distance = 20000)
stations_part_proj_4 = stations_part_proj_4.to_crs(epsg=4326)
stations_part_proj_4.plot(ax=ax, edgecolor='red', facecolor='none')



stations_part_proj_3 = stations_part_proj[stations_part_proj[part] == '3']
stations_part_proj_3['geom_stations'] = stations_part_proj_3['geometry']

stations_part_proj_3['geometry'] = stations_part_proj_3.buffer(distance = 4000)
stations_part_proj_3 = stations_part_proj_3.to_crs(epsg=4326)
stations_part_proj_3.plot(ax=ax, edgecolor='lightblue', facecolor='none')

stations_part_proj_2 = stations_part_proj[stations_part_proj[part] == '2']
stations_part_proj_2['geom_stations'] = stations_part_proj_2['geometry']

stations_part_proj_2['geometry'] = stations_part_proj_2.buffer(distance = 1000)
stations_part_proj_2 = stations_part_proj_2.to_crs(epsg=4326)
stations_part_proj_2.plot(ax=ax, edgecolor='yellow', facecolor='none')





stations_part.plot(ax =ax, marker = 'X', color = '#FFFF00', markersize = 15)
print(stations_part)

for x, y, label in zip(stations_part.geometry.x, stations_part.geometry.y, stations_part['Estacion']):  # Assuming 'StationName' is the label column
    plt.text(x, y, label, fontsize=8, ha='right', color='white')

plt.show()




"""





"""
Some documentation and code to keep in mind:
polars.LazyFrame.filter
https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.filter.html#polars.LazyFrame.filter
list1 = [i if i % 2 == 0 else 0 for i in range(1, 11)]
https://docs.pola.rs/py-polars/html/reference/expressions/api/polars.Expr.str.to_datetime.html
https://matplotlib.org/stable/gallery/lines_bars_and_markers/bar_colors.html#sphx-glr-gallery-lines-bars-and-markers-bar-colors-py
https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.item.html
https://docs.pola.rs/user-guide/expressions/casting/#dates
l = auxDateData.select(   pl.col('Fecha-Hora').dt.to_string('%Y-%m-%d %H:%M:%S'))
https://docs.pola.rs/py-polars/html/reference/api/polars.concat.html

"""