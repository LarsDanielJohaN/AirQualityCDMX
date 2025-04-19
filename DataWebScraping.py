import requests as req
from bs4 import BeautifulSoup
import polars as pl
import numpy as np
import time


"""
The following method "scrape_aire_cdmx" returns a polars data frame with particle concentration data of a particular type (
maximum ('MAXIMOS'), minimum ('MINIMOS') or avarage ('HORARIOS') recorded value) per hour (taking values 1,..., 24), for a given year (advailable from 2005-2024) and month (coded '01', ..., '12'). Data 
is organized by recording station and values of which data was not recorded are identified by null. 

The variables of input are:
Type of concentration: qtipo \in {HORARIOS, 'MAXIMOS', 'MINIMOS'}  ('HORARIOS' is set as default)

Type of particle:  parametro \in {'so2', 'co', 'nox', 'no2', 'no', 'o3', 'o3_8h', 'pm10', 'pm10nc', 'pm2', 'pm2nc', 'wsp', 'wdr', 'tmp', 'rh'  }
the nc variables stand for Now Cast. 

('so2' is set as default)
Year: anio \in {'2005', ..., '2024'} ('2023' set as default)

Month: qmes  \in {'01',... '12' } ('01' January is default)
number corresponds to order of ocurrance in the year. 
qmes is only advailable for Promedios Horarios, in other case qmes should be set to ''
http://www.aire.cdmx.gob.mx/estadisticas-consultas/concentraciones/respuesta.php?qtipo=MAXIMOS&parametro=co&anio=2008&qmes=
"""


def scrape_aire_cdmx(qtipo = 'HORARIOS', parametro = 'so2', anio = '2023', qmes = '01'):

    #Creates general link for consultancy in www.aire.cdmx.gob.mx on the estadisticas-consultas cite. 
    est_cons_link = f"http://www.aire.cdmx.gob.mx/estadisticas-consultas/concentraciones/respuesta.php?qtipo={qtipo}&parametro={parametro}&anio={anio}&qmes={qmes}"
    r_est_cons = req.get(est_cons_link) #Makes request to page. 
   
    soup_est_cons = BeautifulSoup(r_est_cons.content) #Process page code with Beautiful Soup. 
    titls = soup_est_cons.findAll('td', attrs= {'class':"textoscentradosnegrobold"}) #td´s with "textoscentradosnegrobold" contain column names. 
    titls = list(titls) #Stores td´s for further processing. This contains repeated values. 

    i = 0 #Creates auxliary counter. 
    aset = set() #Auxiliary set. 
    labls = {} #Auxiliary dictionary, later to be converted to Polars data frame. 

    #The following gets table labels. 
    while (titls[i] in aset) == False: #While the data checked isnt an already repeated value. 
        tag = titls[i].find('b').text #Obtains all b tags. 
        aset.add(titls[i]) 
        labls[tag[4:7]] = [np.nan] #Creates auxiliary value to aux (this to make the column type to string)
        i+=1
    df_f= pl.DataFrame(labls) #Creates Polars Data Frame to store all data. 

    if qtipo == 'HORARIOS': #Changes first two column names for Fecha (date) and Hora (time). 
        a = {df_f.columns[0]:'Fecha', df_f.columns[1]:'Hora'}
    else:
        a = {df_f.columns[0]:'Fecha'}
    df_f = df_f.rename(a) #Changes first two column names for Fecha (date) and Hora (time). 
    df_f = df_f.with_columns(df_f['Fecha'].cast(pl.String)) #For convinience, changes column types of Fecha to String

    labls = df_f.columns #Stores column names for further use. 
    trs = soup_est_cons.findAll('tr') #<tr>´s contain the data column names and the data itself. 

    j = 0
    for tr in trs: #Goes within web page to store data. 
        tds = tr.find_all('td', attrs = {'class':"textoscentradosrecursostecnicos"}) #textoscentradosrecursostecnicos has the individual cells of data. 
        
        if len(tds) != 0: #If tds actually contains the requiered data, store it. 
            arr_td = [td.get_text() for td in tds] #Stores the data obtained from each of the <td>´s 
            n_dat = pl.DataFrame({labls[r]:[val_check(arr_td[r]  )] for r in range(0,i)}, nan_to_null=True) #Converts the obtained data to a Polars data frame (to be added to the rest)
            df_f.extend(n_dat) #Adds obtained data to the rest. 
        j+=1

    df_f = df_f.filter(df_f['Fecha'] != 'NaN') #Eliminates auxiliary aux row. 
    return df_f



def scrape_estaciones(timeProt = 10): #Gets information on individual stations. 
    outer_link = 'http://www.aire.cdmx.gob.mx/default.php?opc=%27ZaBhnmI=&dc=%27ZA==' #Link of general information about stations. 
    req_outer = req.get(outer_link) #Makes request to obtain page on outer_link. 
    soup_out_req = BeautifulSoup(req_outer.content)#Process page code with Beautiful Soup. 

    gen_table = soup_out_req.findAll('th', attrs={'class':'encabezado-fila'}) #Obtains table with individual station information (including links for more detailed information)
    est_links = [] #Creates list to store links. 

    for th in gen_table: #Iterates through all stations info to obtain their particular information links. 
        est_links.append(th.a.get('href'))
    time.sleep(timeProt) #Waits timeProt for scrapping protection. 


    cols = [pl.Series('Estacion', [], dtype = pl.String),  pl.Series('Nombre', [], dtype = pl.String), pl.Series('Direccion', [], dtype = pl.String), pl.Series('Edo', [], dtype = pl.String),
            pl.Series('Latitud', [], dtype = pl.String), pl.Series('Longitud', [], dtype = pl.String), pl.Series('RO3', [], dtype = pl.String)  , pl.Series('RCO', [], dtype = pl.String) ,
            pl.Series('RSO2', [], dtype = pl.String) , pl.Series('RNO2', [], dtype = pl.String) , pl.Series('RPM10', [], dtype = pl.String)   , pl.Series('RPM2.5', [], dtype = pl.String)      ]
    
    data_final = pl.DataFrame(cols)
    for link in est_links:
        data_final.extend(handl_estacion(link)) #Adds estation data to final data. 
        time.sleep(timeProt) #Waits timeProt for scrapping protection. 

    return data_final

    
     




def handl_estacion(link_est): #Takes information about individual stations. 
    print(link_est)
    req_est = req.get(link_est) #Makes request for station. 
    soup_est = BeautifulSoup(req_est.content) #Process page code with Beautiful Soup. 
    get_tables = soup_est.findAll('table', attrs = {'class':'entornos-info'}) #Finds tables with all information. 
    cols = ['Estacion', 'Nombre', 'Direccion', 'Edo', 'Latitud', 'Longitud', 'RO3', 'RCO', 'RSO2', 'RNO2', 'RPM10', 'RPM2.5']

    t1 = get_tables[0].findAll('td') #Finds 'td'´s (these have all information. )
    t2 = get_tables[1].findAll('td')

    #The following explore subdivisions of the webpage which contain information. 
    rank_part = t1[3].table
    rank_part = rank_part.findAll('tr')
    rank_part = rank_part[1]
    rank_part = rank_part.findAll('td')
    
    #Obtains data from the different td´s. 
    raw = [t1[0].get_text(), t1[1].get_text(), t2[0].get_text(), t2[2].get_text(), t2[3].get_text(), t2[4].get_text() ] 

    for td in rank_part: #Adds data for representability. 
        raw.append(td.get_text()) 
    
    for i in range(0, len(raw)): #Takes out extra strings. 
        aux = raw[i].replace('\r', '')
        aux = aux.replace('\n', '')
        aux = aux.replace('\t', '')

        raw[i] = aux

    data = {cols[i]:[raw[i]] for i in range(0, len(raw))} #Takes all data into a dictionary. 
    return pl.DataFrame(data) #Returns data as a polars dataframe. 








def val_check(val):
    try:
        val = float(val)
        return val
    except:

        if str(val) == 'nr':
            return np.nan
        else:
            return val
    



