
"""
Last edited date: Tue 18 of June, 2024. 
Author: Lars Daniel Johansson Niño
Purpose: This python script scrapes air quality data for Mexico city. 
This is done by downloading data (in .csv) directly from the www.aire.cdmx.com.mx/estadisticas-consultas/concentraciones
and making cell by cell comparisons with requests made by the method. 

Notes/Pending improuvements: 
Some possible efficiency improuvements could be to eliminate duplicate values with regards to dates. 
i.e. perform some data.unique(mantain_ord = True) from the very beggining. 

-For day_horarios_co_R.RData, parameters were:
    unit_delta = timedelta(days = 1) 
    unit_ran = 24 
    unit_n = 1
    low_delta = timedelta(days = 1)
    err_delta = timedelta(hours = 1)


"""
import os
scrpt_path = os.path.abspath(__file__) #Gets absolute path of the current script. 
scrpt_dir = os.path.dirname(scrpt_path) # Get the directory name of the current script
os.chdir(scrpt_dir) #Chenges working direct  ory to the python file´s path. 
r_home = os.environ.get('R_HOME') #Sets R_HOME

import polars as pl
import numpy as np
from datetime import datetime, timedelta
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri


def process_missing(unit, err_delta, low_date, upp_date):
    fech_hora = unit['Fecha-Hora'].to_list() #Convert datetime to list. 
    miss_fecha = []
    miss_fecha_hora = []
    miss_hora = []


    if fech_hora[0] != low_date: #Special case, first unit is missing. 
        missing_date_time = low_date
        miss_hora.append(float(missing_date_time.hour) + 1.0)
        miss_fecha.append(missing_date_time.date())
        miss_fecha_hora.append(missing_date_time)

    elif fech_hora[len(fech_hora)-1] !=  upp_date - err_delta: #Special case, last unit is missing (in construction!!!)
        print()


    for i in range(1, len(fech_hora)): #Iterate through ascending pairs of dates to look for missing observations. 
        if (fech_hora[i]- fech_hora[i-1]) > err_delta: #If the distance between date i and i-1 is greater than a day, then that date-time is missing. 
            missing_date_time = fech_hora[i-1] + err_delta

            miss_hora.append(float(missing_date_time.hour) + 1.0)
            miss_fecha.append(missing_date_time.date())
            miss_fecha_hora.append(missing_date_time)

    
    cols_aux = [pl.Series('Fecha',miss_fecha),  pl.Series('Hora', miss_hora)] + [pl.Series(col, [np.nan]*len(miss_fecha) ) for col in horarios_part.columns[2:(len(unit.columns)-1 ) ] ] +  [pl.Series('Fecha-Hora',   miss_fecha_hora, dtype = pl.Datetime(time_unit = 'us') )]                                                                                             
    print("Esto es miss_hora: ",miss_hora)

    return pl.DataFrame(cols_aux)

def isExtraordinary(unit, lim, unit_n): #Handles some encountered extroardinary cases. 
    num = 0 #num = 0 encodes the abscence of problems
    aux = unit['Fecha-Hora'].unique()

    if len(aux) != lim: #Encountered case 1, repeated values and missing hours at the same time (may result in lim == unit_ran) while data isnt appropiate.        
        num = 1
    return num


def process_unit(unit, estaciones, limit_percentage, unit_n, unit_ran, err_delta, low_date, upp_date):
    aux = unit
    lim, a = aux.shape
    stations = []
    date_n = unit['Fecha'][0]
    print('thisis date_n',date_n)
    print(type(date_n))
    hours = {f'Hour{i+1}':[] for i in range(0,unit_ran)} 
    extraordinary_case = isExtraordinary(unit, lim, unit_n) #Checks data for extraordinary cases. 
    print(aux)

    if lim != unit_ran or extraordinary_case != 0 :
        if lim > unit_ran: #If there are more observations than days-hours in unit, then there are duplicates. 
            aux = unit.unique(maintain_order=True) #Keep unique values in DataFarme. 
            lim, a = aux.shape    #Update lim value. 
            print("lim > unit_ran")
        elif lim < unit_ran: #In other case, there are missing days-hours recordings.

            aux.extend(process_missing(aux, err_delta, low_date, upp_date)) #Look for missing date-hours and fill with np.nan (this missingness will be better addressed later)
            aux = aux.sort( pl.col('Fecha-Hora')) #Re-sort by date and time. 
            lim, a = aux.shape    #Update lim value. 
            print("lim < unit_ran")
        elif extraordinary_case == 1:
            print("Extraordinary case 1")
            aux = unit.unique(maintain_order=True) #Keep unique values in DataFarme. 
            aux.extend(process_missing(aux, err_delta, low_date, upp_date)) #Look for missing date-hours and fill with np.nan (this missingness will be better addressed later)
            aux = aux.sort( pl.col('Fecha-Hora')) #Re-sort by date and time. 
            lim, a = aux.shape    #Update lim value. 
        else:
            raise Exception("Not known extraordinary case!!")


    for estacion in estaciones: #Checks those stations which have more than 100 missing values. 
        l = aux[estacion].value_counts() #Count the number of occurrances of value s(including NaN)
        l = l.filter(pl.col(estacion) == np.nan) #Select only the row which contains the number of missing values. 
        r,c = l.shape #Record shape for check purposes (i.e. if r != 0, this means that the returned l is non empty)

        if (r != 0 and  100*(l[0,1]/lim) >limit_percentage) == False: #If %missingvalues is greater than the tolerance, eliminate. 
            stations.append(estacion)
            reg = aux[estacion].to_list()
            i = 0
            for hour in hours:
                hours[hour].append(reg[i])
                i+=1
    unit_c = [unit_n]*len(stations)
    date_c = [date_n]*len(stations)


    cols = [pl.Series('Station',stations), pl.Series('unit_n', unit_c, dtype = pl.Int64), pl.Series('date', date_c, dtype= pl.Date)] + [pl.Series(hour, hours[hour]) for hour in hours]
    toRet = pl.DataFrame(cols)

    return toRet


parametro = 'co'
anio = '2023'
qtipo = 'HORARIOS'
file_horarios_part = f'{anio}_{qtipo}_{parametro}.csv'#Gets file path. 

horarios_part = pl.read_csv(file_horarios_part, has_header = True) #Reads data obtained from DataGather1.py

horarios_part = horarios_part.with_columns(horarios_part['Fecha-Hora'].str.to_datetime(format = '%Y-%m-%dT%H:%M:%S.%f')) #Convert 'Fecha-Hora' back to date format. 
horarios_part = horarios_part.with_columns(horarios_part['Fecha'].str.to_date()) #Converts Fecha to date. 

horarios_part = horarios_part.sort(pl.col('Fecha-Hora')) #Sorts by date and time in ascending order. 


unit_delta = timedelta(days = 1) #unit_delta sets the difference between time units. 
unit_ran = 24 #unit_ran contains the numbers of hours contained in a particular unit. (i.e. for a week, there are 7*24 hours, for a three day period unit_week = 3*24)
unit_n = 1 #Sets the unit number. i.e. week/month/day number unit_n
low_delta = timedelta(days = 1)
err_delta = timedelta(hours = 1)

data_final_file = f"{str(unit_ran)}_{file_horarios_part.replace('.csv', '')}_R.RData" #day_horarios_co_R.RData, original name. 


cols = [pl.Series('Station', [],dtype = pl.String ), pl.Series('unit_n', [], dtype = pl.Int64), pl.Series('date', [], dtype= pl.Date)] + [pl.Series(f'Hour{i+1}', [], dtype = pl.Float64) for i in range(0,unit_ran) ] #Creates series list for final data frame. 
data_final = pl.DataFrame( cols) #Creates final data frame. 

row,col = horarios_part.shape #Get dimensions of data. 
estaciones = horarios_part.columns[3: (col-1) ] #Selects station names. 


#The following section filters observations per unit and creates new final data set. 2023-10-29

low_date = datetime.strptime('2023-01-01','%Y-%m-%d' ) #Sets lower bound for first unit. 
upp_date = datetime.strptime('2023-01-01','%Y-%m-%d') #Sets upper bound for first unit. 

limit_date =datetime.strptime('2023-12-30','%Y-%m-%d') #Sets bound date for year 2023. 

while upp_date <= limit_date: #While the upper bound for unit is lower than the limit date for the year. 
    print(low_date, upp_date, unit_n ,'----------------------------------------------------------')
    unit = horarios_part.filter(  pl.col('Fecha').is_between(low_date, upp_date) ) #Select data corresponding to the unit in low_date and upp_date
    data_final.extend(process_unit(unit, estaciones, 13.5, unit_n, unit_ran, err_delta, low_date, upp_date)) #Process data for the unit. 
    low_date = upp_date + low_delta 
    upp_date = upp_date + unit_delta
    unit_n +=1 


data_final = data_final.sort('Station')
data_final = data_final.to_pandas()
print(data_final)

pandas2ri.activate() # Activates automatic  conversion from pandas DataFrame to R data frame.
r_data_final = pandas2ri.py2rpy(data_final) #Converts pandas data frame into an R data frame. 
robjects.globalenv['r_data'] = r_data_final # Assign the R data frame to an R variable.

robjects.r('str(r_data)') #Prints created R data set. 
robjects.r(f'save(r_data, file="{data_final_file}")') #Saves created data. 

