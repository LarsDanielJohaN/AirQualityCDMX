"""
Last edited date: Wed 19 of June, 2024
Created by: Lars Daniel Johansson Niño
Purpose: Add week and month level information where the unit level is day to data resulting from DataGather2. 
"""


import os
scrpt_path = os.path.abspath(__file__) #Gets absolute path of the current script. 
scrpt_dir = os.path.dirname(scrpt_path) # Get the directory name of the current script
os.chdir(scrpt_dir) #Chenges working direct  ory to the python file´s path. 
r_home = os.environ.get('R_HOME') #Sets R_HOME

import polars as pl
import math
import bisect as bs
import numpy as np
from datetime import datetime, timedelta
import pyreadr as rd
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
import pandas as pd


#The following section reads the RData document, temporarely turns it into a pandas document (rpy2 only supports pandas to the extent of my knowledge),
#and then converts it into a polars Dataframe. 
file_path = "day_horarios_co_R.RData" #Sets file path. 
unit_data = rd.read_r(file_path)
unit_data = pl.DataFrame(unit_data['r_data'])
period_name = 'week'
print(unit_data)

unit_n_vals = np.unique(unit_data['unit_n'].to_list())
period_len = 7


period_lims = [unit_n_vals[i] for i in range(period_len-1, len(unit_n_vals), period_len)]

unit_n = unit_data['unit_n'].to_list()
period_pos = [bs.bisect_left(period_lims, i) + 1 for i in unit_n]

period_pos = pl.Series(period_name, period_pos)
unit_data = unit_data.with_columns(period_pos)


unit_data = unit_data.to_pandas()
print(unit_data)

pandas2ri.activate() # Activates automatic  conversion from pandas DataFrame to R data frame.
r_unit_data = pandas2ri.py2rpy(unit_data) #Converts pandas data frame into an R data frame. 
robjects.globalenv['r_data'] = r_unit_data # Assign the R data frame to an R variable.

robjects.r('str(r_data)') #Prints created R data set. 
robjects.r(f'save(r_data, file="{file_path}")') #Saves created data. 







"""
usefull:
rpy2 documentation: https://rpy2.github.io/doc/latest/html/index.html
"""
