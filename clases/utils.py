from decouple import config
import pandas as pd
import numpy as np
import os
import glob as gb
import sys
import datetime
from clases.bd.conexion2 import MyDatabase2

def setup_environment():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)

def connect_database(): 
    return MyDatabase2()

def sys_anio(): 
    return config('SYS_ANIO')

def sys_mes_ini(): 
    return config('SYS_MES_INI')

def sys_mes_fin(): 
    return config('SYS_MES_FIN')