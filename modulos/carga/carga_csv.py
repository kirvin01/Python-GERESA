import pandas as pd
import numpy as np
import os
import glob as gb
import sys
from zipfile import ZipFile
from os import remove

import gzip
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
import psycopg2
host = '192.168.1.7'
user = 'admin_ic'
password = '213141'
database = 'irvin_hisminsa'
options="-c search_path=maestros"
port = 5432
pgconn = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    database=database,
    port=port,
    options=options
)
# cursor
pgcursor = pgconn.cursor()
# codigo requerido
pgconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)



pgcursor.execute("TRUNCATE TABLE maestros.nominal_trama_2024")
#ruta_datos = os.path.abspath("../../../data/2023/csv/")
# print(ruta_datos)

# carpeta contenedor de datos Zip
dx = gb.glob("D:/Irvin/Irvin/Python/data/2024/csv/*.csv")
print(dx)

# lista de df
list_df = []
for f in dx:
    ruta = f
    print(f)  

    #pgcursor.execute("TRUNCATE TABLE maestros.nominal_trama_2023")
    with open(f, mode='r', encoding="ISO-8859-1") as f:
        next(f)
        pgcursor.copy_from(f, 'nominal_trama',    sep=',', null='', columns=None)
    #   print("Carga Completada: "+name)
print("Termino el Proceso")

pgcursor.execute("SELECT crea_nominal_trama2()")