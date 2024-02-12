import pandas as pd
import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values


class CPaciente():

    def __init__(self):
        host = 'localhost'
        user = 'postgres'
        password = '213141'
        database = 'SiCoVac'
        port = 5432

        try:
            self.conn = psycopg2.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                port=port
            )
            if (self.conn != None):
                print("Conexion exitosa con la Base de datos:"+ database)
                self.cur = self.conn.cursor()
                self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        except (Exception, Error) as ex:
            print("Error al conectar")
    def sql(self, sql):
         self.cur.execute(sql)
      
    def df(self, sql):
        s = pd.read_sql(sql, con=self.conn)
        return s

    def sqli(self, df, table):
        df = df.fillna(psycopg2.extensions.AsIs('NULL'))
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        query = "INSERT INTO  %s (%s) VALUES %%s" % (table, cols)

        execute_values(self.cur, query, tuples)
        return cols
        # execute_values(pgcursor,query, tuples)

        # self.cur.execute(query,)

    def close(self):
        self.cur.close()
        self.conn.close()