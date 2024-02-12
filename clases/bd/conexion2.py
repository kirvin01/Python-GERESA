from decouple import config
import pandas as pd
import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
from sqlalchemy import create_engine




class MyDatabase2():

    def __init__(self):
       # host = '192.168.1.7'
       # user = 'postgres'
       # password = '213141'
       # database = 'irvin_hisminsa'
       # port = 5432
        
        host = config('DB_HOST')
        user = config('DB_USER')
        password = config('DB_PASSWORD')
        database = config('DB_DATABASE')
        port = config('DB_PORT')

        try:
            self.conn = psycopg2.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                port=port
            )
            if (self.conn != None):
                print("Conexion exitosa con la Base de datos:" + database)
                self.cur = self.conn.cursor()
                self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            # Agrega el motor de SQLAlchemy a la clase
                self.engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')


        except (Exception, Error) as ex:
            print("Error al conectar",ex)

    def sql(self, sql):
         self.cur.execute(sql)
      
    def df(self, sql):
        s = pd.read_sql(sql, con=self.conn)
        return s
    def insert_df(self, df, table_name,schema):
        try:
            # Inserta el DataFrame en la base de datos usando SQLAlchemy
            df.to_sql(table_name, self.engine,schema=schema,if_exists='replace', index=False)
            print(f'DataFrame insertado en la tabla {table_name}')

        except Exception as e:
            print(f'Error al insertar el DataFrame en la tabla {table_name}: {str(e)}')


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


#db = MyDatabase2()
#p=db.sql('SELECT mhe.id_establecimiento, mhe.codigo_red ,mhe.codigo_microred  FROM maestro_his_establecimiento mhe WHERE mhe.codigo_disa =11')
#db.cur_sql()
#print(db)

#db.close()


