from decouple import config
import pandas as pd
import sys
import unicodedata
sys.path.insert(0, config('PROYECTO_DIR'))
from clases.bd.conexion import  SQLServerConnector
conn = SQLServerConnector('DBGERESA')
pd.set_option('display.max_columns', None)

# 📂 Ruta del archivo Excel
archivo = config('PROYECTO_DATA') + "/OTROS/CG18_DIRECTORIO DE HOSPITALES GENERALES.xlsx"

# 📄 Leer archivo
df = pd.read_excel(archivo)

# 🧹 Limpiar nombres de columnas
def limpiar_columna(texto):
    texto_sin_tildes = ''.join(
        c for c in unicodedata.normalize('NFKD', texto)
        if not unicodedata.combining(c)
    )
    return texto_sin_tildes.replace(' ', '_')

df.columns = [limpiar_columna(col) for col in df.columns]

# 🔍 Crear un TypeSet personalizado solo con tipos básicos
type_set = v.typesets.TypeSet(
    types=[
        v.Integer(),
        v.Float(),
        v.Boolean(),
        v.DateTime(),
        v.String()
    ]
)

# 🔄 Detectar y convertir tipos usando el TypeSet
for col in df.columns:
    detected_type = type_set.detect_type(df[col])
    df[col] = type_set.convert(df[col], detected_type)

print(df.dtypes)  # Verificación

# 🗄 Mapeo Pandas → SQLAlchemy
from sqlalchemy import Integer, Float, Date, Boolean, String

sql_type_map = {
    'int64': Integer(),
    'float64': Float(),
    'datetime64[ns]': Date(),
    'bool': Boolean(),
    'object': String()
}

dtype_mapping = {
    col: sql_type_map.get(str(df[col].dtype), String())
    for col in df.columns
}

# 💾 Insertar en SQL Server
conn.insertar_dataframe(
    df,
    'CG18_CamasSaludMental',
    if_exists='replace',
    dtype=dtype_mapping
)

print("✅ DataFrame insertado con tipos correctos en SQL Server")
