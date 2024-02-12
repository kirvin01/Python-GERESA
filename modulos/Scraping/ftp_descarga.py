import paramiko
import os

def download_files_sftp(hostname, port, username, password, remote_path, local_path):
    try:
        # Crear el objeto SSHClient
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Conectar al servidor SFTP
        client.connect(hostname, port, username, password)

        # Crear el objeto SFTPClient
        sftp = client.open_sftp()

        # Verificar si la ruta local existe, si no, crearla
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        # Listar archivos remotos en el directorio
        remote_files = sftp.listdir(remote_path)

        # Descargar cada archivo remoto
        for remote_file in remote_files:
            remote_file_path = os.path.join(remote_path, remote_file)
            local_file_path = os.path.join(local_path, remote_file)
            sftp.get(remote_file_path, local_file_path)

        print("Descarga completa.")

    except Exception as e:
        print(f"Error durante la descarga: {e}")

    finally:
        # Cerrar la conexión
        if sftp:
            sftp.close()
        client.close()

    

# Configuramos los datos de conexión
host = "181.177.250.128"
port = 22
username = "usr_dhr_008"
password = "X3veTg8z62R74J"

# Ruta remota y local
remote_directory = "/data_cvs_nominal/2024/"
local_directory = "D:/Irvin/Irvin/Python/data/2024"

# Llamar a la función para descargar archivos
download_files_sftp(host, port, username, password, remote_directory, local_directory)

#====================== descargar de RAR=================================
import rarfile
import os
import glob as gb


# Ruta de la carpeta que contiene archivos .rar
carpeta_rar = "D:/Irvin/Irvin/Python/data/2024/"

# Lista todos los archivos en la carpeta
archivos_rar = gb.glob(os.path.join(carpeta_rar, "*.rar"))

# Carpeta de destino para los archivos extraídos
carpeta_destino = "D:/Irvin/Irvin/Python/data/2024/csv"

# Crear la carpeta de destino si no existe
if not os.path.exists(carpeta_destino):
    os.makedirs(carpeta_destino)

# Itera sobre los archivos .rar y los extrae
for archivo_rar in archivos_rar:
    try:
        # Abre el archivo RAR
        with rarfile.RarFile(archivo_rar, 'r') as rar:
            # Extrae los archivos al directorio de destino
            rar.extractall(carpeta_destino)
        print(f"Archivos extraídos de {archivo_rar} a {carpeta_destino}")
    except rarfile.Error as e:
        print(f"Error al extraer {archivo_rar}: {e}")

print("Proceso completado de descarga y descomprimir rar.") 

# ===============================================================================    