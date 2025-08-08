import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


def connect_to_db_load_csv():
    # Configurar una base de datos relacional PostgreSQL/SQL Server)
    # Setup config
    load_dotenv()  
    user = os.getenv('DB_USER') 
    password = os.getenv('DB_PASSWORD')
    host = 'localhost'
    port = '5432'
    db_name = 'm2_pi'

    # Conexión a DB
    connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}'
    print(connection_string)
    engine = create_engine(connection_string)

    #Cargar datos raw a bronze schema
    def cargar_csv_db(file):
        try:
            # Leer el CSV
            df = pd.read_csv('Files/' + file)  
            print(file + ' Leido!')
            # Transformar el nombre del archivo
            file = re.sub(r'csv', '', file, flags=re.IGNORECASE) 
            file = re.sub(r'[^a-zA-Z_]', '', file).lower()
            print('Nombre archivo transformado: ' + file)
            df.columns = df.columns.str.lower()  
            # Cargar el csv a la tabla correspondiente
            df.to_sql(file, engine, if_exists='replace', index=False, schema='bronze')
            print('Datos insertados a tabla: ' + file)
        except Exception as e:
            print('Error en la carga: ' + str(e))

    archivos = os.listdir('Files/')
    for archivo in archivos:
        try:
            cargar_csv_db(archivo)
        except Exception as e:
            print('Error en la carga: ' + e)