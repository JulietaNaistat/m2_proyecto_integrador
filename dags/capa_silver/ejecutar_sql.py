from sqlalchemy import create_engine
import os

def ejecutar_sql():
    # Leer el archivo SQL
    with open('capa_silver/sql_statements.sql', 'r') as f:
        query = f.read()

    # Setup
    user = os.getenv('DB_USER') 
    password = os.getenv('DB_PASSWORD')
    host = 'localhost'
    port = '5432'
    db_name = 'm2_pi'

    # Conexión a DB
    connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}'
    engine = create_engine(connection_string)

    # Ejecutar
    with engine.connect() as conn:
        conn.execute(query)