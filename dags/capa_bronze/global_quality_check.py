import pandas as pd
import os
import re as re


#Consumir el CSV y verificar duplicados y nulls antes de cargarlo a la DB
def global_quality_check():
    def quality_check(filename):
        df = pd.read_csv('Files/' + filename) #AB_NYC.csv
        dupes_any = str(df.duplicated().any())
        dupes_sum = str(df.duplicated().sum())
        #print(df.describe())
        print(df.info())
        print('Archivo: ' + str(filename) + ' | Hay duplicados?: '+ dupes_any + ' | Cantidad de duplicados: ' + dupes_sum)
        null_percentage = df.isnull().mean() * 100  # Calcular el porcentaje de valores nulos por columna
        print("Porcentaje de valores nulos por columna:")
        print(null_percentage)

    # Aplicamos la funcion a la lista de archivos
    archivos = os.listdir('/Files')
    print(archivos)

    for archivo in archivos:
        try:
            quality_check(archivo)
        except Exception as e:
            print('Error en la carga: ' + e)


