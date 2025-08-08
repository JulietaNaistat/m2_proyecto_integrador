import requests
import pandas as pd
import os
from dotenv import load_dotenv

def consume_api():
    load_dotenv()  
    apikey = os.getenv('APIKEY_CURRENCY') 

    # URL del endpoint
    url = "https://api.currencyapi.com/v3/latest"
    params = {
        "apikey": apikey,
        "currencies": "EUR,GBP,ARS"
    }

    response = requests.get(url, params=params)


    if response.status_code == 200:
        data = response.json()
        
        # Extraer fecha de actualización
        last_updated = data['meta']['last_updated_at']
        
        # Extraer datos de currencies
        currency_data = []
        for code, info in data['data'].items():
            currency_data.append({
                'currency': code,
                'value': info['value'],
                'last_updated_at': last_updated
            })
        
        df = pd.DataFrame(currency_data)
        
        # Guardar como CSV
        df.to_csv("Files/currency_data.csv", index=False)
        print("Datos guardados en 'currency_data.csv'")
    else:
        print(f"Error al consumir el endpoint: {response.status_code}")



