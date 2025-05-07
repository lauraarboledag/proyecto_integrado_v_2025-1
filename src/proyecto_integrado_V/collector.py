import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
from logger import Logger
import os

class Collector:
    def __init__(self, logger):
        self.url = 'https://es.finance.yahoo.com/quote/BTC-EUR/history/?period1=1410912000&period2=1746572832&guccounter=1&guce_referrer=aHR0cHM6Ly9wcm9iYWJsZS1naWdnbGUtNTZnN2c0dnhnOXczN3JyNi5naXRodWIuZGV2Lw&guce_referrer_sig=AQAAAJ8r8wZNb8Et2HQJNE7izszyrpRwYqg7okoVgb7hIG_GEjTCjZglC1dNZ6blSw0mUPF9miBoihoPA5g2ptwZY05SCU4pk5B-I2aoZorbWtl6JdYR2JqCGiyNosMQQNlMHhOsSuIBLEd_PqfqpufR1pr11XDK5ACI7K4KWOzbTwjy'
        self.logger = logger

        # Usa una ruta relativa para las carpetas
        base_path = os.path.dirname(os.path.abspath(__file__))  # Obtiene la ruta base
        static_path = os.path.join(base_path, 'static', 'data')  # Carpeta relativa
        os.makedirs(static_path, exist_ok=True)  # Crea la carpeta si no existe

    def collector_data(self):
        retries = 3  # Número de reintentos
        for attempt in range(retries):
            try:
                df = pd.DataFrame()
                headers = {
                    'User-Agent': ''
                }
                response = requests.get(self.url, headers=headers)

                if response.status_code != 200:
                    self.logger.error('Collector', 'collector_data', f"Error al consultar la URL: {response.status_code}")
                    if attempt < retries - 1:  # Si no es el último intento
                        self.logger.warning('Collector', 'collector_data', f"Reintentando ({attempt + 1}/{retries})...")
                        time.sleep(5)  # Espera de 5 segundos antes de reintentar
                    continue  # Volver a intentar si no es el último intento

                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.select_one('div[data-testid="history-table"] table')

                if table is None:
                    self.logger.error('Collector', 'collector_data', 'Error al buscar la tabla data-testid=history-table')
                    return df

                headerss = [th.get_text(strip=True) for th in table.thead.find_all('th')]
                rows = []
                for tr in table.tbody.find_all('tr'):
                    columns = [td.get_text(strip=True) for td in tr.find_all('td')]
                    if len(columns) == len(headerss):
                        rows.append(columns)

                df = pd.DataFrame(rows, columns=headerss).rename(columns={
                    'Fecha': 'fecha',
                    'Abrir': 'abrir',
                    'Máx.': 'max',
                    'Mín.': 'min',
                    'CerrarPrecio de cierre ajustado para splits.': 'cerrar',
                    'Cierre ajustadoPrecio de cierre ajustado para splits y distribuciones de dividendos o plusvalías.': 'cierre_ajustado',
                    'Volumen': 'volumen'
                })

                return df  # Si la consulta fue exitosa, termina y retorna los datos

            except Exception as error:
                self.logger.error('Collector', 'collector_data', f"Error al obtener los datos de la URL: {error}")
                if attempt < retries - 1:  # Si no es el último intento
                    self.logger.warning('Collector', 'collector_data', f"Reintentando ({attempt + 1}/{retries})...")
                    time.sleep(5)  # Espera de 5 segundos antes de reintentar
                continue  # Volver a intentar si no es el último intento

        return df  # Retorna el DataFrame vacío si después de varios intentos no se obtiene la respuesta
