import requests
import pandas as pd
from bs4 import BeautifulSoup
import os

class Collector:
    def __init__(self, logger):
        self.url = 'https://es.finance.yahoo.com/quote/BTC-EUR/history/?period1=1410912000&period2=1746572832'
        self.logger = logger

        # Ruta relativa al script
        base_path = os.path.dirname(os.path.abspath(__file__))
        static_path = os.path.join(base_path, 'static', 'data')
        os.makedirs(static_path, exist_ok=True)

    def collector_data(self):
        try:
            df = pd.DataFrame()
            headers = {
                'User-Agent': 'Mozilla/5.0'
            }

            response = requests.get(self.url, headers=headers)
            if response.status_code != 200:
                self.logger.error('Collector', 'collector_data', f"Error al consultar la url : {response.status_code}")
                return df

            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.select_one('div[data-testid="history-table"] table')
            if table is None:
                self.logger.error('Collector', 'collector_data', "Error al buscar la tabla data-testid=history-table")
                return df

            headerss = [th.get_text(strip=True) for th in table.thead.find_all('th')]
            rows = []
            for tr in table.tbody.find_all('tr'):
                colums = [td.get_text(strip=True) for td in tr.find_all('td')]
                if len(colums) == len(headerss):
                    rows.append(colums)

            df = pd.DataFrame(rows, columns=headerss).rename(columns={
                'Fecha': 'fecha',
                'Abrir': 'abrir',
                'Máx.': 'max',
                'Mín.': 'min',
                'CerrarPrecio de cierre ajustado para splits.': 'cerrar',
                'Cierre ajustadoPrecio de cierre ajustado para splits y distribuciones de dividendos o plusvalías.': 'cierre_ajustado',
                'Volumen': 'volumen'
            })

            self.logger.info('Collector', 'collector_data', f"Datos obtenidos exitosamente {df.shape}")
            return df

        except Exception as error:
            self.logger.error('Collector', 'collector_data', f"Error al obtener los datos de la url: {error}")
            return pd.DataFrame()
