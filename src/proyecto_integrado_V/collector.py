import requests
import pandas as pd
from bs4 import BeautifulSoup
from logger import Logger


class Collector:
    def __init__(self, logger):
        self.url = 'https://es.finance.yahoo.com/quote/BTC-EUR/history/?period1=1410912000&period2=1746572832'
        self.logger = logger

    def collector_data(self):
        try:
             df = pd.DataFrame()
             headers= {
                 'User-Agent':''
             }
             response = requests.get(self.url, headers=headers)
             if response.status.code !=200:
                 self.logger.error("Error al consultar la url: {}".format(response.status_code))
                 return df
             soup = BeautifulSoup(response.text, 'html.parser')
             table = soup.select_one('div[data-testid="history-table"] table')
             if table is None:
                 self.logger.error("Error al buscar la tabla data-testid= history-table")
                 return df
             headerss = [th.get_text(strip=True) for th in table.thead.find_all('th')]
             rows = []
             for tr in table.tbody.find_all('tr'):
                 columns = [td.get_text(strip=True)for td in tr.find_all('td')]
                 if len(columns) == len(headerss):
                     rows.append(columns)
                     
                     df =pd.DataFrame(rows, columns=headerss).rename(columns={
                         
                     }
                                                                     )
            
             self.logger.info("Datos obtenidos exitosamente ".format(df.shape))
        except Exception as error:
            self.logger.info("error al obtener los datos de la url {error}")
        
            