from logger import Logger
from collector import Collector
import pandas as pd
import os

def main():
    logger = Logger()
    logger.info('Main', 'main', 'Inicializar clase Logger')

    collector = Collector(logger=logger)
    logger.info('Main', 'main', 'Inicializar clase Collector')

    df = collector.collector_data() 

    if df.empty:
        logger.warning('Main', 'main', 'No se extrajeron datos, DataFrame vacío')
    else:
        output_path = "/src/proyecto_integrado_V/static/data/BTC_EUR_data.csv"
        df.to_csv(output_path, index=False)
        logger.info('Main', 'main', f'Datos guardados en {output_path}')
        print(df.head()) 

if __name__ == "__main__":
    main()
