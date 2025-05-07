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
        # Ruta de guardado relativa
        base_path = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(base_path, 'static', 'data')
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, 'BTC_EUR_data.csv')

        df.to_csv(output_path, index=False)
        logger.info('Main', 'main', f'Datos guardados en {output_path}')
        print(df.head())

if __name__ == "__main__":
    main()
