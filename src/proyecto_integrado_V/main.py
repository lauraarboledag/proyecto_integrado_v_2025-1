from logger import Logger
from collector import Collector
import pandas as pd
import os
import sqlite3

def main():
    logger = Logger()
    logger.info('Main', 'main', 'Inicializar clase Logger')

    collector = Collector(logger=logger)
    logger.info('Main', 'main', 'Inicializar clase Collector')

    df = collector.collector_data()

    if df.empty:
        logger.warning('Main', 'main', 'No se extrajeron datos, DataFrame vacío')
    else:
        # Ruta base
        base_path = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(base_path, 'static', 'data')
        os.makedirs(output_dir, exist_ok=True)

        # Guardar CSV
        csv_path = os.path.join(output_dir, 'BTC_EUR_data.csv')
        df.to_csv(csv_path, index=False)
        logger.info('Main', 'main', f'Datos guardados en {csv_path}')

        # Guardar SQLite
        db_path = os.path.join(output_dir, 'btc_eur_data.db')
        table_name = 'btc_eur_history'

        try:
            conn = sqlite3.connect(db_path)
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.close()
            logger.info('Main', 'main', f'Datos guardados en base de datos SQLite: {db_path} (tabla: {table_name})')
        except Exception as e:
            logger.error('Main', 'main', f'Error al guardar en SQLite: {e}')

        print(df.head())

if __name__ == "__main__":
    main()
