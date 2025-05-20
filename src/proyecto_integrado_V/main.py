from logger import Logger
from collector import Collector
from transformacion import DataEnricher
from modeller import Modeler
import pandas as pd
import os
import sqlite3

def main():
    # 1) Inicializar logger
    logger = Logger()
    logger.info('Main', 'main', 'Inicializar clase Logger')

    # 2) Recopilar datos
    collector = Collector(logger)
    logger.info('Main', 'main', 'Inicializar clase Collector')
    df = collector.collector_data()

    # 3) Verificar que hay datos
    if df.empty:
        logger.warning('Main', 'main', 'No se extrajeron datos, DataFrame vacío')
        return

    # 4) Enriquecer datos (internos + macro)
    enricher = DataEnricher(logger)
    df = enricher.enriquecer(df)

    # 5) Definir rutas de salida
    base_path  = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_path, 'static', 'data')
    os.makedirs(output_dir, exist_ok=True)

    # 6) Guardar CSV
    csv_path = os.path.join(output_dir, 'BTC_EUR_data.csv')
    try:
        df.to_csv(csv_path, index=False)
        logger.info('Main', 'main', f'Datos guardados en CSV: {csv_path}')
    except Exception as e:
        logger.error('Main', 'main', f'Error al guardar CSV: {e}')

    # 7) Guardar en SQLite
    db_path    = os.path.join(output_dir, 'btc_eur_data.db')
    table_name = 'btc_eur_history'
    try:
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        logger.info('Main', 'main', f'Datos guardados en SQLite: {db_path} (tabla: {table_name})')
    except Exception as e:
        logger.error('Main', 'main', f'Error al guardar en SQLite: {e}')
    
    # 10) Mostrar resultado breve
    print(df.head())
    
    # 8) Entrenar el modelo
        
    modeler = Modeler(logger)
    metrics = modeler.entrenar(df)
    print(f"RMSE: {metrics['rmse']:.4f}, MAE: {metrics['mae']:.4f}")
    
    # 9) Predicción de ejemplo (reutiliza el mismo df u otro nuevo)
    preds = modeler.predecir(df)
    print("Primeras 5 predicciones:", preds[:5])

if __name__ == "__main__":
    main()
