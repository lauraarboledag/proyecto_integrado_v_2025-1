import os
import pandas as pd
from modeller import Modeler
from collector import Collector
from logger import Logger

def main():
    logger = Logger()
    collector = Collector(logger)
    modeler = Modeler(logger)

    # 1. Obtener datos
    df = collector.collector_data()
    
    # 2. Entrenar el modelo (opcional, si quieres reentrenar)
    modeler.entrenar(df)
    
    # 3. Hacer predicciones
    preds = modeler.predecir(df)
    
    # 4. Añadir columna de predicción al DataFrame
    df['prediccion'] = preds

     # 5. Guardar DataFrame completo en CSV dentro de la carpeta static/data
    base_path = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_path, 'static', 'data')
    os.makedirs(data_dir, exist_ok=True)
    csv_path = os.path.join(data_dir, 'BTC_EUR_data_predicciones.csv')
    df.to_csv(csv_path, index=False)
    
    logger.info('main', 'main', f'DataFrame con predicciones guardado en {csv_path}')
    print(f'DataFrame guardado en {csv_path}')

    # Mostrar solo las primeras 50 filas en consola
    print(df.head(20))

if __name__ == '__main__':
    main()