import pandas as pd
import numpy as np
from logger import Logger

class DataTransformer:
    def __init__(self, logger: Logger):
        self.logger = logger

    def transformar(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            self.logger.info('Transformer', 'transformar', 'Iniciando transformación de datos')

            # Asegurar que las columnas necesarias están presentes
            required_cols = ['fecha', 'cerrar']
            if not all(col in df.columns for col in required_cols):
                self.logger.error('Transformer', 'transformar', 'Faltan columnas necesarias en el DataFrame')
                return df

            # Convertir la columna 'fecha' a datetime y ordenar
            df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')
            df = df.dropna(subset=['fecha', 'cerrar'])
            df = df.sort_values(by='fecha')

            # Asegurar que 'cerrar' es numérica
            df['cerrar'] = pd.to_numeric(df['cerrar'].str.replace('.', '', regex=False)
                                                     .str.replace(',', '.', regex=False), errors='coerce')

            df = df.dropna(subset=['cerrar'])

            # Calcular retorno logarítmico diario
            df['retorno_log'] = np.log(df['cerrar'] / df['cerrar'].shift(1))

            # Calcular medias móviles
            df['media_movil_7'] = df['cerrar'].rolling(window=7).mean()
            df['media_movil_30'] = df['cerrar'].rolling(window=30).mean()

            # Calcular volatilidad (desviación estándar)
            df['volatilidad_7'] = df['retorno_log'].rolling(window=7).std()
            df['volatilidad_30'] = df['retorno_log'].rolling(window=30).std()

            self.logger.info('Transformer', 'transformar', 'Transformación completada con éxito')
            return df

        except Exception as e:
            self.logger.error('Transformer', 'transformar', f'Error durante la transformación: {e}')
            return df
