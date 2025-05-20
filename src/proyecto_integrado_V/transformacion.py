# src/proyecto_integrado_V/transformacion.py

import pandas as pd
import numpy as np
from logger import Logger

class DataEnricher:
    def __init__(self, logger: Logger):
        self.logger = logger

    def enriquecer(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Realiza la limpieza, transformaciones internas y enriquece con indicador macroeconómico.
        Pasos:
        0) Limpieza de datos (duplicados, nulos, precios no válidos, reset índice)
        1) Transformaciones internas (retorno logarítmico, medias móviles, volatilidad)
        2) Enriquecimiento macro (tasa de interés del BCE)
        """
        try:
            # 0) LIMPIEZA ------------------------------------------------
            self.logger.info('DataEnricher', 'enriquecer', 'Iniciando limpieza de datos')

            # Eliminar duplicados por fecha
            df = df.drop_duplicates(subset=['fecha'])

            # Convertir fecha y descartar nulos
            df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')
            df = df.dropna(subset=['fecha', 'cerrar'])

            # Normalizar 'cerrar' a float
            df['cerrar'] = (
                df['cerrar']
                .astype(str)
                .str.replace('.', '', regex=False)
                .str.replace(',', '.', regex=False)
                .pipe(pd.to_numeric, errors='coerce')
            )
            df = df.dropna(subset=['cerrar'])

            # Filtrar precios no válidos
            df = df[df['cerrar'] > 0]

            # Ordenar cronológicamente y resetear índice
            df = df.sort_values('fecha').reset_index(drop=True)
            self.logger.info('DataEnricher', 'enriquecer', 'Limpieza completada')

            # 1) TRANSFORMACIONES INTERNAS -------------------------------
            self.logger.info('DataEnricher', 'enriquecer', 'Iniciando transformaciones internas')
            df['retorno_log'] = np.log(df['cerrar'] / df['cerrar'].shift(1))
            df['ma_7']        = df['cerrar'].rolling(window=7).mean()
            df['ma_30']       = df['cerrar'].rolling(window=30).mean()
            df['vol_7']       = df['retorno_log'].rolling(window=7).std()
            df['vol_30']      = df['retorno_log'].rolling(window=30).std()
            self.logger.info('DataEnricher', 'enriquecer', 'Transformaciones internas finalizadas')

            # 2) ENRIQUECIMIENTO MACRO (TASA BCE) --------------------------
            self.logger.info('DataEnricher', 'enriquecer', 'Añadiendo indicador macro: tasa BCE')
            url_bce = (
                'https://sdw.ecb.europa.eu/quickviewexport.do'
                '?SERIES_KEY=120.EXR.D.EUR.N.X&csv.x=yes'
            )
            df_bce = (
                pd.read_csv(url_bce, skiprows=5, names=['fecha', 'tasa_bce'], parse_dates=['fecha'])
                  .dropna(subset=['tasa_bce'])
                  .sort_values('fecha')
            )

            # Unir datasets
            df = pd.merge(df, df_bce, on='fecha', how='left')
            self.logger.info('DataEnricher', 'enriquecer', 'Enriquecido con tasa BCE')

            return df

        except Exception as e:
            self.logger.error('DataEnricher', 'enriquecer', f'Error durante el enriquecimiento: {e}')
            return df
