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
        1) Transformaciones internas (retorno logarítmico diario, medias móviles a 7 y 30 días, volatilidad a 7 y 30 días)
        2) Limpieza de métricas (eliminar filas con NaN en métricas clave)
        3) Enriquecimiento macro (tasa de interés del BCE)
        4) Relleno de NaN en tasa BCE y reset índice final
        """
        try:
            # 0) LIMPIEZA ------------------------------------------------
            self.logger.info('DataEnricher', 'enriquecer', 'Iniciando limpieza de datos')
            # Eliminar duplicados por fecha
            df = df.drop_duplicates(subset=['fecha'])
            # Convertir 'fecha' a datetime y descartar nulos básicos
            df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')
            df = df.dropna(subset=['fecha', 'cerrar', 'abrir', 'max', 'min', 'volumen'])
            # Normalizar 'cerrar' a float
            df['cerrar'] = (
                df['cerrar']
                .astype(str)
                .str.replace('.', '', regex=False)
                .str.replace(',', '.', regex=False)
                .pipe(pd.to_numeric, errors='coerce')
            )
            df = df.dropna(subset=['cerrar'])
            df = df[df['cerrar'] > 0]
            # Ordenar cronológicamente y resetear índice
            df = df.sort_values('fecha').reset_index(drop=True)
            self.logger.info('DataEnricher', 'enriquecer', 'Limpieza inicial completada')

            # 1) TRANSFORMACIONES INTERNAS -------------------------------
            self.logger.info('DataEnricher', 'enriquecer', 'Iniciando transformaciones internas')
            df['retorno_log_diario'] = np.log(df['cerrar'] / df['cerrar'].shift(1))
            df['media_movil_7d']     = df['cerrar'].rolling(window=7).mean()
            df['media_movil_30d']    = df['cerrar'].rolling(window=30).mean()
            df['volatilidad_7d']     = df['retorno_log_diario'].rolling(window=7).std()
            df['volatilidad_30d']    = df['retorno_log_diario'].rolling(window=30).std()
            self.logger.info('DataEnricher', 'enriquecer', 'Transformaciones internas finalizadas')

            # 2) LIMPIEZA DE MÉTRICAS -------------------------------------
            self.logger.info('DataEnricher', 'enriquecer', 'Eliminando filas con NaN en métricas clave')
            df = df.dropna(subset=[
                'retorno_log_diario', 'media_movil_7d', 'media_movil_30d',
                'volatilidad_7d', 'volatilidad_30d'
            ]).reset_index(drop=True)
            self.logger.info('DataEnricher', 'enriquecer', 'Limpieza de métricas completada')

            # 3) ENRIQUECIMIENTO MACRO (TASA BCE) --------------------------
            self.logger.info('DataEnricher', 'enriquecer', 'Descargando indicador macro: tasa_interes_bce')
            url_bce = (
                'https://sdw.ecb.europa.eu/quickviewexport.do'
                '?SERIES_KEY=120.EXR.D.EUR.N.X&csv.x=yes'
            )
            df_bce = (
                pd.read_csv(url_bce, skiprows=5, names=['fecha', 'tasa_interes_bce'], parse_dates=['fecha'])
                  .dropna(subset=['tasa_interes_bce'])
                  .sort_values('fecha')
            )
            df = pd.merge(df, df_bce, on='fecha', how='left')
            self.logger.info('DataEnricher', 'enriquecer', 'Enriquecido con tasa_interes_bce')

            # 4) RELLENO DE NaN EN TASA BCE -------------------------------
            self.logger.info('DataEnricher', 'enriquecer', 'Rellenando NaN en tasa_interes_bce')
            df['tasa_interes_bce'] = df['tasa_interes_bce'].fillna(method='ffill').fillna(method='bfill')
            df = df.reset_index(drop=True)
            self.logger.info('DataEnricher', 'enriquecer', 'Proceso de enriquecimiento completado')

            return df

        except Exception as e:
            self.logger.error('DataEnricher', 'enriquecer', f'Error durante el enriquecimiento: {e}')
            return df