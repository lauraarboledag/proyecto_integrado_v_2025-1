import os
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error
from logger import Logger

class Modeler:
    def __init__(self, logger: Logger):
        self.logger = logger
        self.model = None
        # Ruta para guardar el modelo
        base_path = os.path.dirname(os.path.abspath(__file__))
        models_dir = os.path.join(base_path, 'static', 'models')
        os.makedirs(models_dir, exist_ok=True)
        self.model_path = os.path.join(models_dir, 'model.pkl')

    def entrenar(self, df: pd.DataFrame):
        """
        Entrena un modelo de regresión para predecir el precio de cierre 'cerrar'
        a partir de características técnicas y macroeconómicas.
        Guarda el modelo entrenado en static/models/model.pkl.
        Devuelve las métricas de evaluación (RMSE y MAE).
        """
        try:
            self.logger.info('Modeler', 'entrenar', 'Iniciando entrenamiento del modelo')
            # Variables predictoras y objetivo
            features = [
                'retorno_log_diario', 'media_movil_7d', 'media_movil_30d',
                'volatilidad_7d', 'volatilidad_30d']
            X = df[features]
            y = df['cerrar']

            # Dividir en entrenamiento y prueba
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )

            # Crear y entrenar el modelo
            self.model = RandomForestRegressor(n_estimators=100, random_state=42)
            self.model.fit(X_train, y_train)

            # Predicción y métricas
            preds = self.model.predict(X_test)
            rmse = np.sqrt(mean_squared_error(y_test, preds))
            mae = mean_absolute_error(y_test, preds)

            # Guardar el modelo
            joblib.dump(self.model, self.model_path)
            self.logger.info('Modeler', 'entrenar', f'Modelo guardado en {self.model_path}')

            # Log métricas
            self.logger.info('Modeler', 'entrenar', f'RMSE: {rmse:.4f}, MAE: {mae:.4f}')
            return {'rmse': rmse, 'mae': mae}

        except Exception as e:
            self.logger.error('Modeler', 'entrenar', f'Error en entrenamiento: {e}')
            return None

    def predecir(self, df: pd.DataFrame) -> np.ndarray:
        """
        Carga el modelo guardado y retorna predicciones para nuevos datos.
        """
        try:
            if self.model is None:
                # Cargar desde disco si no está en memoria
                self.model = joblib.load(self.model_path)
                self.logger.info('Modeler', 'predecir', f'Modelo cargado desde {self.model_path}')

            features = [
                'retorno_log_diario', 'media_movil_7d', 'media_movil_30d',
                'volatilidad_7d', 'volatilidad_30d'
            ]
            X_new = df[features]
            preds = self.model.predict(X_new)
            return preds

        except Exception as e:
            self.logger.error('Modeler', 'predecir', f'Error en predicción: {e}')
            return np.array([])
