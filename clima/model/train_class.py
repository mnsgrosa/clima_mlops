import xgboost as xgb
import mlflow
import mlflow.xgboost
import os
import glob
import optuna
from mlflow.models import infer_signature
from datetime import datetime
from sklearn.metrics import mean_squared_error

class MyModel:
    def __init__(self, experiment_name = 'clima'):
        self.experiment_name = experiment_name
        self.model_path='./clima/model/model_storage'
        self.model_max = self.get_model('max')
        self.model_min = self.get_model('min')
        if self.model_max is None or self.model_min is None:
            self.model_max = self.create_model()
            self.model_min = self.create_model()

    def get_latest_model(self, mode:str = 'max'):
        try:
            runs_df = mlflow.search_runs(
                experiment_names = [self.experiment_name],
                filter_string = f'tags.model_type = "{mode}"',
                order_by = ['start_time DESC'],
                max_results = 1
            )
            if not runs_df.empty:
                run_id = runs_df.iloc[0].run_id
                model_uri = f'runs:/{run_id}/mdel_{mode}'
                return mlflow.xgboost.load_model(model_uri)
        except Exception as e:
            return None
        return None

    def create_default_model(self, params):
        return xgb.XGBRegressor(objective = 'reg:squarederror', 
                                n_estimators = 1000, 
                                learning_rate = 0.05, 
                                early_stopping_rounds = 50
                                )

    def optimize(self, X, y, X_eval, y_eval, mode:str = 'max', n_trials = 50):
        target_model = self.model_max if mode == 'max' else self.model_min

        def objective(trial):
            params = {
                'n_estimators': trial.suggest_int('n_estimators', 200, 2000),
                'learning_rate': trial.suggest_float('learning_rate', 1e-3, 0.1, log=True),
                'max_depth': trial.suggest_int('max_depth', 3, 10),
                'subsample': trial.suggest_float('subsample', 0.6, 1.0),
                'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
            }

            model = xgb.XGBRegressor(objective = 'reg:squarederror', **params)
            model.fit(X, y, eval_set = [(X_eval, y_eval)], verbose = False, early_stopping_rounds = 50)
            preds = model.predict(X_eval)
            rmse = mean_squared_error(y_eval, preds, squared = False)
            return rmse

        study = optuna.create_study(direction = 'minimize')
        study.optimize(objective, n_trials = n_trials)
        target_model.set_params(**study.best_params)

    def fit(self, X, y, X_eval, y_eval, mode:str = 'max'):
        target_model = self.model_max if mode == 'max' else self.model_min
        
        with mlflow.start_run() as run:
            mlflow.set_tag('model_type', mode)
            mlflow.log_params(target_model.get_params())
            
            target_model.fit(
                X, y, 
                eval_set=[(X_eval, y_eval)], 
                verbose=False
            )
            
            mlflow.xgboost.log_model(
                xgb_model=target_model,
                artifact_path=f'model_{mode}'
            )
            print(f"Successfully trained and logged '{mode}' model to run_id: {run.info.run_id}")

    def predict(self, X, mode:str = 'max'):
        model = self.model_max if mode == 'max' else self.model_min
        return model.predict(X)