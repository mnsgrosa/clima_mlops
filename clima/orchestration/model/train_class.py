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
        mlflow.set_experiment(experiment_name)
        self.experiment_name = experiment_name
        self.model_path='./clima/model/model_storage/'
        self.model_max = None
        self.model_min = None
        self.mode = mode

    async def get_latest_model(self, mode:str = 'max'):
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
                loaded_model = mlflow.xgboost.load_model(model_uri)
            if mode == 'max':
                self.model_max = loaded_model
            else:
                self.model_min = loaded_model
            return loaded_model
        except Exception as e:
            return None
        return None

    async def create_default_model(self, params):
        return xgb.XGBRegressor(objective = 'reg:squarederror', 
                                n_estimators = 1000, 
                                learning_rate = 0.05, 
                                early_stopping_rounds = 50
                                )

    async def optimize(self, X_train, y_train, X_eval, y_eval, mode:str = 'max', n_trials = 50):
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
            model.fit(X_train, y_train, eval_set = [(X_eval, y_train)], verbose = False, early_stopping_rounds = 50)
            preds = model.predict(X_eval)
            rmse = mean_squared_error(y_train, preds, squared = False)
            return rmse

        study = optuna.create_study(direction = 'minimize')
        study.optimize(objective, n_trials = n_trials)
        target_model.set_params(**study.best_params)


    async def fit(self, X, y, X_eval, y_eval, mode:str = 'max', params = None):
        target_model = self.model_max if mode == 'max' else self.model_min
        
        if target_model is None:
            target_model = self.create_default_model()
            if params:
                target_model.set_params(**params)
        

        with mlflow.start_run() as run:
            mlflow.set_tag('model_type', mode)
            mlflow.log_params(target_model.get_params())
            
            target_model.fit(
                X_train, y_train, 
                eval_set=[(X_eval, y_train)], 
                verbose=False
            )
            
            mlflow.xgboost.log_model(
                xgb_model=target_model,
                artifact_path=f'model_{mode}'
            )
            print(f"Successfully trained and logged '{mode}' model to run_id: {run.info.run_id}")

    async def predict(self, X_eval, mode:str = 'max'):
        if self.mode != 'pred':
            raise Exception('Mode selected to training')
        model = self.model_max if mode == 'max' else self.model_min
        return model.predict(X_eval)

    async def save_model(self, mode = 'max'):
        model = self.model_max if mode == 'max' else self.model_min
        if model is None:
            raise Exception('Model not found')

        full_path = f'{self.model_path}{mode}'
        mlflow.xgboost.save_model(model, full_path)