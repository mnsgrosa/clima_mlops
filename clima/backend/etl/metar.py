import pandas as pd
import httpx
import mlflow.statsmodels
import numpy as np
from pmdarima.model_selection import RollingForecastCV
from sklearn.metrics import mean_squared_error, mean_absolute_error
from statsmodels.tsa.statespace.sarimax import SARIMAX
from scipy.stats import kstest, chisquare
from math import sin, cos, radians
from sklearn.preprocessing import LabelEncoder

class MetarModelController:
    def __init__(self, model_name: str, order = (1,1,1), seasonal_order = (1,1,1,12), window_size = 30, step_size = 1, forecast_horizon = 1):
        self.model_uri = f'models:/{model_name}/latest'
        self.model = self.init(model_uri)
        self.numerical_columns = ['pressao', 'temperatura', 'vento_dir_seno', 'vento_dir_cosseno',  'umidade', 'vento_int', 'visibilidade'] 
        self.categorical_column = 'tempo'
        self.confidence = 0.05
        self.drifting = False
        self.order = order
        self.seasonal_order = seasonal_order
        self.window_size = window_size
        self.step_size = step_size
        self.forecast_horizon = forecast_horizon

    def init(self, model_path):
        try:
            model = mlflow.statsmodels.load_model(model_uri)
            return model
        except:
            return None

    def etl(self, data):
        temp = data.copy()
        
        temp.drop(columns = ['estacao'], inplace = True)
        
        temp['data'] = pd.to_datetime(temp['data'])

        le = LabelEncoder()
        temp['tempo'] = le.fit_transform(temp['tempo'])
        
        temp.drop(columns = 'tempo_desc', inplace = True)
        
        seno = lambda x: sin(radians(x))
        cosseno = lambda x: cos(radians(x))
        temp['vento_dir_seno'] = temp['vento_dir'].apply(seno)
        temp['vento_dir_cosseno'] = temp['vento_dir'].apply(cosseno)
        temp.drop(columns = 'vento_dir', inplace = True)

        self.temp['umidade'] /= 100

        self.processed_data = temp.copy()
        return self.processed_data

    def decision_data_drift(self, data):
        resultados = {}
        for numerical in self.numerical_columns:
            temp = kstest(data[numerical].values, random_state = 42).pvalue
            if temp < self.confidence:
                self.drift = True
            resultados[numerical] = temp
        
        resultados[self.categorical_column] = chisquare(data[self.categorical_column].values).pvalue - self.confidence
        return resultados

    def get_target(self, data):
        if not self.processed_data.empty:
            self.processed_data['y'] = data
            return True
        return False

    def train(self, data):
        if 'y' not in self.processed_data.columns:
            return []

        if self.model is None:
            self.create_model()
        
        y = self.processed_data.y
        n = len(y)
        preds = []
        actuals = []
        fold_results = []

        max_train_end = n - self.forecast_horizon
        n_folds = (max_train_end - self.window_size) // self.step_size + 1

        for folds in range(n_folds):
            train_start = fold * self.step_size
            train_end = train_start + self.window_size
            test_start = train_end
            test_end = min(test_end, self.forecast_horizon, n)

            if test_end >= train_end:
                y_train = y.iloc[train_start:train_end]
                y_test = y.iloc[test_start:test_end]

            exog_train = self.processed_data.iloc[train_start:train_end].drop(columns = ['y'])
            exog_test = self.processed_data.iloc[test_start:test_end].drop(columns = ['y'])

            try:
                model = SARIMAX(
                    y_train,
                    exog = exog_train,
                    order = self.order,
                    seasonal_order = self.seasonal_order
                )

                fitted_model = model.fit(disp = False)

                forecast = fitted_model.forecast(
                    steps = len(y_test),
                    exog = exog_test
                )

                fold_result = {
                    'fold': fold,
                    'train_start': train_start,
                    'train_end': train_end,
                    'test_start': test_start,
                    'test_end': test_end,
                    'mae': mean_absolute_error(y_test, forecast),
                    'rmse': np.sqrt(mean_squared_error(y_test, forecast)),
                    'mape': np.mean(np.abs((y_test - forecast) / y_test)) * 100
                }

                fold_results.append(fold_result)
                preds.extend(forecast.values)
                actuals.extend(y_test.values)
            except Exception as e:
                continue
            
        self.results = fold_results
        self.predictions_ = predictions
        self.actuals_ = actuals

        if predictions and actuals:
            overall_metrics = {
                'mean_mae': np.mean([r['mae'] for r in fold_results]),
                'mean_rmse': np.mean([r['rmse'] for r in fold_results]),
                'mean_mape': np.mean([r['mape'] for r in fold_results]),
                'std_mae': np.std([r['mae'] for r in fold_results]),
                'std_rmse': np.std([r['rmse'] for r in fold_results]),
                'std_mape': np.std([r['mape'] for r in fold_results]),
                'overall_mae': mean_absolute_error(actuals, predictions),
                'overall_rmse': np.sqrt(mean_squared_error(actuals, predictions)),
                'overall_mape': np.mean(np.abs((np.array(actuals) - np.array(predictions)) / np.array(actuals))) * 100,
                'n_folds': len(fold_results)
            }
        else:
            overall_metrics = {}
            
        return {
            'fold_results': fold_results,
            'overall_metrics': overall_metrics,
            'predictions': predictions,
            'actuals': actuals
        }
            
    def get_results(self):
        if not self.results:
            return pd.DataFrame()
        return pd.DataFrame(self.results)
    
def hyperparameter_search(y, exog=None, param_grid=None, window_size=100):
    if param_grid is None:
        param_grid = {
            'order': [(1,1,1), (2,1,1), (1,1,2), (2,1,2)],
            'seasonal_order': [(1,1,1,12), (1,1,2,12), (2,1,1,12)]
        }
    
    best_score = float('inf')
    best_params = None
    results = []
    
    for order in param_grid['order']:
        for seasonal_order in param_grid['seasonal_order']:
            try:
                cv = MetarModelController(
                    order=order,
                    seasonal_order=seasonal_order,
                    window_size=window_size,
                    forecast_horizon=1
                )
                
                result = cv.rolling_cv_forecast(y, exog)
                score = result['overall_metrics'].get('mean_rmse', float('inf'))
                
                results.append({
                    'order': order,
                    'seasonal_order': seasonal_order,
                    'mean_rmse': score,
                    'mean_mae': result['overall_metrics'].get('mean_mae', float('inf'))
                })
                
                if score < best_score:
                    best_score = score
                    best_params = {'order': order, 'seasonal_order': seasonal_order}
                    
            except Exception as e:
                print(f"Error with params {order}, {seasonal_order}: {str(e)}")
                continue
    
    return best_params, pd.DataFrame(results).sort_values('mean_rmse')