import pandas as pd
import httpx
import mlflow.sklearn
from sklearn.metrics import r2_score, mean_squared_error
from math import sin, cos, radians
from sklearn.preprocessing import LabelEncoder

class MetarModelController:
    def __init__(self, model_name: str, url:str = 'http://localhost:8000'):
        self.model_uri = f'models:/{model_name}/latest'
        self.model = self.init(model_uri)
    
    def get_metar(self, estacao: str = 'SBRF'):
        with httpx.Client() as client:
            response = client.get(self.url + f'/get/metar', params = {'estacao': estacao})
            response.raise_for_status()
        self.db_data = pd.DataFrame(response.json())


    def init(self, model_path):
        return mlflow.sklearn.load_model(model_uri)

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
        pass

    def train(self):
        pass