from fastapi import FastAPI
from schemas import Metar, Previsao, Previsoes
from client.db_handler import DBHandler
from client.config import DB
from typing import List, Dict, Any

app = FastAPI()
handler = DBHandler(DB)

@app.get('/get/metar', response_model = Metar)
def get_metar(estacao: str = None, restricao: RestrictionMetar = None):
    return handler.get_data(estacao = estacao, restriction = restricao)

@app.get('/get/previsao', response_model = Previsoes)
def get_previsao(cidade: str = None, restricao: RestrictionPrevisao = None):
    return caller.get_previsao(cidade, restriction = restricao)

@app.post('/post/clima')
def post_metar(tempo: Metar):
    try:
        handler.upsert_data('metar', list(Metar.model_fields.keys()), tempo.model_dump().values)
        return {'status': True}
    except Exception as e:
        return {'status': False}

@app.post('/post/previsao')
def post_previsao(previsao: Previsao):
    try:
        handler.upsert_data('pred_estacao', list(Previsao.model_fields.keys()), previsao.model_dump().values)
        return {'status': True}
    except Exception as e:
        return {'status': False}

        