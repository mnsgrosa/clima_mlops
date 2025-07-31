from fastapi import FastAPI
from schemas import (
    Metar, Metars, Previsao, Previsoes, StatusMessage, RestrictionMetar, RestrictionPrevisao,
    DistribuicaoMetar, DistribuicoesMetar
)
from client.db_handler import DBHandler
from client.config import DB
from typing import List, Dict, Any
from etl.metar import MetarModelController
import pandas as pd
import pickle

app = FastAPI()
handler = DBHandler(DB)
model_metar = MetarModelController('metar')


@app.get('/get/metar', response_model = Metars)
def get_metar(estacao: str = None, restricao: RestrictionMetar = {}):
    df = handler.get_data(estacao = estacao, restriction = restricao)
    data = df.to_dict(orient = 'records')
    obj = Metars(items = data)
    return obj

@app.get('/get/previsao', response_model = Previsoes)
def get_previsao(cidade: str = None, restricao: RestrictionPrevisao = None):
    df = caller.get_previsao(cidade, restriction = restricao)
    data = df.to_dict(orient = 'records')
    obj = Previsoes(preds = data)
    return obj

@app.get('/get/distribuicao', response_model = DistribuicoesMetar)
def get_distribuicoes():
    df = handler.get_data('distribuicoes_metar')
    data = df.to_dict(orient = 'records')
    obj = DistribuicoesMetar(items = data)
    return obj

@app.post('/post/clima', response_model = StatusMessage)
def post_metar(tempo: Metar):
    try:
        ans = handler.upsert_data('metar', list(Metar.model_fields.keys()), tempo.model_dump().values)
        return StatusMessage(status = ans)
    except Exception as e:
        return StatusMessage(status = ans)

@app.post('/post/previsao', response_model = StatusMessage)
def post_previsao(previsao: Previsao):
    try:
        ans = handler.upsert_data('pred_estacao', list(Previsao.model_fields.keys()), previsao.model_dump().values)
        return StatusMessage(status = ans)
    except Exception as e:
        return StatusMessage(status = ans)

@app.post('/post/distribuicao', response_model = StatusMessage)
def post_distribuicao(distribuicao: DistribuicoesMetar):
    try:
        ans = model_metar.distribuir(distribuicao.items)
        handler.upsert_multiple_data('distribuicoes_metar', list(DistribuicaoMetar.model_fields.keys()), ans)
        return StatusMessage(status = True)
    except Exception as e:
        return StatusMessage(status = False)

@app.get('/train')
def train():
    return