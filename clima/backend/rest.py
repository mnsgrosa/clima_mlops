from fastapi import FastAPI
from schemas import (
    StatusMessage, RestrictionMetar, RestrictionPrevisao, ResponseGet, MetarsPost, PrevisoesPost, 
    DistribuicoesPost, DistribuicaoMetar
)
from client.db_handler import DBHandler
from client.config import DB
from typing import List, Dict, Any
import pandas as pd

app = FastAPI()
handler = DBHandler(DB)

@app.get('/get/metar', response_model = ResponseGet)
def get_metar(restricao: RestrictionMetar = RestrictionMetar()):
    df = handler.get_data('metar', restriction = restricao.model_dump())
    obj = ResponseGet(df = df)
    return obj

@app.get('/get/previsao', response_model = ResponseGet)
def get_previsao(restricao: RestrictionPrevisao):
    df = caller.get_data('pred_estacao', restriction = restricao.model_dump())
    obj = ResponseGet(df = df)
    return obj

@app.post('/post/clima', response_model = StatusMessage)
def post_metar(tempo: MetarsPost):
    try:
        colunas = tempo.items[0].model_fields.keys()
        ans = handler.upsert_data('metar', list(Metar.items.model_fields.keys()), tempo.model_dump())
        return StatusMessage(status = ans)
    except Exception as e:
        return StatusMessage(status = ans, error = e)

@app.post('/post/previsao', response_model = StatusMessage)
def post_previsao(previsoes: PrevisoesPost):
    try:
        colunas = previsao.items[0].model_fields.keys()
        ans = handler.upsert_multiple_data('pred_estacao', colunas, previsoes.model_dump())
        return StatusMessage(status = ans)
    except Exception as e:
        return StatusMessage(status = ans, error = e)