from fastapi import FastAPI
from .schemas import (
    StatusMessage, RestrictionMetar, RestrictionPrevisao, ResponseGet, MetarPost, PrevisoesPost, 
    DistribuicoesPost, DistribuicaoMetar, RestrictionDistribuicoes
)
from .client.db_handler import DBHandler
from .client.config import DB
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

@app.get('/get/distribuicao', response_model = ResponseGet)
def get_distribuicao(restricao: RestrictionDistribuicoes):
    df = handler.get_data('metar_dist', restricao.model_dump())
    obj = ResponseGet(df = df)
    return obj
    
@app.post('/post/metar', response_model = StatusMessage)
def post_metar(tempo: MetarsPost):
    try:
        ans = handler.upsert_multiple_data('metar', list(Metar.items[0].keys()), tempo.model_dump())
        return StatusMessage(status = ans)
    except Exception as e:
        return StatusMessage(status = False, error = e)

@app.post('/post/previsao', response_model = StatusMessage)
def post_previsao(previsoes: PrevisoesPost):
    try:
        colunas = previsao.items[0].model_fields.keys()
        ans = handler.upsert_multiple_data('pred_estacao', colunas, previsoes.model_dump())
        return StatusMessage(status = ans)
    except Exception as e:
        return StatusMessage(status = False, error = e)

@app.post('/post/distribuicao', response_model = StatusMessage)
def post_distribuicao(distribuicao: DistribuicaoPost):
    try:
        colunas = distribuicoes.model_fields.keys()
        ans = handler.upsert_data('metar_dist', colunas, distribuicoes.model_dump())
        return StatusMessage(status = ans)
    except Exception as e:
        return StatusMessage(status = False, error = e)

if __name__ == '__main__':
    app.run()