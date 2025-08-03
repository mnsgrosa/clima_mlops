from pydantic import BaseModel, Field
from typing import List, Optional, Tuple, Dict, Any
from pandas import DataFrame

class MetarPost(BaseModel):
    estacao: str = Field(...)
    dia: int = Field(...)
    mes: int = Field(...)
    ano: int = Field(...)
    pressao: float = Field(...)
    temperatura: float = Field(...)
    tempo: int = Field(...)
    umidade: float = Field(...)
    vento_dir_seno: float = Field(...)
    vento_dir_cosseno: float = Field(...)
    vento_int: float = Field(...)
    visibilidade: float = Field(...)

class MetarsPost(BaseModel):
    items: List[MetarPost] = Field(...)

class ResponseGet(BaseModel):
    df: Dict[str, Any] = Field(...)

class Previsao(BaseModel):
    cidade: str = Field(...)
    data: str = Field(...)
    dia: str = Field(...)
    tempo: str = Field(...)
    maxima: float = Field(...)
    minima: float = Field(...)
    iuv: float = Field(...)
    distancia: int = Field(...)

class PrevisoesPost(BaseModel):
    preds: List[Previsao] = Field(...)

class RestrictionMetar(BaseModel):
    estacao: Optional[str] = None
    dia: Optional[int] = None
    mes: Optional[int] = None
    ano: Optional[int] = None
    pressao: Optional[float]= None
    temperatura: Optional[float] = None
    tempo: Optional[str] = None
    umidade: Optional[float] = None
    vento_dir_seno: Optional[float] = None
    vento_dir_cosseno: Optional[float] = None
    vento_int: Optional[int] = None
    visibilidade: Optional[int] = None

class RestrictionPrevisao(BaseModel):
    cidade: Optional[str] = None
    data: Optional[str] = None
    dia: Optional[str] = None
    tempo: Optional[str] = None
    maxima: Optional[str] = None
    minima: Optional[str] = None
    iuv: Optional[str] = None
    diferenca_dias: Optional[int] = None

class StatusMessage(BaseModel):
    status: bool = Field(...)
    error: Optional[str] = Field(default = None)

class DistribuicaoMetar(BaseModel):
    estacao: str = Field(...)
    pressao: float = Field(...)
    temperatura: float = Field(...)
    tempo: int = Field(...)
    umidade: float = Field(...)
    vento_dir_seno: float = Field(...)
    vento_dir_cosseno: float = Field(...)
    vento_int: float = Field(...)
    umidade: float = Field(...)
    visibilidade: float = Field(...)

class DistribuicoesPost(BaseModel):
    items: List[DistribuicaoMetar] = Field(...)