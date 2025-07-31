from pydantic import BaseModel, Field
from typing import List, Optional

class Metar(BaseModel):
    data: str = Field(...)
    pressao: float = Field(...)
    temperatura: float = Field(...)
    tempo: str = Field(...)
    tempo_desc: str = Field(...)
    umidade: float = Field(...)
    vento_dir: str = Field(...)
    vento_int: float = Field(...)
    visibilidade: float = Field(...)

class Previsao(BaseModel):
    cidade: str = Field(...)
    data: str = Field(...)
    dia: str = Field(...)
    tempo: str = Field(...)
    maxima: float = Field(...)
    minima: float = Field(...)
    iuv: float = Field(...)

class Previsoes(BaseModel):
    preds: List[Previsao] = Field(...)

class RestrictionMetar(BaseModel):
    estacao: Optinal[str] = None
    data: Optional[str] = None
    pressao: Option[float]= None
    temperatura: Optional[float] = None
    tempo: Optional[str] = None
    tempo_desc: Optional[str] = None
    umidade: Optional[float] = None
    vento_dir: Optional[str] = None
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