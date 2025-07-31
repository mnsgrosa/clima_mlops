from fastapi import FastAPI
from client.api_clima import CPTECApiCaller

@app.get("/clima")
