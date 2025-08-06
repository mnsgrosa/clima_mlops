# Clima MLOps
![image](./image.png)

# CPTec ta vivo!!!
(APIbrasil esta hospedando agora!)[https://brasilapi.com.br/docs#tag/CPTEC/operation/searchcities(/cptec/v1/cidade/:cityName)]

## Descrição (Português)

Este projeto é um orquestrador de MLOps para dados climáticos. O objetivo é construir um pipeline que coleta dados por 30 dias, treina um modelo inicial e, em seguida, começa a fazer previsões diárias. O sistema também inclui um mecanismo de detecção de data drift que é verificado mensalmente para decidir se o modelo precisa ser retreinado.

### Funcionalidades

*   **Coleta de Dados:** Coleta dados climáticos por 30 dias para construir o dataset inicial.
*   **Treinamento Inicial:** Treina um modelo de previsão do tempo após a coleta inicial de dados.
*   **Previsão:** Realiza previsões diárias do clima.
*   **Detecção de Drift:** Verifica mensalmente se há data drift nos dados para garantir a qualidade das previsões.
*   **Retreinamento:** Dispara o retreinamento do modelo caso seja detectado um data drift significativo.

## Description (English)

This project is an MLOps orchestrator for weather data. The goal is to build a pipeline that collects data for 30 days, trains an initial model, and then starts making daily predictions. The system also includes a data drift detection mechanism that is checked monthly to decide if the model needs to be retrained.

### Features

*   **Data Collection:** Collects weather data for 30 days to build the initial dataset.
*   **Initial Training:** Trains a weather forecasting model after the initial data collection.
*   **Prediction:** Performs daily weather forecasts.
*   **Drift Detection:** Checks for data drift monthly to ensure the quality of the predictions.
*   **Retraining:** Triggers model retraining if significant data drift is detected.
