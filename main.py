'''
import ast
import json
from dataclasses import dataclass
from typing import List

from fastapi import FastAPI, Request

from Services.GeneralAnalyticsService import GeneralAnalyticsService

app = FastAPI()

@dataclass
class Test:
    symbol: str
    date: str
    close: float
    def __init__(self, symbol: str, date: str, close: float):
        self.symbol = symbol
        self.date = str
        self.close = close


@app.post("/general_analytics")
async def general_analytics(request, data: Test):
    #raw_body = await request.body()
    #raw_text = raw_body.decode("utf-8")
    print(999999999999999999999999999999999999)
    print(type(request))
    l = json.loads(request)
    print(type(data))
    analytics_service = GeneralAnalyticsService()
    analytics_service.create_final_json_response(l)

    return {"ok": 200}
'''
from dataclasses import dataclass

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from Services.GeneralAnalyticsService import GeneralAnalyticsService

app = FastAPI()


# Простая модель данных
@dataclass
class StockItem(BaseModel):
    symbol: str
    date: str
    close: float


# Эндпоинт который ожидает список данных
@app.post("/general_analytics")
async def general_analytics(data: List[StockItem]):
    """
    Простой эндпоинт для нормализации цен

    Просто передайте массив объектов:
    [
      {"symbol": "AAPL", "date": "2024-01-01", "close": 150.0},
      {"symbol": "GOOGL", "date": "2024-01-01", "close": 2800.0}
    ]
    """
    try:
        # Конвертируем в список словарей
        data_list = [item.dict() for item in data]

        # Используем сервис
        service = GeneralAnalyticsService()
        result = service.create_final_json_response(data_list)

        return {
            "success": True,
            "data": result
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))