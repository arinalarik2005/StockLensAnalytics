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


from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from Services.GeneralAnalyticsService import GeneralAnalyticsService

app = FastAPI(title="Stock Analytics API")


class StockItem(BaseModel):
    """Модель входных данных: тикер, дата, цена закрытия."""
    symbol: str
    date: str      # формат YYYY-MM-DD (можно заменить на date, если нужна строгая валидация)
    close: float


@app.post("/general_analytics", summary="Нормализация цен")
def general_analytics(data: List[StockItem]):
    """
    Принимает список объектов StockItem, нормализует цены (относительно первой даты каждого символа)
    и возвращает среднюю нормализованную цену по всем символам для каждой даты.
    """
    try:
        # Преобразуем Pydantic модели в словари (Pydantic v2)
        data_dicts = [item.model_dump() for item in data]

        service = GeneralAnalyticsService()
        result = service.create_final_json_response(data_dicts)

        return {"success": True, "data": result}

    except Exception as e:
        # В реальном проекте лучше использовать конкретные исключения,
        # но для краткости оставлен общий перехват.
        raise HTTPException(status_code=500, detail=str(e))
'''

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import List, Optional, Dict

from Services.GeneralAnalyticsService import GeneralAnalyticsService
from Services.HeatmapForSectors import HeatmapForSectors
from Services.Top10AntyCrisisService import Top10AntiCrisisService

app = FastAPI(title="Stock Analytics API")

# ---------- Модели для /general_analytics ----------
class StockItem(BaseModel):
    symbol: str
    date: str
    close: float

# ---------- Модели для /anti-crisis-top10 ----------
class StockDataItem(BaseModel):
    symbol: str = Field(..., description="Тикер (MOEX или акция)")
    date: str = Field(..., description="Дата в формате YYYY-MM-DD")
    close: float = Field(..., description="Цена закрытия")
    avg_dividend: Optional[float] = Field(None, description="Средняя дивидендная доходность (только для акций)")
    value: Optional[str] = Field(None, description="Объём торгов в рублях (только для акций)")

class AntiCrisisResultItem(BaseModel):
    ticker: str
    relative_strength: float
    resilient_ratio: float
    dividend_yield: float
    avg_volume: float
    score: float
    rank: int


# ---------- Модели для /heatmap-sectors ----------
class AntiCrisisResponse(BaseModel):
    success: bool
    data: List[AntiCrisisResultItem]

class SectorStockItem(BaseModel):
    """Одна запись для расчёта корреляций между секторами."""
    symbol: str = Field(..., description="Тикер")
    date: str = Field(..., description="Дата в формате YYYY-MM-DD")
    close: float = Field(..., gt=0, description="Цена закрытия")
    sector: str = Field(..., description="Название сектора")

class SectorCorrelationResponse(BaseModel):
    sectors: List[str]
    matrix: List[List[float]]
    stocks_per_sector: Dict[str, int]

# ---------- Эндпоинт 1: Нормализация цен ----------
@app.post("/general_analytics", summary="Нормализация цен")
def general_analytics(data: List[StockItem]):
    """
    Принимает список объектов StockItem, нормализует цены (относительно первой даты каждого символа)
    и возвращает среднюю нормализованную цену по всем символам для каждой даты.
    """
    try:
        data_dicts = [item.model_dump() for item in data]
        service = GeneralAnalyticsService()
        result = service.create_final_json_response(data_dicts)
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------- Эндпоинт 2: Антикризисный топ-10 ----------
@app.post("/anti-crisis-top10", response_model=AntiCrisisResponse)
def anti_crisis_top10(
    data: List[StockDataItem],
    liquidity_min: float = Query(50, description="Минимальный средний дневной объём (млн руб.)")
):
    """
    Принимает исторические данные по индексу MOEX и акциям,
    вычисляет антикризисный рейтинг и возвращает топ-10 акций.
    """
    try:
        data_dicts = [item.model_dump() for item in data]
        service = Top10AntiCrisisService()
        result = service.run_analysis(data_dicts, liquidity_min)
        return AntiCrisisResponse(success=True, data=result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

# ---------- Эндпоинт 3: Тепловая карта по секторам ----------
@app.post("/sector-correlations", response_model=SectorCorrelationResponse)
def sector_correlations(data: List[SectorStockItem]):
    """
    Принимает исторические данные по акциям с указанием сектора.
    Возвращает матрицу средних попарных корреляций между секторами.
    """
    try:
        # Преобразуем Pydantic модели в словари (Pydantic v2)
        data_dicts = [item.model_dump() for item in data]
        service = HeatmapForSectors()
        result = service.compute_sector_correlations(data_dicts)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")