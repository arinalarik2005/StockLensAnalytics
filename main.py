from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import List, Optional, Dict

from Services.GeneralAnalyticsService import GeneralAnalyticsService
from Services.HeatmapForSectors import HeatmapForSectors
from Services.Top10AntyCrisisService import Top10AntiCrisisService
from Services.PortfolioService import PortfolioService
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

# ---------- Модели для /portfolio/own-weights ----------
class OwnWeightsItem(BaseModel):
    symbol: str
    date: str
    close: float
    percentage: float

class OwnWeightsRequest(BaseModel):

    def validate_weights(cls, v):
        # Проверяем постоянство веса для каждого тикера
        weight_by_symbol = {}
        for item in v:
            sym = item.symbol
            w = item.percentage
            if sym in weight_by_symbol:
                if abs(weight_by_symbol[sym] - w) > 1e-6:
                    raise ValueError(f"Для тикера {sym} найдены разные значения Percentage")
            else:
                weight_by_symbol[sym] = w
        # Проверяем сумму весов
        total = sum(weight_by_symbol.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"Сумма весов должна быть 1, получено {total}")
        return v

class OwnWeightsResponse(BaseModel):
    expected_return: float
    volatility: float
    sharpe_ratio: float

# ---------- Модели для /portfolio/optimize ----------
class OptimizeItem(BaseModel):
    symbol: str
    date: str
    close: float

class OptimizeResponse(BaseModel):
    weights: Dict[str, float]
    expected_return: float
    volatility: float
    sharpe_ratio: float

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


# ---------- Эндпоинт 4 : Рассчет метрик для портфеля с заданными весами /portfolio/own-weights ----------
@app.post("/portfolio/own-weights", response_model=OwnWeightsResponse)
def own_weights(
    data: List[OwnWeightsItem],  # FastAPI автоматически распарсит список
    risk_free_rate: float = Query(0.05, ge=0, le=1, description="Годовая безрисковая ставка")
):
    """
    Рассчитать метрики для портфеля с заданными весами.
    Каждая запись должна содержать symbol, date, close, percentage.
    """
    # Валидация уже выполнена через модель, но нам нужно извлечь уникальные веса
    weight_by_symbol = {}
    for item in data:
        sym = item.symbol
        w = item.percentage
        if sym not in weight_by_symbol:
            weight_by_symbol[sym] = w


    try:
        data_dicts = [item.dict() for item in data]
        metrics = PortfolioService().calculate_metrics(
            data=data_dicts,
            weights=weight_by_symbol,
            risk_free_rate=risk_free_rate
        )
        return metrics
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

# ---------- Эндпоинт 5: Рассчет метрик для портфеля без заданных весов /portfolio/optimize ----------
@app.post("/portfolio/optimize", response_model=OptimizeResponse)
def optimize(
    data: List[OptimizeItem],
    risk_free_rate: float = Query(0.05, ge=0, le=1, description="Годовая безрисковая ставка"),
    linkage_method: str = Query("ward", description="Метод кластеризации: single, complete, average, ward")
):
    """
    Оптимизировать портфель методом HRP.
    Принимает только symbol, date, close. Возвращает оптимальные веса и метрики.
    """
    try:
        data_dicts = [item.dict() for item in data]
        result = PortfolioService.optimize(
            data=data_dicts,
            linkage_method=linkage_method,
            risk_free_rate=risk_free_rate
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")