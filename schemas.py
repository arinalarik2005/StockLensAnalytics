from pydantic import BaseModel, Field
from typing import List, Optional, Dict


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
# schemas.py
from pydantic import BaseModel, Field
from typing import List
from datetime import date

class QuoteItem(BaseModel):
    symbol: str
    date: date
    close: float

class OptimizeRequest(BaseModel):
    m1: int = Field(..., ge=1, le=4, description="Ответ на вопрос 1 (1-4)")
    m2: int = Field(..., ge=1, le=4, description="Ответ на вопрос 2 (1-4)")
    m3: int = Field(..., ge=1, le=4, description="Ответ на вопрос 3 (1-4)")
    m4: int = Field(..., ge=1, le=4, description="Ответ на вопрос 4 (1-4)")
    quotes: List[QuoteItem]

class OptimizeResponse(BaseModel):
    weights: dict[str, float]
    expected_return: float
    volatility: float
    sharpe_ratio: float
    risk_profile: str  # conservative, moderate, aggressive
    experience_level: str  # novice, intermediate, expert