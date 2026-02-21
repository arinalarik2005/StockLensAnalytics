import pandas as pd
from typing import List, Dict, Any
from pypfopt import HRPOpt

class PortfolioService:
    """Единый сервис для работы с портфелем."""
    def _prepare_returns(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        pivot = df.pivot(index='date', columns='symbol', values='close')
        pivot = pivot.dropna(axis=1, how='any')
        returns = pivot.pct_change().dropna()
        if returns.empty:
            raise ValueError("Недостаточно данных для расчёта доходностей")
        return returns


    def calculate_metrics(
        self,
        data: List[Dict[str, Any]],
        weights: Dict[str, float],
        risk_free_rate: float = 0.05
    ) -> Dict[str, float]:
        returns = self._prepare_returns(data)
        missing = set(weights.keys()) - set(returns.columns)
        if missing:
            raise ValueError(f"Тикеры отсутствуют в данных: {missing}")

        hrp = HRPOpt(returns)
        hrp.weights = pd.Series(weights)
        exp_return, volatility, sharpe = hrp.portfolio_performance(
            risk_free_rate=risk_free_rate,
            frequency=252
        )
        return {
            "expected_return": round(exp_return * 100, 2),
            "volatility": round(volatility * 100, 2),
            "sharpe_ratio": round(sharpe, 2)
        }


    def optimize(
        self,
        data: List[Dict[str, Any]],
        linkage_method: str = 'ward',
        risk_free_rate: float = 0.05
    ) -> Dict[str, Any]:
        returns = self._prepare_returns(data)
        if returns.shape[1] < 2:
            raise ValueError("Для оптимизации нужно минимум 2 актива")
        hrp = HRPOpt(returns)
        weights = hrp.optimize(linkage_method=linkage_method)
        weights = {k: round(v, 4) for k, v in weights.items()}
        metrics = self.calculate_metrics(data, weights, risk_free_rate)
        return {"weights": weights, **metrics}