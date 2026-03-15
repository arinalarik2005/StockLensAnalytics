# services/portfolio.py
# (без изменений, кроме добавленной поддержки max_weight, уже реализовано ранее)
# Приводим финальную версию для полноты.

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from pypfopt import HRPOpt

class PortfolioService:
    @staticmethod
    def _prepare_returns(data: List[Dict[str, Any]], min_common_days: int = 20) -> pd.DataFrame:
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        pivot = df.pivot(index='date', columns='symbol', values='close')
        total_dates = len(pivot)
        missing_stats = pivot.isnull().sum()
        missing_per_ticker = missing_stats[missing_stats > 0].sort_values(ascending=False)
        pivot_clean = pivot.dropna(how='any')
        if pivot_clean.empty:
            msg = "❌ Нет ни одной общей даты для всех выбранных тикеров.\n"
            if not missing_per_ticker.empty:
                msg += "📊 Тикеры с наибольшим числом пропусков:\n"
                for ticker, misses in missing_per_ticker.head(5).items():
                    coverage = (total_dates - misses) / total_dates * 100
                    msg += f"   • {ticker}: пропущено {misses} из {total_dates} дат (присутствует в {coverage:.1f}% дней)\n"
                msg += "💡 Рекомендация: исключите указанные тикеры из запроса."
            else:
                msg += "Возможно, тикеры имеют непересекающиеся временные интервалы."
            raise ValueError(msg)
        if len(pivot_clean) < min_common_days:
            present_counts = pivot.notnull().sum()
            min_present = present_counts.min()
            problematic = present_counts[present_counts == min_present].index.tolist()
            msg = (
                f"⚠️ После удаления пропусков осталось всего {len(pivot_clean)} общих дат "
                f"(требуется минимум {min_common_days}).\n"
                f"🔍 Тикеры с наибольшими пропусками: {problematic} "
                f"(присутствуют только в {min_present} из {total_dates} дат).\n"
                "💡 Рекомендация: исключите указанные тикеры из запроса и повторите попытку."
            )
            raise ValueError(msg)
        returns = pivot_clean.pct_change().dropna()
        if returns.empty:
            raise ValueError("Недостаточно данных для расчёта доходностей.")
        return returns

    @classmethod
    def calculate_metrics(cls, data: List[Dict[str, Any]], weights: Dict[str, float],
                          risk_free_rate: float = 0.05, min_common_days: int = 20,
                          returns: Optional[pd.DataFrame] = None) -> Dict[str, float]:
        if returns is None:
            returns = cls._prepare_returns(data, min_common_days)
        missing = set(weights.keys()) - set(returns.columns)
        if missing:
            raise ValueError(f"Тикеры отсутствуют в данных: {missing}.")
        hrp = HRPOpt(returns)
        hrp.weights = pd.Series(weights)
        exp_return, volatility, sharpe = hrp.portfolio_performance(risk_free_rate=risk_free_rate, frequency=252)
        return {
            "expected_return": round(exp_return * 100, 2),
            "volatility": round(volatility * 100, 2),
            "sharpe_ratio": round(sharpe, 2)
        }

    @classmethod
    def optimize(cls, data: List[Dict[str, Any]], linkage_method: str = 'ward',
                 risk_free_rate: float = 0.05, min_common_days: int = 20,
                 max_weight: Optional[float] = None) -> Dict[str, Any]:
        returns = cls._prepare_returns(data, min_common_days)
        if returns.shape[1] < 2:
            raise ValueError("Для оптимизации нужно минимум 2 актива.")
        hrp = HRPOpt(returns)
        weights = hrp.optimize(linkage_method=linkage_method)
        if max_weight is not None:
            weights = cls._cap_weights(weights, max_weight)
        weights = {k: round(v, 4) for k, v in weights.items()}
        metrics = cls.calculate_metrics(data, weights, risk_free_rate, min_common_days, returns)
        return {"weights": weights, **metrics}

    @staticmethod
    def _cap_weights(weights: Dict[str, float], max_weight: float) -> Dict[str, float]:
        weights = weights.copy()
        excess_total = 0.0
        for k, v in list(weights.items()):
            if v > max_weight:
                excess = v - max_weight
                excess_total += excess
                weights[k] = max_weight
        if excess_total > 0:
            under = {k: v for k, v in weights.items() if v < max_weight}
            if under:
                total_under = sum(under.values())
                for k in under:
                    weights[k] += excess_total * (under[k] / total_under)
            else:
                n = len(weights)
                for k in weights:
                    weights[k] += excess_total / n
        total = sum(weights.values())
        if not np.isclose(total, 1.0):
            weights = {k: v / total for k, v in weights.items()}
        return weights