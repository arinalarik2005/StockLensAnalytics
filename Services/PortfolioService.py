import pandas as pd
import numpy as np
from typing import List, Dict, Any
from pypfopt import HRPOpt

class PortfolioService:
    """Единый сервис для работы с портфелем."""

    @staticmethod
    def _prepare_returns(
        data: List[Dict[str, Any]],
        min_common_days: int = 20
    ) -> pd.DataFrame:
        """
        Преобразует входные данные в DataFrame дневных доходностей.
        Оставляет только даты, по которым есть данные по ВСЕМ тикерам.
        При недостатке данных выбрасывает исключение с указанием проблемных тикеров.
        """
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        pivot = df.pivot(index='date', columns='symbol', values='close')

        total_dates = len(pivot)
        # Статистика пропусков по каждому тикеру
        missing_stats = pivot.isnull().sum()
        missing_per_ticker = missing_stats[missing_stats > 0].sort_values(ascending=False)

        # Удаляем строки, где есть хотя бы один пропуск
        pivot_clean = pivot.dropna(how='any')

        # Случай 1: нет ни одной общей даты
        if pivot_clean.empty:
            msg = "❌ Нет ни одной общей даты для всех выбранных тикеров за 5-летний период.\n"
            if not missing_per_ticker.empty:
                msg += "📊 Тикеры с наибольшим числом пропусков:\n"
                for ticker, misses in missing_per_ticker.head(5).items():
                    coverage = (total_dates - misses) / total_dates * 100
                    msg += f"   • {ticker}: пропущено {misses} из {total_dates} дат (присутствует в {coverage:.1f}% дней)\n"
                msg += "💡 Рекомендация: исключите указанные тикеры из запроса."
            else:
                msg += "Возможно, тикеры имеют непересекающиеся временные интервалы."
            raise ValueError(msg)

        # Случай 2: общих дат меньше минимума
        if len(pivot_clean) < min_common_days:
            # Анализируем, какие тикеры имеют наименьшее количество дней
            present_counts = pivot.notnull().sum()
            min_present = present_counts.min()
            problematic = present_counts[present_counts == min_present].index.tolist()

            msg = (
                f"⚠️ После удаления пропусков осталось всего {len(pivot_clean)} общих дат "
                f"(требуется минимум {min_common_days}) за 5-летний период.\n"
                f"🔍 Тикеры с наибольшими пропусками: {problematic} "
                f"(присутствуют только в {min_present} из {total_dates} дат).\n"
                "💡 Рекомендация: исключите указанные тикеры из запроса и повторите попытку."
            )
            raise ValueError(msg)

        # Расчёт доходностей
        returns = pivot_clean.pct_change().dropna()
        if returns.empty:
            raise ValueError("Недостаточно данных для расчёта доходностей (даже после удаления пропусков).")
        return returns

    @classmethod
    def calculate_metrics(
        cls,
        data: List[Dict[str, Any]],
        weights: Dict[str, float],
        risk_free_rate: float = 0.05,
        min_common_days: int = 20
    ) -> Dict[str, float]:
        """
        Рассчитывает метрики портфеля для заданных весов.
        """
        returns = cls._prepare_returns(data, min_common_days)

        missing = set(weights.keys()) - set(returns.columns)
        if missing:
            raise ValueError(
                f"Тикеры отсутствуют в данных после обработки пропусков: {missing}. "
                "Возможно, по ним вообще нет общих дат с остальными."
            )

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

    @classmethod
    def optimize(
        cls,
        data: List[Dict[str, Any]],
        linkage_method: str = 'ward',
        risk_free_rate: float = 0.05,
        min_common_days: int = 20
    ) -> Dict[str, Any]:
        """
        Оптимизирует веса методом HRP.
        """
        returns = cls._prepare_returns(data, min_common_days)

        if returns.shape[1] < 2:
            raise ValueError("Для оптимизации нужно минимум 2 актива с общими датами.")

        hrp = HRPOpt(returns)
        weights = hrp.optimize(linkage_method=linkage_method)
        weights = {k: round(v, 4) for k, v in weights.items()}
        metrics = cls.calculate_metrics(data, weights, risk_free_rate, min_common_days)
        return {"weights": weights, **metrics}