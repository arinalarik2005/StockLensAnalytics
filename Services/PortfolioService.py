import pandas as pd
import numpy as np
from typing import List, Dict, Any

class PortfolioService:
    """
    Сервис для расчёта метрик портфеля: доходность, риск, корреляция, Шарп, диверсификация.
    """

    @staticmethod
    def calculate_metrics(data: List[Dict[str, Any]], risk_free_rate: float = 0.05) -> Dict[str, float]:
        """
        Принимает список записей с полями symbol, date, close, percentage.
        Возвращает словарь с метриками.
        """
        if not data:
            raise ValueError("Пустой входной список")

        df = pd.DataFrame(data)
        required = {'symbol', 'date', 'close', 'percentage'}
        if not required.issubset(df.columns):
            raise ValueError(f"Отсутствуют поля: {required - set(df.columns)}")

        df['date'] = pd.to_datetime(df['date'])

        # Проверяем, что веса постоянны для каждого тикера
        weights_dict = {}
        for symbol, group in df.groupby('symbol'):
            percentages = group['percentage'].unique()
            if len(percentages) > 1:
                raise ValueError(f"Для тикера {symbol} найдены разные percentage: {percentages}")
            weights_dict[symbol] = percentages[0]

        total_weight = sum(weights_dict.values())
        if not np.isclose(total_weight, 1.0):
            raise ValueError(f"Сумма весов должна быть 1, получено {total_weight}")

        # Строим сводную таблицу
        pivot = df.pivot(index='date', columns='symbol', values='close')
        missing = set(weights_dict.keys()) - set(pivot.columns)
        if missing:
            raise ValueError(f"Тикеры отсутствуют в данных: {missing}")

        tickers_order = list(weights_dict.keys())
        pivot = pivot[tickers_order]

        # Доходности
        returns = np.log(pivot / pivot.shift(1)).dropna()
        if returns.empty:
            raise ValueError("Недостаточно данных для расчёта доходностей")

        ann_factor = 252
        mean_returns = returns.mean() * ann_factor * 100          # % годовых
        cov_matrix = returns.cov() * ann_factor

        weights = np.array([weights_dict[t] for t in tickers_order])

        portfolio_return = np.dot(weights, mean_returns)

        portfolio_variance = np.dot(weights.T, np.dot(cov_matrix, weights))
        portfolio_volatility_pct = np.sqrt(portfolio_variance) * 100   # в %

        # Средняя корреляция
        corr_matrix = returns.corr()
        upper_tri = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
        avg_corr = upper_tri.stack().mean()
        if np.isnan(avg_corr):
            avg_corr = 0.0

        # Коэффициент Шарпа
        if risk_free_rate > 0:
            excess_return = portfolio_return - risk_free_rate * 100
            sharpe = excess_return / portfolio_volatility_pct if portfolio_volatility_pct != 0 else 0.0
        else:
            sharpe = 0.0

        div_index = max(0.0, min(1.0, 1.0 - avg_corr))

        return {
            "expected_return": round(portfolio_return, 2),
            "volatility": round(portfolio_volatility_pct, 2),
            "average_correlation": round(avg_corr, 4),
            "sharpe_ratio": round(sharpe, 2),
            "diversification_index": round(div_index, 4)
        }