import pandas as pd
import numpy as np
from typing import List, Dict, Any, Union


class HeatmapForSectors:
    """
    Сервис для расчёта корреляционной матрицы между секторами.
    """

    def compute_sector_correlations(self, data: List[Dict[str, Union[str, float]]]) -> Dict[str, Dict[str, Union[str, float]]]:
        """
        Принимает список записей с полями Symbol, Date, Close, Sector.
        Возвращает структуру:
        {
            "sectors": list[str],
            "matrix": list[list[float]],
            "stocks_per_sector": dict[str, int]
        }
        """
        if not data:
            raise ValueError("Входной список пуст")

        df = pd.DataFrame(data)
        required_cols = {'symbol', 'date', 'close', 'sector'}
        if not required_cols.issubset(df.columns):
            raise ValueError(f"Отсутствуют обязательные колонки: {required_cols - set(df.columns)}")

        # Дата -> datetime
        df['date'] = pd.to_datetime(df['date'])

        # Сортируем сектора для стабильного порядка
        sectors = sorted(df['sector'].unique())
        n_sectors = len(sectors)

        # Широкий формат: даты × тикеры, значения = цена
        pivot = df.pivot(index='date', columns='symbol', values='close')
        returns = pivot.pct_change().dropna()

        # Словарь тикер -> секторs
        ticker_sector = df[['symbol', 'sector']].drop_duplicates().set_index('symbol')['sector'].to_dict()

        # Матрица корреляций между секторами
        corr_matrix = np.zeros((n_sectors, n_sectors))
        stocks_per_sector = {}

        for i, s1 in enumerate(sectors):
            tickers1 = [t for t, sec in ticker_sector.items() if sec == s1]
            stocks_per_sector[s1] = len(tickers1)
            corr_matrix[i, i] = 1.0

            for j, s2 in enumerate(sectors):
                if i >= j:
                    continue  # заполняем только верхний треугольник, потом симметрично

                tickers2 = [t for t, sec in ticker_sector.items() if sec == s2]

                corr_values = []
                for t1 in tickers1:
                    if t1 not in returns.columns:
                        continue
                    for t2 in tickers2:
                        if t2 not in returns.columns:
                            continue
                        corr = returns[t1].corr(returns[t2])
                        if not np.isnan(corr):
                            corr_values.append(corr)

                avg_corr = round(np.mean(corr_values), 4) if corr_values else 0.0
                corr_matrix[i, j] = avg_corr
                corr_matrix[j, i] = avg_corr

        return {
            "sectors": sectors,
            "matrix": corr_matrix.tolist(),
            "stocks_per_sector": stocks_per_sector
        }