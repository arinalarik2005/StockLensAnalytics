import pandas as pd
import numpy as np
from typing import List, Dict, Union, Any


class HeatmapForSectors:
    """
    Сервис для расчёта корреляционной матрицы между секторами.
    Диагональ матрицы всегда равна 1.0.
    """

    def compute_sector_correlations(
        self,
        data: List[Dict[str, Union[str, float]]]
    ) -> Dict[str, Any]:

        if not data:
            raise ValueError("Входной список пуст")

        df = pd.DataFrame(data)

        required_cols = {'symbol', 'date', 'close', 'sector'}
        if not required_cols.issubset(df.columns):
            raise ValueError(
                f"Отсутствуют обязательные колонки: "
                f"{required_cols - set(df.columns)}"
            )

        # Приведение и сортировка дат
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')

        # Pivot: даты × тикеры
        pivot = df.pivot(index='date', columns='symbol', values='close')

        # Доходности
        returns = pivot.pct_change()

        # Полная матрица корреляций акций
        stock_corr = returns.corr()

        # Сектора и маппинг
        sectors = sorted(df['sector'].unique())
        sector_to_tickers = (
            df[['symbol', 'sector']]
            .drop_duplicates()
            .groupby('sector')['symbol']
            .apply(list)
            .to_dict()
        )

        n_sectors = len(sectors)
        corr_matrix = np.full((n_sectors, n_sectors), np.nan)
        stocks_per_sector = {
            sector: len(sector_to_tickers.get(sector, []))
            for sector in sectors
        }

        for i, s1 in enumerate(sectors):
            tickers1 = sector_to_tickers.get(s1, [])
            corr_matrix[i, i] = 1.0  # по ТЗ

            for j in range(i + 1, n_sectors):
                s2 = sectors[j]
                tickers2 = sector_to_tickers.get(s2, [])

                if not tickers1 or not tickers2:
                    continue

                # Подматрица корреляций между двумя секторами
                sub_corr = stock_corr.loc[tickers1, tickers2]

                # Среднее без NaN
                values = sub_corr.values.flatten()
                values = values[~np.isnan(values)]

                if len(values) > 0:
                    avg_corr = round(float(values.mean()), 4)
                    corr_matrix[i, j] = avg_corr
                    corr_matrix[j, i] = avg_corr

        return {
            "sectors": sectors,
            "matrix": corr_matrix.tolist(),
            "stocks_per_sector": stocks_per_sector
        }