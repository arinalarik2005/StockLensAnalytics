import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Union


class Top10AntiCrisisService:
    """
    Сервис для поиска топ-10 акций, наиболее устойчивых к рыночным падениям.
    """

    def prepare_data(self, json_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not json_data:
            raise ValueError("Пустой входной список")

        df = pd.DataFrame(json_data)
        df['date'] = pd.to_datetime(df['date'])

        moex_df = df[df['symbol'] == 'MOEX'][['date', 'close']].copy()
        if moex_df.empty:
            raise ValueError("Нет данных по индексу MOEX")

        stocks_df = df[df['symbol'] != 'MOEX'].copy()
        if stocks_df.empty:
            raise ValueError("Нет данных по акциям")

        # Переименовываем поля
        stocks_df = stocks_df.rename(columns={
            'symbol': 'Symbol',
            'avg_dividend': 'AvgDividend',
            'value': 'value'
        })

        # Преобразуем value из строки в число (если возможно)
        # Если преобразование не удаётся или None, ставим 0
        stocks_df['value'] = pd.to_numeric(stocks_df['value'], errors='coerce').fillna(0).astype(int)

        stocks_dict = {}
        for symbol, group in stocks_df.groupby('Symbol'):
            stocks_dict[symbol] = group[['date', 'close', 'value', 'AvgDividend']].copy()

        return {'moex': moex_df, 'stocks': stocks_dict}

    def find_stress_days(self, moex_data: pd.DataFrame, threshold: float = -1.0) -> List[str]:
        """Возвращает список дат, когда дневная доходность MOEX ≤ threshold."""
        returns = moex_data['close'].pct_change() * 100
        stress_mask = returns.iloc[1:] <= threshold
        return moex_data['date'].iloc[1:][stress_mask].tolist()

    @staticmethod
    def calculate_anti_crisis_score(
            stock_data: pd.DataFrame,
            stress_dates: List[str],
            moex_data: pd.DataFrame
    ) -> Optional[Dict[str, float]]:
        """Рассчитывает метрики устойчивости для одной акции с выравниванием по датам."""
        # Оставляем только стрессовые дни
        stress_stock = stock_data[stock_data['date'].isin(stress_dates)].copy()
        stress_moex = moex_data[moex_data['date'].isin(stress_dates)].copy()

        # Объединяем по дате, чтобы гарантировать одинаковые дни для обоих рядов
        merged = pd.merge(
            stress_stock[['date', 'close']],
            stress_moex[['date', 'close']],
            on='date',
            suffixes=('_stock', '_moex')
        )

        if len(merged) < 2:  # нужно минимум 2 точки для pct_change
            return None

        # Доходности (pct_change даст NaN в первой строке)
        stock_returns = merged['close_stock'].pct_change().dropna() * 100
        moex_returns = merged['close_moex'].pct_change().dropna() * 100

        if len(stock_returns) == 0:
            return None

        # 1. Средняя относительная сила
        relative_strength = (stock_returns.values - moex_returns.values).mean()

        # 2. Доля дней, когда акция показала себя лучше рынка
        better_days = sum(stock_returns.values > moex_returns.values)
        resilient_ratio = better_days / len(stock_returns)

        # 3. Дивидендная доходность (последнее значение)
        dividend_yield = stock_data['AvgDividend'].iloc[-1] if 'AvgDividend' in stock_data.columns else 0.0

        # 4. Средний объём (млн руб.)
        avg_volume = stock_data['value'].mean() / 1_000_000 if 'value' in stock_data.columns else 0.0

        # Итоговый скор (формула из прототипа)
        score = (relative_strength * 2) + (resilient_ratio * 100) + dividend_yield

        return {
            'relative_strength': round(relative_strength, 2),
            'resilient_ratio': round(resilient_ratio, 3),
            'dividend_yield': round(dividend_yield, 2),
            'avg_volume': round(avg_volume, 2),
            'score': round(score, 2)
        }

    def get_anti_crisis_top10(
        self,
        data: Dict[str, Any],
        liquidity_min: float = 50.0
    ) -> pd.DataFrame:
        """
        Основной аналитический метод.
        Возвращает DataFrame с колонками:
        ticker, relative_strength, resilient_ratio, dividend_yield, avg_volume, score, rank
        """
        stress_dates = self.find_stress_days(data['moex'], threshold=-1.0)
        if not stress_dates:
            return pd.DataFrame()

        results = []
        for ticker, stock_df in data['stocks'].items():
            avg_vol = stock_df['value'].mean() / 1_000_000 if 'value' in stock_df.columns else 0
            if avg_vol < liquidity_min:
                continue

            score_data = self.calculate_anti_crisis_score(stock_df, stress_dates, data['moex'])
            if score_data:
                results.append({'ticker': ticker, **score_data})

        if not results:
            return pd.DataFrame()

        df_results = pd.DataFrame(results)
        df_results = df_results.sort_values('score', ascending=False).head(10)
        df_results['rank'] = range(1, len(df_results) + 1)
        return df_results

    def run_analysis(
        self,
        json_data: List[Dict[str, Any]],
        liquidity_min: float = 50.0
    ) -> List[Dict[str, Any]]:
        """
        Публичный метод: принимает «сырой» список словарей,
        возвращает список словарей с топ-10 акциями.
        """
        prepared = self.prepare_data(json_data)
        df_top10 = self.get_anti_crisis_top10(prepared, liquidity_min)
        return [] if df_top10.empty else df_top10.to_dict(orient='records')