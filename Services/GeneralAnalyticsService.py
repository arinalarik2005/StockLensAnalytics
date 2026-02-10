import pandas as pd
import numpy as np


class GeneralAnalyticsService:
    def normalize_prices_from_json(self, json_data):
        """
        Нормирование цен из JSON данных
        """
        print()
        df = pd.DataFrame(json_data)
        df['date'] = pd.to_datetime(df['date'])

        normalized_dfs = []

        for symbol, group in df.groupby('symbol'):
            group = group.sort_values('date').copy()
            first_price = group['close'].iloc[0]
            group['normalized'] = (group['close'] / first_price) * 100
            normalized_dfs.append(group)

        normalized_df = pd.concat(normalized_dfs, ignore_index=True)

        avg_normalized = normalized_df.groupby('date')['normalized'].mean().reset_index()
        avg_normalized['normalized'] = avg_normalized['normalized'].round(2)

        return avg_normalized

    def create_final_json_response(self, json_data):
        """
           Создает финальный JSON ответ без метаданных
           """
        normalized = self.normalize_prices_from_json(json_data)

        # Преобразуем результат в список словарей с датами в строковом формате
        final_response = [
            {
                "date": row['date'].strftime('%Y-%m-%d'),
                "normalized": float(row['normalized'])
            }
            for _, row in normalized.iterrows()
        ]

        return final_response