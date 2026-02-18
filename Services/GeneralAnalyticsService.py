import pandas as pd
from typing import List, Dict, Any


class GeneralAnalyticsService:
    """
    Сервис для нормализации цен акций и подготовки итогового ответа.
    """

    def normalize_prices_from_json(self, json_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Принимает список записей {symbol, date, close}.
        Возвращает DataFrame с колонками date и normalized — средняя нормализованная цена.
        """
        if not json_data:
            return pd.DataFrame(columns=["date", "normalized"])

        df = pd.DataFrame(json_data)
        df["date"] = pd.to_datetime(df["date"])

        # Нормализация: цена относительно первого значения в группе * 100
        df["first_price"] = df.groupby("symbol")["close"].transform("first")
        df["normalized"] = (df["close"] / df["first_price"]) * 100

        # Усреднение по датам
        avg_normalized = (
            df.groupby("date")["normalized"]
            .mean()
            .round(2)
            .reset_index()
        )

        return avg_normalized

    def create_final_json_response(self, json_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Возвращает список словарей для JSON-ответа:
        [{"date": "2024-01-01", "normalized": 100.0}, ...]
        """
        normalized_df = self.normalize_prices_from_json(json_data)

        if normalized_df.empty:
            return []

        return [
            {
                "date": row["date"].strftime("%Y-%m-%d"),
                "normalized": float(row["normalized"]),
            }
            for _, row in normalized_df.iterrows()
        ]