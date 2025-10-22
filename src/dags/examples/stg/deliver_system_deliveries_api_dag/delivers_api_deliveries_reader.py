from datetime import datetime
from typing import Dict, List, Tuple
import requests as r
import json

class DeliveriesReader:
    def get_page(self, headers, api_endpoint: str, offset: int, limit: int) -> List[Dict]:
        """
        Читает одну страницу курьеров.
        Возвращает список словарей: {'_id': '...', 'name': '...'}
        """
        params = {
            "sort_field": "order_id",
            "sort_direction": "asc",
            "limit": limit,
            "offset": offset,
        }
        resp = r.get(f"{api_endpoint}/deliveries", params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
    
