from datetime import datetime
from typing import Dict, List, Tuple
import requests as r
import json

class CouriersReader:
    def get_page(self, headers, api_endpoint: str, offset: int, limit: int) -> List[Dict]:
        """
        Читает одну страницу курьеров.
        Возвращает список словарей: {'_id': '...', 'name': '...'}
        """
        params = {
            "sort_field": "id",
            "sort_direction": "asc",
            "limit": limit,
            "offset": offset,
        }
        resp = r.get(f"{api_endpoint}/couriers", params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
    
