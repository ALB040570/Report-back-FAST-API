import os
from typing import Any, Dict, List

import json
import requests


from app.models.filters import Filters
from app.models.remote_source import RemoteSource

SERVICE360_BASE_URL = os.getenv("SERVICE360_BASE_URL", "http://45.8.116.32")


def _build_mock_records(remote_source: RemoteSource, filters: Filters) -> List[Dict[str, Any]]:
    """
    Генератор тестовых данных, если remoteSource.url начинается с mock://
    Это позволяет тестировать pivot без реального бэкенда.
    """
    # Можно варьировать структуру в зависимости от id/url, если захочется
    records: List[Dict[str, Any]] = [
        {"cls": "A", "year": 2024, "value": 10, "count": 1},
        {"cls": "A", "year": 2024, "value": 20, "count": 2},
        {"cls": "B", "year": 2024, "value": 5,  "count": 3},
        {"cls": "B", "year": 2023, "value": 15, "count": 4},
    ]
    return records


def load_records(remote_source: RemoteSource) -> List[Dict[str, Any]]:
    """
    Загружает сырые записи из удалённого источника,
    используя поля remoteSource (url, method, body, headers).

    Ожидаемый формат ответа такой же, как в Service360:

        {
          "result": {
            "records": [ ... ]
          }
        }

    или, как fallback:

        [ ... ]  # если API сразу возвращает список записей
    """

    # 1. Базовые поля источника
    method = (remote_source.method or "POST").upper()
    url = (remote_source.url or "").strip()
    base_url = SERVICE360_BASE_URL.rstrip("/")

    if not url:
        return []
    if url.startswith("mock://"):
        return _build_mock_records(remote_source, Filters())
    if url.startswith("http://") or url.startswith("https://"):
        full_url = url
    elif url.startswith("/"):
        full_url = f"{base_url}{url}"
    else:
        full_url = f"{base_url}/{url.lstrip('/')}"
    headers = remote_source.headers or {}

    # 2. Формируем тело запроса
    body: Any = remote_source.body or {}

    # Если body пустой, но есть rawBody-строка — пробуем распарсить как JSON
    if (not body) and remote_source.rawBody:
        try:
            body = json.loads(remote_source.rawBody)
        except Exception:
            # Если не получилось распарсить, отправим как есть
            body = remote_source.rawBody
    if isinstance(body, dict):
        body = {**body}
        body.pop("__joins", None)

    # 3. HTTP-запрос к удалённому источнику
    try:
        if method == "GET":
            response = requests.get(
                full_url,
                headers=headers,
                params=body if isinstance(body, dict) else None,
                timeout=30,
            )
        else:
            # Для наших сервисов обычный вариант — POST с JSON-телом
            json_body = body if isinstance(body, (dict, list)) else None
            response = requests.post(
                full_url,
                headers=headers,
                json=json_body,
                timeout=30,
            )
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to load remote source: {exc}") from exc

    response.raise_for_status()
    data = response.json()

    # 4. Достаём records из стандартной структуры Service360
    if isinstance(data, dict):
        result = data.get("result") or data.get("data") or data
        if isinstance(result, dict):
            records = result.get("records")
            if isinstance(records, list):
                print(f"[load_records] URL={full_url}, records={len(records)}")
                return records

    # 5. Fallback: если API вернул просто список
    if isinstance(data, list):
        print(f"[load_records] URL={full_url}, records={len(data)}")
        return data

    # Если формат неожиданный — пока возвращаем пустой список
    print(f"[load_records] URL={full_url}, records=0")
    return []
