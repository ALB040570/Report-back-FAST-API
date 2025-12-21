from typing import Any, Dict

from pydantic import BaseModel

from app.models.filters import Filters
from app.models.remote_source import RemoteSource
from app.models.snapshot import Snapshot


class ViewRequest(BaseModel):
    """
    Запрос на построение представления:
    - templateId — идентификатор шаблона (как в Service360)
    - remoteSource — нормализованный источник данных (из API отчётов)
    - snapshot — конфигурация pivot/фильтров/метрик/сортировок
    - filters — глобальные и контейнерные фильтры
    """
    templateId: str
    remoteSource: RemoteSource
    snapshot: Snapshot
    filters: Filters
