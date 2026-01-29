from typing import Optional

from pydantic import BaseModel


class ComputedField(BaseModel):
    id: Optional[str] = None
    fieldKey: Optional[str] = None
    expression: Optional[str] = None
    resultType: Optional[str] = None
