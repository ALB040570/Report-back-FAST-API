from pydantic import BaseModel


class ReportJobQueuedResponse(BaseModel):
    job_id: str
    status: str
