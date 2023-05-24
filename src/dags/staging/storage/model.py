from pydantic import BaseModel

class Workflow(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: dict

    class Config:
        # Проверка всех типов
        smart_union = True