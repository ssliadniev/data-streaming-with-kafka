from pydantic import BaseModel


class SensorReading(BaseModel):
    datetime: str
    room: str
    consumption_level: float

    def to_json(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_json(cls, data: bytes) -> "SensorReading":
        return cls.model_validate_json(data.decode("utf-8"))


class AnomalyAlert(BaseModel):
    room: str
    datetime: str
    level: float
    threshold_exceeded: float

    def to_json(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_json(cls, data: bytes) -> "AnomalyAlert":
        return cls.model_validate_json(data.decode("utf-8"))
