import os
from pydantic import BaseModel


class ImmPortConfig(BaseModel):
    username: str | None
    password: str | None

    @classmethod
    def from_env(cls):
        return cls(
            username=os.environ["IMMPORT_USERNAME"],
            password=os.environ["IMMPORT_PASSWORD"]
        )
