from logging import FileHandler, Formatter, StreamHandler

from pydantic import BaseModel


class Handler(BaseModel):
    handler: StreamHandler | FileHandler
    formatter: Formatter = None

    model_config = {"arbitrary_types_allowed": True}
