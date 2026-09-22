import logging

from keboola.component.exceptions import UserException
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)


class Configuration(BaseModel):
    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")
