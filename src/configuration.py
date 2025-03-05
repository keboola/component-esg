import logging
from pydantic import BaseModel, ValidationError
from keboola.component.exceptions import UserException


class Configuration(BaseModel):
    client_id: int = None
    entity_id: int = None
    client_reporting_period_id: int = None
    endpoint: str
    template_id: int = None
    debug: bool = False

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")
