import logging
import re

from keboola.component.exceptions import UserException
from pydantic import BaseModel, ValidationError, field_validator, model_validator


class Configuration(BaseModel):
    client_id: str = ""
    entity_period: str = ""
    reporting_period_id: int = 0
    entity_id: int = 0
    endpoints: list[str] = ["templates_structure", "lookup_tables"]
    template_id: str = ""
    debug: bool = False

    @field_validator("client_id", "template_id")
    def split_id_string(cls, v):
        if v != "" and isinstance(v, str):
            return int(v.split("-", 1)[0])
        return v

    @model_validator(mode="after")
    def extract_entity_period_ids(self):
        if not self.entity_period:
            return self
        try:
            match = re.match(r"(\d+)-.*?\s{3}(\d+)-", self.entity_period)
            if not match:
                raise ValueError(
                    "Invalid format for 'entity_period'. Expected '123-Period name   456-Entity name'."
                )
            self.reporting_period_id = int(match.group(1))
            self.entity_id = int(match.group(2))
        except Exception as e:
            raise UserException(f"Error parsing entity_period: {e}")
        return self

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")
