from typing import Union

from pandas import DataFrame

from .handler import DBHandler
from .model import ModelAPI


class PanChemy(object):
    def __init__(self, base_model, engine, dialects="postgresql"):
        self.dialects = dialects
        self._engine = engine
        self._db: DBHandler = DBHandler(engine)
        self.base = base_model
        self.models = self.__get_registry_models(base_model)

    def __get_registry_models(self, base_model):
        return [
            c
            for c in base_model.registry._class_registry.values()
            if getattr(c, "__tablename__", None)
        ]

    def init(self):
        for m in self.models:
            setattr(self, m.__name__, ModelAPI(self._engine, m))  # type : ModelAPI

    def to_df(self, stmt, index: Union[list, str] = None):
        return self._db.stmt_to_df(stmt, index)

    def save_records(self, table: str, record: dict):
        return self._db.save_records(table, record)

    def delete_records(self, table: str, df: DataFrame):
        return self._db.delete_records(table, df)
