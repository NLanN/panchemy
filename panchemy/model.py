from operator import or_
from typing import Union

from pandas import DataFrame
from sqlalchemy import select

from panchemy.handler import DBHandler


class ModelAPI:
    def __init__(self, engine, model):
        self._db: DBHandler = DBHandler(engine)
        self._model = model
        self._table = model.__table__
        self._columns = {c.name: c for c in model.__table__.columns}
        self._primary_keys = list(model.__table__.primary_key)
        self._foreign_keys = list(model.__table__.foreign_keys)
        self.base_stmt = select(model)

    def __convert_to_set(self, att) -> set:
        if isinstance(att, list):
            return set(att)
        if isinstance(att, str):
            return {att, }
        return set()

    def _normalize_fields(self, exclude, fields, dropna):
        exclude = self.__convert_to_set(exclude)
        fields = self.__convert_to_set(fields)
        dropna = self.__convert_to_set(dropna)

        diff = (dropna - self._columns.keys()) | (fields - self._columns.keys())
        if diff:
            raise Exception(
                f"Fields {diff} not existed in table {str(self._table)}")
        fields = list(fields - exclude)

        return fields, dropna

    def get(self, fields: list = None, index: Union[list, str] = None,
            exclude: Union[list, str] = None,
            dropna: Union[list, str] = None) -> DataFrame:
        fields, dropna = self._normalize_fields(fields=fields, exclude=exclude,
                                                dropna=dropna)

        stmt = self.base_stmt

        if fields:
            model_fields = [getattr(self._model, f) for f in fields]
            stmt = select(*model_fields)

        if dropna:
            if dropna.__len__() > 1:
                cond = or_(*[f for f in dropna])
            else:
                cond = self._columns[dropna.pop()].isnot(None)

            stmt = stmt.where(cond)

        df = self._db.stmt_to_df(stmt, index)
        return df

    def insert(self, df: DataFrame, key_only: bool = True):
        fields = list(self._columns.keys())
        records = (
            df.reset_index()
                .filter(fields)
                .astype(object)
                .where(lambda x: x.notnull(), None)
                .to_dict(orient='records')
        )
        if key_only:
            rtn_fields = self._primary_keys
        else:
            rtn_fields = list(self._columns.values())

        df = self._db.save_records(self._table, records, rtn_fields)
        return df

    def upsert(self, df: DataFrame, fields: list = None):
        # TODO: just for Postgresql
        ...

    def delete(self, df: DataFrame = None):
        df = df.reset_index()
        df = self._db.delete_records(self._table, df=df,
                                     pk=self._primary_keys,
                                     rtn_fields=self._primary_keys)
        return df
