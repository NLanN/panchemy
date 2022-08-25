from operator import or_
from typing import Union

from pandas import DataFrame
from sqlalchemy import select

from .handler import DBHandler

class ModelAPI:
    def __init__(self, engine, model, chunk_size, dialect='postgres'):
        self._db: DBHandler = DBHandler(engine)
        self._model = model
        self._table = model.__table__
        self._columns = {c.name: c for c in model.__table__.columns}
        self._primary_keys = list(model.__table__.primary_key)
        self._unique_cols = [c for c in model.__table__.columns if c.unique]
        self._foreign_keys = list(model.__table__.foreign_keys)
        self._chunk_size = chunk_size
        self.base_stmt = select(model)

    def __convert_to_set(self, att) -> set:
        if isinstance(att, list):
            return set(att)
        if isinstance(att, str):
            return {
                att,
            }
        return set()

    def _normalize_fields(self, exclude, fields, dropna):
        exclude = self.__convert_to_set(exclude)
        fields = self.__convert_to_set(fields)
        dropna = self.__convert_to_set(dropna)

        diff = (dropna - self._columns.keys()) | (fields - self._columns.keys())
        if diff:
            raise Exception(f"Fields {diff} not existed in table {str(self._table)}")
        fields = list(fields - exclude)

        return fields, dropna

    def get(
        self,
        fields: list = None,
        index: Union[list, str] = None,
        exclude: Union[list, str] = None,
        dropna: Union[list, str] = None,
    ) -> DataFrame:
        fields, dropna = self._normalize_fields(
            fields=fields, exclude=exclude, dropna=dropna
        )

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
            .to_dict(orient="records")
        )
        if key_only:
            rtn_fields = self._primary_keys
        else:
            rtn_fields = list(self._columns.values())

        df = self._db.save_records(self._table, records, rtn_fields, self._chunk_size)
        return df



    def _mysql_upsert(self):
        ...


    def upsert(self, df: DataFrame, fields: list = None, key_only=True):
        model_fields = set(self._columns.keys())
        data_cols = set(df.columns.values.tolist())
        fields = model_fields & data_cols
        records = (
            df.reset_index()
                .filter(fields)
                .astype(object)
                .where(lambda x: x.notnull(), None)
                .to_dict(orient="records")
        )
        if key_only:
            rtn_fields = self._primary_keys
        else:
            rtn_fields = list(self._columns.values())

        unique_cols = [c.name for c in self._table.columns if c.unique]
        unique_cols.extend([k.name for k in self._table.primary_key])
        unique_cols = set(unique_cols) & set(records[0].keys())

        df = self._db.pg_upsert_records(self._table, records, unique_cols, rtn_fields, self._chunk_size)
        return df

    def delete(self, df: DataFrame = None):
        df = df.reset_index()
        df = self._db.delete_records(
            self._table, df=df, pk=self._primary_keys, rtn_fields=self._primary_keys
        )
        return df
