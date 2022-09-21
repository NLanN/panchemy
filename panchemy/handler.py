import itertools
from typing import Iterable, Union

import pandas as pd
from panchemy.log import logger
from pandas import DataFrame
from sqlalchemy import and_, delete, insert
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker


class DBHandler:
    def __init__(self, engine):
        self._Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    def _session(self):
        session = self._Session()
        try:
            yield session
            session.flush()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _slice_to_chunk(self, iterable: Iterable, size):
        it = iter(iterable)
        while True:
            chunk = tuple(itertools.islice(it, size))
            if not chunk:
                return
            yield chunk

    def save_records(
            self, table, records, rtn_fields: list = None, chunk_size=10000
    ) -> DataFrame:
        result_df = []
        session = next(self._session())
        for chunk in self._slice_to_chunk(records, chunk_size):
            stmt = insert(table, chunk).returning(*rtn_fields)
            record = session.execute(stmt)
            session.commit()
            data = record.all()
            df = pd.DataFrame.from_records(data, columns=[f.name for f in rtn_fields])
            result_df.append(df)

        df = pd.concat(result_df)
        logger.info(f"Table [{table.name}] inserted with {len(df)} rows")
        return df

    def mysql_upsert_records(self, table, records, rtn_fields: list = None,
                             chunk_size=10000) -> DataFrame:
        result_df = []
        session = next(self._session())
        for chunk in self._slice_to_chunk(records, chunk_size):
            stmt = mysql_insert(table, chunk). \
                on_duplicate_key_update(). \
                returning(*rtn_fields)
            record = session.execute(stmt)
            session.commit()
            data = record.all()
            df = pd.DataFrame.from_records(data, columns=[f.name for f in rtn_fields])
            result_df.append(df)

        df = pd.concat(result_df)
        logger.info(f"Table [{table.name}] inserted with {len(df)} rows")
        return df

    def pg_upsert_records(self, table, records, unique_cols, rtn_fields: list = None,
                          chunk_size=10000) -> DataFrame:
        results = []
        session = next(self._session())
        data_cols = [c for c in table.columns if c.name not in table.primary_key]
        for chunk in self._slice_to_chunk(records, chunk_size):
            insert_stmt = pg_insert(table, chunk)
            stmt = insert_stmt.on_conflict_do_update(
                index_elements=unique_cols,
                set_={c.name: getattr(insert_stmt.excluded, c.name) for c in
                      data_cols}).returning(*rtn_fields)

            record = session.execute(stmt)
            session.commit()
            data = record.all()
            df = pd.DataFrame.from_records(data, columns=[f.name for f in rtn_fields])
            results.append(df)

        df = pd.concat(results)
        logger.info(f"Table [{table.name}] inserted with {len(df)} rows")
        return df

    def _prepare_in_operator(self, pk, df):
        pk_params = []
        for k in pk:
            pk_params.append(k.in_(df.get(str(k.name)).values.tolist()))
        return and_(*pk_params)

    def delete_records(
            self, table, pk, df: DataFrame, rtn_fields: list = None, chunk_size=10000
    ):
        session = next(self._session())
        pk_params = self._prepare_in_operator(pk=pk, df=df)
        stmt = delete(table).where(pk_params).returning(*rtn_fields)
        record = session.execute(stmt)
        session.commit()
        data = record.all()
        df = pd.DataFrame.from_records(data, columns=[f.name for f in rtn_fields])
        return df

    def stmt_to_df(self, stmt, index: Union[list, str] = None) -> DataFrame:
        session = next(self._session())

        df = pd.read_sql(stmt, session.bind)
        if index:
            df = df.set_index(index)

        return df
