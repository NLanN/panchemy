import itertools
from typing import Union, Iterable

import pandas as pd
from pandas import DataFrame
from sqlalchemy import delete, insert, and_
from sqlalchemy.orm import sessionmaker


class DBHandler:
    def __init__(self, engine):
        self._Session = sessionmaker(autocommit=False, autoflush=False,
                                     bind=engine)

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
        while True:
            chunk = tuple(itertools.islice(iter(iterable), size))
            if not chunk:
                return
            yield chunk

    def save_records(self, table, records, rtn_fields: list = None,
                     chunk_size=10000) -> DataFrame:
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
        print(f'[{table.name}] Inserted {len(df)} rows')
        return df

    def _prepare_in_operator(self, pk, df):
        pk_params = []
        for k in pk:
            # pk_params.update({k : df[[str(k)]]})
            print(df.get(str(k.name)).values)
            print(type(df.get(str(k.name)).values))
            pk_params.append(k.in_(df.get(str(k.name)).values.tolist()))
        return and_(*pk_params)

    def delete_records(self, table, pk, df: DataFrame,
                       rtn_fields: list = None, chunk_size=10000):
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
