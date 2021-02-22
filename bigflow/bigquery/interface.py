import typing
from pathlib import Path
from abc import ABC, abstractmethod
from .job import DEFAULT_RETRY_COUNT
from .job import DEFAULT_RETRY_PAUSE_SEC

from bigflow.commons import public

DEFAULT_RUNTIME = '1970-01-01'


@public()
class BigQueryOperation(ABC):

    @abstractmethod
    def to_job(self, id: str, retry_count: int = DEFAULT_RETRY_COUNT, retry_pause_sec: int = DEFAULT_RETRY_PAUSE_SEC):
        pass

    @abstractmethod
    def run(self, runtime=DEFAULT_RUNTIME):
        pass


@public()
class Dataset(ABC):

    @abstractmethod
    def write_truncate(self, table_name: str, sql: str, partitioned: bool = True) -> BigQueryOperation:
        pass

    @abstractmethod
    def write_append(self, table_name: str, sql: str, partitioned: bool = True) -> BigQueryOperation:
        pass

    @abstractmethod
    def write_tmp(self, table_name: str, sql: str) -> BigQueryOperation:
        pass

    @abstractmethod
    def collect(self, sql: str) -> BigQueryOperation:
        pass

    @abstractmethod
    def collect_list(self, sql: str, record_as_dict: bool = False) -> BigQueryOperation:
        pass

    @abstractmethod
    def create_table(self, create_query: str) -> BigQueryOperation:
        pass

    @abstractmethod
    def create_table_from_schema(
            self,
            table_name: str,
            schema: typing.Union[typing.List[dict], Path, None] = None,
            table=None):
        pass

    @abstractmethod
    def insert(
            self,
            table_name: str,
            records: typing.Union[typing.List[dict], Path]):
        pass

    @abstractmethod
    def delete_dataset(self):
        pass