from abc import ABCMeta, abstractmethod
from .job import DEFAULT_RETRY_COUNT
from .job import DEFAULT_RETRY_PAUSE_SEC

DEFAULT_RUNTIME = '1970-01-01'


class BigQueryOperation(object, metaclass=ABCMeta):

    @abstractmethod
    def to_job(self, id: str, retry_count: int = DEFAULT_RETRY_COUNT, retry_pause_sec: int = DEFAULT_RETRY_PAUSE_SEC):
        pass

    def run(self, runtime=DEFAULT_RUNTIME):
        pass


class Dataset(object, metaclass=ABCMeta):

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
    def collect_list(self, sql: str) -> BigQueryOperation:
        pass

    @abstractmethod
    def create_table(self, create_query: str) -> BigQueryOperation:
        pass
