from dataclasses import dataclass, field
from typing import List


@dataclass
class Storage:
    id: str
    options: dict

    def get_absolute_path(self):
        pass

    @classmethod
    def from_dict(cls, data):
        storage_type = data.get('json_type', 'Storage')
        if storage_type == 'CsvStorage':
            return CsvStorage.from_dict(data)
        elif storage_type == 'ParquetStorage':
            return ParquetStorage.from_dict(data)
        elif storage_type == 'JdbcStorage':
            return JdbcStorage.from_dict(data)
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")


@dataclass
class CsvStorage(Storage):
    path: str
    storage_account: str
    container: str
    sas_token: str
    json_type: str = field(default="CsvStorage")

    def get_absolute_path(self):
        return self.path if self.storage_account is None else (
            f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/{self.path}")

    @classmethod
    def from_dict(cls, data):
        return cls(
            id=data.get('id', None),
            options=data.get('options', None),
            path=data.get('path', None),
            storage_account=data.get('storage_account', None),
            container=data.get('container', None),
            sas_token=data.get('sas_token', None)
        )


@dataclass
class ParquetStorage(Storage):
    path: str
    storage_account: str
    container: str
    sas_token: str
    json_type: str = field(default="ParquetStorage")

    def get_absolute_path(self):
        return self.path if self.storage_account is None else (
            f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/{self.path}")

    @classmethod
    def from_dict(cls, data):
        return cls(
            id=data.get('id', None),
            options=data.get('options', None),
            path=data.get('path', None),
            storage_account=data.get('storage_account', None),
            container=data.get('container', None),
            sas_token=data.get('sas_token', None)
        )


@dataclass
class JdbcStorage(Storage):
    table: str
    uri: str
    json_type: str = field(default="JdbcStorage")

    def get_absolute_path(self):
        pass

    @classmethod
    def from_dict(cls, data):
        return cls(
            id=data.get('id', None),
            options=data.get('options', None),
            table=data.get('table', None),
            uri=data.get('uri', None)
        )


@dataclass
class Transformation:
    id: str
    sql: str


@dataclass
class Target:
    from_sql_query: str
    output: Storage

    def sql(self):
        return f"select * from {self.from_sql_query}"

    @classmethod
    def from_dict(cls, data):
        return cls(
            from_sql_query=data.get('from_sql_query', None),
            output=Storage.from_dict(data.get('output', {}))
        )


@dataclass
class BatchPlan:
    inputs: List['Storage']
    transformations: List['Transformation']
    targets: List['Target']

    @classmethod
    def from_dict(cls, data):
        inputs = [Storage.from_dict(input_data) for input_data in data.get('inputs', [])]
        transformations = [Transformation(**transformation) for transformation in data.get('transformations', [])]
        targets = [Target.from_dict(target) for target in data.get('targets', [])]

        return cls(inputs=inputs, transformations=transformations, targets=targets)
