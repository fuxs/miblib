# Copyright 2024 Michael Bungenstock
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import time
from typing import Any
from collections.abc import Callable

from google import auth
from google.cloud import bigquery, bigquery_storage_v1
from google.cloud.bigquery.table import TableReference
from google.cloud.bigquery_storage_v1 import types, writer

from ._proto_helper import (
    SchemaField,
    ValueType,
    build_clazz,
    build_desriptor_proto,
    build_struct_filler,
    create_converter,
    create_mapping,
)


class BQWriterBase:

    def __init__(
        self,
        table_id: str,
        type_mapping: dict[str, ValueType] | None = None,
        converter: dict[str, Callable[[Any], Any]] | None = None,
        batch: bool = False,
        exactly_once: bool = False,
        bqc: bigquery.Client | None = None,
        wc: bigquery_storage_v1.BigQueryWriteClient | None = None,
    ):
        project_id = None
        if bqc is None or wc is None:
            credentials, project_id = auth.default()
            if bqc is None:
                bqc = bigquery.Client(credentials=credentials, project=project_id)
            if wc is None:
                wc = bigquery_storage_v1.BigQueryWriteClient(credentials=credentials)
        self._table_ref = TableReference.from_string(
            table_id=table_id, default_project=project_id
        )
        self._parent = self._table_ref.to_bqstorage()
        self._wc = wc
        self._table = bqc.get_table(table_id)
        self._type_mapping = create_mapping(type_mapping)
        self._converter = create_converter(converter)
        if batch or exactly_once:
            if batch:
                _type = types.WriteStream.Type.PENDING
            else:
                _type = types.WriteStream.Type.COMMITTED
            self._write_stream = self._wc.create_write_stream(
                parent=self._parent,
                write_stream=types.WriteStream(type_=_type),
            )
            self._stream_name = self._write_stream.name
        else:
            self._stream_name = f"{self._table_ref.path}/streams/_default"[1:]
        self._exactly_once = exactly_once
        self._batch = batch
        self._rows = types.ProtoRows()
        self._size = 0
        self._total_size = 0
        self._futures = []

    def update_schema(self, schema: list[SchemaField]):
        self._descriptor = build_desriptor_proto(
            fields=schema, mapping=self._type_mapping
        )
        self._clazz = build_clazz(self._descriptor)
        self._actions = build_struct_filler(fields=schema, converter=self._converter)

    def open(self, offset: int = 0):
        self.update_schema(self._table.schema)
        self._offset = offset
        schema = types.ProtoSchema(proto_descriptor=self._descriptor)
        proto_data = types.AppendRowsRequest.ProtoData(writer_schema=schema)
        template = types.AppendRowsRequest(
            write_stream=self._stream_name, proto_rows=proto_data
        )
        self._rpc_stream = writer.AppendRowsStream(self._wc, template)
        return self

    def append(self, *args):
        total = 0
        for src in args:
            dst = self._clazz()
            self._actions(dst, src)
            serialized = dst.SerializeToString()
            self._rows.serialized_rows.append(serialized)
            size = len(serialized)
            total += size
            self._size += size
            if self._size > 9_000_000:  # if larger than 9MB then submit
                self.submit()
        return total

    def submit(self):
        number_of_rows = len(self._rows.serialized_rows)
        if number_of_rows > 0:
            request = types.AppendRowsRequest()
            if self._exactly_once:
                request.offset = self._offset
                self._offset += number_of_rows
            proto_data = types.AppendRowsRequest.ProtoData(rows=self._rows)
            request.proto_rows = proto_data
            self._futures = [f for f in self._futures if f.running()]
            self._futures.append(self._rpc_stream.send(request))
            print(f"Sent {number_of_rows} rows with {self._size} bytes")
            self._rows = types.ProtoRows()
            self._total_size += self._size
            self._size = 0

    def close(self):
        if self._rpc_stream is not None:
            self.submit()
            for f in self._futures:
                f.result()
            self._rpc_stream.close()
            self._rpc_stream = None
            if self._batch:
                # A PENDING type stream must be "finalized" before being committed.
                self._wc.finalize_write_stream(name=self._write_stream.name)
                cr = types.BatchCommitWriteStreamsRequest(
                    parent=self._parent, write_streams=[self._write_stream.name]
                )
                self._wc.batch_commit_write_streams(cr)
            elif self._write_stream:
                # optional for custom streams, but we want to be neat
                request = types.FinalizeWriteStreamRequest(name=self._stream_name)
                self._wc.finalize_write_stream(request=request)

    def __enter__(self):
        return self.open()

    def __exit__(self, type, value, traceback):
        self.close()


class BQBatchWriter(BQWriterBase):

    def __init__(
        self,
        table_id: str,
        type_mapping: dict[str, ValueType] | None = None,
        converter: dict[str, Callable[[Any], Any]] | None = None,
        bqc: bigquery.Client | None = None,
        wc: bigquery_storage_v1.BigQueryWriteClient | None = None,
    ) -> None:
        super().__init__(
            table_id=table_id,
            type_mapping=type_mapping,
            converter=converter,
            batch=True,
            exactly_once=True,
            bqc=bqc,
            wc=wc,
        )


class BQStreamWriter(BQWriterBase):

    def __init__(
        self,
        table_id: str,
        type_mapping: dict[str, ValueType] | None = None,
        converter: dict[str, Callable[[Any], Any]] | None = None,
        exactly_once: bool = True,
        bqc: bigquery.Client | None = None,
        wc: bigquery_storage_v1.BigQueryWriteClient | None = None,
    ) -> None:
        super().__init__(
            table_id=table_id,
            type_mapping=type_mapping,
            converter=converter,
            batch=False,
            exactly_once=exactly_once,
            bqc=bqc,
            wc=wc,
        )

    def send(self, *args):
        total = self.append(*args)
        self.submit()
        return total

    # def _submit(self):
    #    number_of_rows = len(self._rows.serialized_rows)
    #    if number_of_rows > 0:
    #        request = types.AppendRowsRequest()
    #        if self._exactly_once:
    #            request.offset = self._offset
    #            self._offset += number_of_rows
    #        proto_data = types.AppendRowsRequest.ProtoData(rows=self._rows)
    #        request.proto_rows = proto_data
    #        self._futures = [f for f in self._futures if f.running()]
    #        self._futures.append(self._rpc_stream.send(request))
    #        print(f"Sent {number_of_rows} with {self._size} bytes")
    #        self._rows = types.ProtoRows()
    #        self._total_size += self._size
    #        self._size = 0

    # def close(self):
    #    if self._rpc_stream is not None:
    #        for f in self._futures:
    #            f.result()
    #        self._rpc_stream.close()
    #        self._rpc_stream = None
    #        if self._write_stream:
    #            request = types.FinalizeWriteStreamRequest(name=self._stream_name)
    #            self._wc.finalize_write_stream(request=request)


def write_single_batch(table_id: str, data: list):
    start = time.time()
    total = 0
    with BQBatchWriter(table_id=table_id) as s:
        total = s.append(*data)
    end = time.time()
    print(f"Submitted {total} bytes in {end - start}s")


def stream_single_batch(table_id: str, data: list):
    start = time.time()
    total = 0
    with BQStreamWriter(table_id=table_id) as s:
        total = s.send(*data)
    end = time.time()
    print(f"Submitted {total} bytes in {end - start}s")
