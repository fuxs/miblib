# -*- coding: utf-8 -*-
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
from google import auth
from google.protobuf import descriptor_pb2, message_factory
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types, writer
from typing import Any, Callable, TypeAlias
import datetime as dt
import time

ValueType: TypeAlias = descriptor_pb2.FieldDescriptorProto.Type.ValueType


def create_mapping(
    custom: dict[str, ValueType] = {},
) -> dict[str, ValueType]:
    if custom is None:
        custom = {}
    return {
        "STRING": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        "BYTES": descriptor_pb2.FieldDescriptorProto.TYPE_BYTES,
        "INTEGER": descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
        "INT64": descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
        "FLOAT": descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE,
        "FLOAT64": descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE,
        "BOOLEAN": descriptor_pb2.FieldDescriptorProto.TYPE_BOOL,
        "BOOL": descriptor_pb2.FieldDescriptorProto.TYPE_BOOL,
        "RECORD": descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
        "TIME": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        "DATETIME": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        "DATE": descriptor_pb2.FieldDescriptorProto.TYPE_INT32,
        "GEOGRAPHY": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        "JSON": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        "TIMESTAMP": descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
        "NUMERIC": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        "BIGNUMERIC": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    } | custom


def build_desriptor_proto(fields, name="Row", mapping=create_mapping()):
    desc = descriptor_pb2.DescriptorProto()
    desc.name = name
    for number, field in enumerate(fields, 1):
        if field.mode == "NULLABLE":
            label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        elif field.mode == "REPEATED":
            label = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
        else:
            label = descriptor_pb2.FieldDescriptorProto.LABEL_REQUIRED
        ft = mapping.get(
            field.field_type, descriptor_pb2.FieldDescriptorProto.TYPE_STRING
        )
        if field.field_type == "RECORD" or field.field_type == "STRUCT":
            type_name = f"MIB{name}{field.name.title()}"
            desc.nested_type.append(
                build_desriptor_proto(field.fields, type_name, mapping)
            )
            desc.field.add(
                name=field.name,
                number=number,
                type=ft,
                type_name=type_name,
                label=label,
            )
        else:
            desc.field.add(name=field.name, number=number, type=ft, label=label)
    return desc


def build_clazz(descriptor_proto: descriptor_pb2.DescriptorProto):
    file_descriptor = descriptor_pb2.FileDescriptorProto()
    file_descriptor.name = "row.proto"
    file_descriptor.package = "mib"
    file_descriptor.message_type.add().CopyFrom(descriptor_proto)
    return message_factory.GetMessages([file_descriptor])[
        f"mib.{descriptor_proto.name.title()}"
    ]


def create_converter(custom: dict[str, Callable[[Any], Any]] = {}):
    return {
        "STRING": lambda v: str(v),
        "BOOL": lambda v: bool(v),
        "BOOLEAN": lambda v: bool(v),
        "INTEGER": lambda v: int(v),
        "INT64": lambda v: int(v),
        "FLOAT": lambda v: float(v),
        "FLOAT64": lambda v: float(v),
        "TIME": lambda v: v.strftime("%T.%f"),  # e.g. '17:51:22.817863'
        "DATETIME": lambda v: v.isoformat(
            sep=" ", timespec="milliseconds"
        ),  # e.g. '2024-09-29 17:59:13.834'
        "DATE": lambda v: (v - dt.datetime(1970, 1, 1)).days,
        "TIMESTAMP": lambda v: int((v.timestamp() * 1_000_000)),
        "JSON": lambda v: str(v),
        "GEOGRAPHY": lambda v: str(v),
        "NUMERIC": lambda v: str(round(v, 9)),
        "BIGNUMERIC": lambda v: str(round(v, 38)),
    } | custom


def build_struct_filler(fields, converter=create_converter()):
    actions = []
    for field in fields:
        if field.field_type == "RECORD" or field.field_type == "STRUCT":
            a = build_struct_filler(list(field.fields), converter)
            if field.mode == "NULLABLE":
                actions.append(
                    lambda dst, src, name=field.name, a=a: (
                        lambda v: (v is not None) and a(getattr(dst, name), v)
                    )(src.get(name))
                )
            elif field.mode == "REPEATED":
                actions.append(
                    lambda dst, src, name=field.name, a=a: (
                        lambda v, d: (v is not None) and [a(d.add(), i) for i in v]
                    )(src.get(name), getattr(dst, name))
                )
            else:  # REQUIRED
                actions.append(
                    lambda dst, src, name=field.name, a=a: a(
                        getattr(dst, name), src.get(name)
                    )
                )
        else:
            # get conversion function, default does nothing
            c = converter.get(field.field_type, lambda v: v)
            if field.mode == "NULLABLE":
                actions.append(
                    lambda dst, src, name=field.name, c=c: (
                        lambda v: (v is not None) and setattr(dst, name, c(v))
                    )(src.get(name))
                )
            elif field.mode == "REPEATED":
                actions.append(
                    lambda dst, src, name=field.name, c=c: (
                        lambda v: (v is not None)
                        and getattr(dst, name).extend([c(r) for r in v])
                    )(src.get(name))
                )
            else:  # REQUIRED
                actions.append(
                    lambda dst, src, name=field.name, c=c: setattr(
                        dst, name, c(src.get(name))
                    )
                )
    return lambda dst, src: [a(dst, src) for a in actions]


class BQBatchWriterStream:
    def __init__(self, stream, clazz, actions, offset) -> None:
        self.stream = stream
        self.rows = types.ProtoRows()
        self.clazz = clazz
        self.actions = actions
        self.offset = offset
        self.size = 0
        self.total_size = 0
        self.futures = []

    def append(self, *args):
        total = 0
        for src in args:
            dst = self.clazz()
            self.actions(dst, src)
            serialized = dst.SerializeToString()
            self.rows.serialized_rows.append(serialized)
            size = len(serialized)
            total += size
            self.size += size
            if self.size > 9_000_000:  # if larger than 9MB then submit
                self.submit()
        return total

    def submit(self):
        number_of_rows = len(self.rows.serialized_rows)
        if number_of_rows > 0:
            request = types.AppendRowsRequest()
            request.offset = self.offset
            proto_data = types.AppendRowsRequest.ProtoData()
            proto_data.rows = self.rows
            request.proto_rows = proto_data
            self.futures.append(self.stream.send(request))
            print(f"Sent {number_of_rows} with {self.size} bytes")
            self.offset += number_of_rows
            self.rows = types.ProtoRows()
            self.total_size += self.size
            self.size = 0

    def close(self):
        self.submit()
        for f in self.futures:
            f.result()
        self.stream.close()
        return self.total_size


class BQBatchWriter:

    def __init__(
        self,
        table_id: str,
        type_mapping: dict[str, ValueType] = None,
        converter: dict[str, Callable[[Any], Any]] = None,
        write_stream: str = None,
        bqc: bigquery.Client = None,
        wc: bigquery_storage_v1.BigQueryWriteClient = None,
    ) -> None:
        if bqc is None or wc is None:
            credentials, project_id = auth.default()
            if bqc is None:
                bqc = bigquery.Client(credentials=credentials, project=project_id)
            if wc is None:
                wc = bigquery_storage_v1.BigQueryWriteClient(credentials=credentials)
        self.wc = wc
        s = table_id.split(".")
        if len(s) != 3:
            raise ValueError(
                f'table_id must be a fully-qualified ID in standard SQL format, e.g., "project.dataset.table_id", got {table_id}'
            )
        self.table_id = table_id
        self.parent = self.wc.table_path(*s)
        if type_mapping is None:
            self.type_mapping = create_mapping()
        else:
            self.type_mapping = create_mapping(type_mapping)
        if converter is None:
            self.converter = create_converter()
        else:
            self.converter = create_converter(converter)
        if write_stream is None:
            ws = types.WriteStream()
            ws.type_ = types.WriteStream.Type.PENDING
            self.write_stream = self.wc.create_write_stream(
                parent=self.parent, write_stream=ws
            ).name
        else:
            self.write_stream = write_stream
        self.streams = []
        self.update_schema(bqc.get_table(table_id).schema)

    def __enter__(self):
        return self.open()

    def __exit__(self, type, value, traceback):
        self.commit()

    def update_schema(self, schema):
        self.descriptor = build_desriptor_proto(
            fields=schema, mapping=self.type_mapping
        )
        self.clazz = build_clazz(self.descriptor)
        self.actions = build_struct_filler(fields=schema, converter=self.converter)

    def open(self, offset: int = 0) -> BQBatchWriterStream:
        schema = types.ProtoSchema()
        schema.proto_descriptor = self.descriptor
        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.writer_schema = schema
        template = types.AppendRowsRequest()
        template.write_stream = self.write_stream
        template.proto_rows = proto_data
        stream = BQBatchWriterStream(
            writer.AppendRowsStream(self.wc, template), self.clazz, self.actions, offset
        )
        self.streams.append(stream)
        return stream

    def commit(self):
        if len(self.streams) > 0:
            total_size = 0
            for s in self.streams:
                total_size += s.close()
            self.streams = []
            # A PENDING type stream must be "finalized" before being committed.
            self.wc.finalize_write_stream(name=self.write_stream)
            cr = types.BatchCommitWriteStreamsRequest()
            cr.parent = self.parent
            cr.write_streams = [self.write_stream]
            self.wc.batch_commit_write_streams(cr)


def write_single_batch(table_id: str, data: list):
    start = time.time()
    total = 0
    with BQBatchWriter(table_id=table_id) as s:
        total = s.append(*data)
    end = time.time()
    print(f"Submitted {total} bytes in {end - start}s")
