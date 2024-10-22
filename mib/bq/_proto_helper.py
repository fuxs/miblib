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
from google.protobuf.message import Message
from google.protobuf import descriptor_pb2, message_factory
from google.cloud import bigquery
from typing import Any, TypeAlias
from collections.abc import Callable
import datetime as dt

ValueType: TypeAlias = descriptor_pb2.FieldDescriptorProto.Type.ValueType
SchemaField: TypeAlias = bigquery.schema.SchemaField


def create_mapping(
    custom: dict[str, ValueType] | None = None,
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


def build_desriptor_proto(
    fields: list[SchemaField],
    name: str = "Row",
    mapping: dict[str, ValueType] | None = None,
) -> descriptor_pb2.DescriptorProto:
    if mapping is None:
        mapping = create_mapping()
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


def build_clazz(descriptor_proto: descriptor_pb2.DescriptorProto) -> type[Message]:
    file_descriptor = descriptor_pb2.FileDescriptorProto()
    file_descriptor.name = "row.proto"
    file_descriptor.package = "mib"
    file_descriptor.message_type.add().CopyFrom(descriptor_proto)
    return message_factory.GetMessages([file_descriptor])[
        f"mib.{descriptor_proto.name.title()}"
    ]


def create_converter(
    custom: dict[str, Callable[[Any], Any]] | None = None
) -> dict[str, Callable[[Any], Any]]:
    if custom is None:
        custom = {}
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
        "TIMESTAMP": lambda v: int(v.timestamp() * 1_000_000),
        "JSON": lambda v: str(v),
        "GEOGRAPHY": lambda v: str(v),
        "NUMERIC": lambda v: str(round(v, 9)),
        "BIGNUMERIC": lambda v: str(round(v, 38)),
    } | custom


def build_struct_filler(
    fields: list[SchemaField],
    converter: dict[str, Callable[[Any], Any]] | None = None,
) -> Callable[[Any, Any],None]:
    if converter is None:
        converter = create_converter()
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
