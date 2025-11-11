#!/usr/bin/env python3
"""
Minimal HTTP server implementing the MDOffload protocol.

Usage::
    ./mdoffload_server.py [-v] [<port>]
"""


import argparse
import base64
import coloredlogs
import grpc
import json
import logging
import os
import re
import shutil
import sys

from collections.abc import Generator
from concurrent import futures

from google.protobuf import any_pb2
from google.protobuf.json_format import MessageToJson
from google.rpc import code_pb2
from google.rpc import error_details_pb2
from google.rpc import status_pb2
from grpc_status import rpc_status

from mdoffload_common import msg_to_log

from mdoffload.v1 import mdoffload_pb2_grpc
from mdoffload.v1 import mdoffload_pb2

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    # ConsoleSpanExporter,
)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.instrumentation.grpc import GrpcInstrumentorServer

from sqlitedict import SqliteDict

resource = Resource(
    attributes={
        SERVICE_NAME: "mdoffload_server",
    }
)

provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(
    OTLPSpanExporter(endpoint="localhost:4317", insecure=True)
)
# processor = BatchSpanProcessor(ConsoleSpanExporter())
provider.add_span_processor(processor)

# Sets the global default tracer provider
trace.set_tracer_provider(provider)

# Creates a tracer from the global tracer provider
tracer = trace.get_tracer(__name__)

grpc_server_instrumentor = GrpcInstrumentorServer()
grpc_server_instrumentor.instrument()


class FilePath:
    def __init__(self, args: argparse.Namespace) -> None:
        self.data_dir = args.data_dir
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

    def objectattr_path(self, bucket_name: str) -> str:
        return os.path.join(self.data_dir, f"{bucket_name}_objects.sqlite")

    def bucketattr_path(self) -> str:
        return os.path.join(self.data_dir, "_buckets.sqlite")


class Attributes:
    def __init__(self, dbfile: str, tablename: str) -> None:
        self.attrs: SqliteDict = SqliteDict(
            dbfile, tablename=tablename, autocommit=True)

    def set(self, key: str, value: str) -> None:
        self.attrs[key] = value

    def get(self, key: str, create_if_missing: bool = False) -> str:
        if key not in self.attrs:
            if not create_if_missing:
                raise KeyError(f"Attribute {key} not found")
            self.attrs[key] = ""
        return self.attrs[key]

    def update(self, attributes_to_add: dict[str, str], attributes_to_delete: list[str]) -> None:
        for k, v in attributes_to_add.items():
            logging.debug(f"Setting attribute {k}={v}")
            self.attrs[k] = v
        for k in attributes_to_delete:
            if '=' in k:
                logging.warning(
                    f"Attribute delete key '{k}' contains '=', may be in error")
            if k in self.attrs:
                logging.debug(f"Deleting attribute {k}")
                del (self.attrs[k])
            else:
                logging.warning(f"Attribute {k} not found, cannot delete")

    def list(self) -> Generator[tuple[str, str], None, None]:
        return ((k, v) for k, v in self.attrs.items())


class ObjectKey:
    def __init__(self, key_name: str, instance_id: str = "") -> None:
        if key_name == "" and instance_id == "":
            raise ValueError(
                "Object key name and instance id cannot both be empty")
        self.key = key_name
        self.instance_id = instance_id

    def __hash__(self) -> int:
        return hash((self.key, self.instance_id))

    def has_instance_id(self) -> bool:
        return self.instance_id != ""

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ObjectKey):
            return False
        return self.key == other.key and self.instance_id == other.instance_id

    def __str__(self) -> str:
        if self.instance_id == "":
            i = "<NULL>"
        else:
            i = self.instance_id
        return f"ObjectKey[{self.key}/{i}]"


class Object:
    def __init__(self, key: ObjectKey, bucket: 'Bucket', args: argparse.Namespace) -> None:
        self.key = key
        self.bucket = bucket
        fp = FilePath(args)
        self.dbfile = fp.objectattr_path(bucket.name)
        self.tablename = "default"
        self.attributes = Attributes(self.dbfile, self.tablename)
        self.args = args

    def attrs(self) -> Attributes:
        return self.attributes

    def locate(self) -> str:
        return f"Object[Bucket='{self.bucket.name}',Key='{self.key}']"


class Bucket:
    def __init__(self, name: str, args: argparse.Namespace) -> None:
        if not name:
            raise ValueError("Bucket name cannot be empty")
        if not re.match(r"^[a-z0-9.-]{3,63}$", name):
            raise ValueError(f"Invalid bucket name: {name}")
        self.name = name
        self.objects: dict[ObjectKey, Object] = {}
        fp = FilePath(args)
        self.dbfile = fp.bucketattr_path()
        self.tablename = f"bucket_{name}"
        self.attributes = Attributes(self.dbfile, self.tablename)
        self.args = args

    def attrs(self) -> Attributes:
        return self.attributes

    def get_object(self, key: ObjectKey, create_if_missing: bool = False) -> Object:
        if key not in self.objects:
            if not create_if_missing:
                raise KeyError(
                    f"Object key {key} not found in bucket {self.name}")
            logging.debug(
                f"Bucket {self.name}: Creating object {key} in bucket {self.name}")
            self.objects[key] = Object(key, self, self.args)
        return self.objects[key]

    def delete_object(self, key: ObjectKey) -> None:
        if key not in self.objects:
            raise KeyError(f"bucket {self.name}: Object key {key} not found")
        del self.objects[key]

    def list(self) -> Generator[tuple[ObjectKey, Object], None, None]:
        return ((k, v) for k, v in self.objects.items())


class Store:
    def __init__(self, args: argparse.Namespace) -> None:
        self.filepaths = FilePath(args)
        self.buckets: dict[str, Bucket] = {}
        self.buckets_id_by_name: dict[str, str] = {}
        self.args = args

    def get_bucket_by_id(self, bucket_id: str, create_if_missing: bool = False) -> Bucket:
        if bucket_id not in self.buckets:
            if not create_if_missing:
                raise KeyError(f"Bucket ID {bucket_id} not found")
            self.buckets[bucket_id] = Bucket()
        return self.buckets[bucket_id]

    def get_bucket_by_name(self, bucket_name: str, create_if_missing: bool = False) -> Bucket:
        if bucket_name not in self.buckets_id_by_name:
            if not create_if_missing:
                raise KeyError(f"Bucket name {bucket_name} not found")
            bucket_id = self.create_bucket(bucket_name)
        else:
            bucket_id = self.buckets_id_by_name[bucket_name]
        return self.get_bucket_by_id(bucket_id, create_if_missing=create_if_missing)

    def create_bucket(self, bucket_name: str) -> str:
        if bucket_name in self.buckets_id_by_name:
            return self.buckets_id_by_name[bucket_name]
        bucket_id = f"bucket-{len(self.buckets)+1}"
        self.buckets_id_by_name[bucket_name] = bucket_id
        self.buckets[bucket_id] = Bucket(bucket_name, self.args)
        return bucket_id


class MDOffloadServer(mdoffload_pb2_grpc.MDOffloadServiceServicer):
    """MDOffload gRPC server implementation."""

    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__()
        self.store = Store(args)
        self.args = args

    def get_bucket(self, bucket_name: str, bucket_id: str, create_if_missing: bool) -> Bucket | None:
        if bucket_id:
            try:
                return self.store.get_bucket_by_id(bucket_id)
            except KeyError:
                return None
        elif bucket_name:
            try:
                return self.store.get_bucket_by_name(bucket_name, create_if_missing=create_if_missing)
            except KeyError:
                return None
        return None

    def get_object(self, bucket: Bucket, object_key: str, instance_id: str, create_if_missing: bool = False) -> Object | None:
        # Allow ValueError to propagate up - this is an protocol error we should return.
        key = ObjectKey(object_key, instance_id)
        try:
            return bucket.get_object(key, create_if_missing=create_if_missing)
        except KeyError:
            return None

    def GetBucketAttributes(
        self,
        request: mdoffload_pb2.GetBucketAttributesRequest,
        context: grpc.ServicerContext,
    ) -> mdoffload_pb2.GetBucketAttributesResponse:
        with tracer.start_as_current_span("GetBucketAttributes"):
            logging.debug(
                f"GetBucketAttributes request: [{msg_to_log(request)}]")

            bucket_id = self.get_bucket(
                request.bucket_name, request.bucket_id, True)
            if bucket_id is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Bucket not found")
                logging.error(
                    f"Bucket not found: {request.bucket_name} / {request.bucket_id}: "
                    f"{context.code()} {context.details().decode('UTF-8')}")
                return mdoffload_pb2.GetBucketAttributesResponse()

            response = mdoffload_pb2.GetBucketAttributesResponse(
                attributes=bucket_id.attrs().list()
            )

            logging.debug(
                f"GetBucketAttributes response: {msg_to_log(response)}")
            return response

    def SetBucketAttributes(
        self,
        request: mdoffload_pb2.SetBucketAttributesRequest,
        context: grpc.ServicerContext,
    ) -> mdoffload_pb2.SetBucketAttributesResponse:
        with tracer.start_as_current_span("SetBucketAttributes"):
            logging.debug(
                f"SetBucketAttributes request: [{msg_to_log(request)}]")

            bucket_id = self.get_bucket(
                request.bucket_name, request.bucket_id, True)
            if bucket_id is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Bucket not found")
                logging.error(f"Bucket not found: {request.bucket_name} / {request.bucket_id}: "
                              f"{context.code()} {context.details().decode('UTF-8')}")
                return mdoffload_pb2.SetBucketAttributesResponse()

            bucket_id.attrs().update(
                request.attributes_to_add, request.attributes_to_delete
            )
            logging.debug(
                f"Updated bucket attributes: {','.join([f'{k}={v}' for k,v in bucket_id.attrs().list()])}")

            response = mdoffload_pb2.SetBucketAttributesResponse()
            logging.debug(
                f"SetBucketAttributes response: {msg_to_log(response)}")
            return response

    def GetObjectAttributes(
        self,
        request: mdoffload_pb2.GetObjectAttributesRequest,
        context: grpc.ServicerContext,
    ) -> mdoffload_pb2.GetObjectAttributesResponse:
        with tracer.start_as_current_span("GetObjectAttributes"):
            logging.debug(
                f"GetObjectAttributes request: [{msg_to_log(request)}]")

            bucket = self.get_bucket(
                request.bucket_name, request.bucket_id, args.create_bucket_if_missing)
            if bucket is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Bucket not found")
                logging.error(
                    f"Bucket not found: {request.bucket_name} / {request.bucket_id}: "
                    f"{context.code()} {context.details().decode('UTF-8')}")
                return mdoffload_pb2.GetObjectAttributesResponse()

            try:
                obj = self.get_object(
                    bucket, request.object_key, request.object_instance_id, args.create_object_if_missing)
                if obj is None:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details("Object not found")
                    return mdoffload_pb2.GetObjectAttributesResponse()
            except ValueError as ve:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(str(ve))
                logging.error(f"Invalid argument: {request.bucket_name} / {request.bucket_id} / "
                              f"{request.object_key} / {request.object_instance_id}: "
                              f"{context.code()} {context.details().decode('UTF-8')}")
                return mdoffload_pb2.GetObjectAttributesResponse()

            logging.debug(f"Fetching attributes on object: {obj.locate()}")

            response = mdoffload_pb2.GetObjectAttributesResponse(
                attributes=obj.attrs().list()
            )
            logging.debug(
                f"GetObjectAttributes response: {msg_to_log(response)}")
            return response

    def SetObjectAttributes(
        self,
        request: mdoffload_pb2.SetObjectAttributesRequest,
        context: grpc.ServicerContext,
    ) -> mdoffload_pb2.SetObjectAttributesResponse:
        with tracer.start_as_current_span("SetObjectAttributes"):
            logging.debug(
                f"SetObjectAttributes request: [{msg_to_log(request)}]")

            bucket = self.get_bucket(
                request.bucket_name, request.bucket_id, args.create_bucket_if_missing)
            if bucket is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Bucket not found")
                logging.error(
                    f"Bucket not found: {request.bucket_name} / {request.bucket_id}: {context.code()} {context.details()}")
                return mdoffload_pb2.SetObjectAttributesResponse()
            obj = self.get_object(
                bucket, request.object_key, request.object_instance_id, args.create_object_if_missing)
            if obj is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Object not found")
                return mdoffload_pb2.SetObjectAttributesResponse()
            logging.debug(f"Setting attributes on object: {obj.locate()}")
            obj.attrs().update(request.attributes_to_add, request.attributes_to_delete)
            logging.debug(
                f"Updated object attributes: {','.join([f'{k}={v}' for k,v in obj.attrs().list()])}")

            response = mdoffload_pb2.SetObjectAttributesResponse()

            logging.debug(
                f"SetObjectAttributes response: {msg_to_log(response)}")
            return response


def _load_credential_from_file(filepath: str) -> bytes:
    """https://github.com/grpc/grpc/blob/master/examples/python/auth/_credentials.py"""
    real_path = os.path.join(os.path.dirname(__file__), filepath)
    with open(real_path, "rb") as f:
        return f.read()


def run(args: argparse.Namespace) -> None:
    server_address = f"127.0.0.1:{args.port}"
    logging.info("Starting gRPC service...\n")
    try:
        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            options=(
                ("grpc.so_reuseport", 0),
            ),  # This apparently helps detect port reuse - see https://github.com/grpc/grpc/issues/16920
        )
        mdoffload_pb2_grpc.add_MDOffloadServiceServicer_to_server(
            MDOffloadServer(args), server
        )

        if args.tls:
            server_crt = _load_credential_from_file(args.server_cert)
            server_key = _load_credential_from_file(args.server_key)
            server_credentials = grpc.ssl_server_credentials(
                (
                    (
                        server_key,
                        server_crt,
                    ),
                )
            )
            server.add_secure_port(server_address, server_credentials)

        else:
            server.add_insecure_port(server_address)

        server.start()
        logging.info(f"Server started, listening on {server_address}")
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    logging.info("Stopping gRPC server...\n")


if __name__ == "__main__":
    from sys import argv

    p = argparse.ArgumentParser(description="Authorizer gRPC server")
    p.add_argument("-B", "--create-bucket-if-missing",
                   help="always create the bucket if it does not exist",
                   action="store_true")
    p.add_argument("-O", "--create-object-if-missing",
                   help="always create the object if it does not exist",
                   action="store_true")
    p.add_argument("port", type=int, help="Listen port",
                   nargs="?", default=8004)
    p.add_argument(
        "-t", "--tls", help="connect to the server using TLS", action="store_true"
    )
    p.add_argument("-v", "--verbose", action="store_true",
                   help="Enable verbose output")

    psav = p.add_argument_group("Persistence configuration")
    psav.add_argument(
        "--data-dir",
        default="./data",
        help="Directory to store attribute database files")
    psav.add_argument(
        "--reset",
        help="Reset all stored data then exit. Doing this on a running server is a bad idea.",
        action="store_true")

    ptls = p.add_argument_group("TLS arguments")
    ptls.add_argument("--ca-cert", help="CA certificate file (NOT YET USED)")
    ptls.add_argument("--server-cert", help="client certificate file")
    ptls.add_argument("--server-key", help="client key file")

    args = p.parse_args()
    if args.verbose:
        coloredlogs.install(level=logging.DEBUG, isatty=True)
    else:
        coloredlogs.install(level=logging.INFO, isatty=True)

    if args.tls:
        if not args.server_cert:
            logging.error("TLS requires a server certificate")
            sys.exit(1)
        if not args.server_key:
            logging.error("TLS requires a server key")
            sys.exit(1)

    if args.reset:
        logging.warning(f"Resetting all stored data in {args.data_dir}")
        data_dir = args.data_dir
        if os.path.exists(data_dir):
            os.removedirs(data_dir)
        shutil.rmtree(data_dir)

    if args.create_bucket_if_missing:
        logging.warning("Will create buckets if missing")
    if args.create_object_if_missing:
        logging.warning("Will create objects if missing")

    run(args)
