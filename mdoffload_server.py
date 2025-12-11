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


class BucketNotEmptyError(RuntimeError):
    pass

class FilePath:
    def __init__(self, args: argparse.Namespace) -> None:
        self.data_dir = args.data_dir
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

    def objectattr_path(self, bucket: 'Bucket') -> str:
        '''
        Object attributes are stored in a per-bucket database file. This is
        named for the bucket ID with '-' replaced by '_', with '_objects_' as a
        prefix.
        '''
        safe_id = bucket.id.replace('-', '_')
        return os.path.join(self.data_dir, f"_objects_{safe_id}.sqlite")

    def bucketattr_path(self) -> str:
        '''
        Fixed path for the bucket attributes database. Each bucket gets its own
        table containing its attributes.
        '''
        return os.path.join(self.data_dir, "_buckets.sqlite")


class Attributes:
    def __init__(self, dbfile: str, tablename: str) -> None:
        self.attrs: SqliteDict = SqliteDict(
            dbfile, tablename=tablename, autocommit=True)

    def close(self) -> None:
        logging.debug(f"Closing attribute database file: {self.attrs.filename}")
        self.attrs.close()

    def purge(self) -> None:
        self.attrs.close()
        logging.debug(f"Purging attribute database file: {self.attrs.filename}")
        os.remove(self.attrs.filename)

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

    def keys(self) -> Generator[str, None, None]:
        return self.attrs.keys()


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
        return f"{self.key}/{i}"

    def tablename(self) -> str:
        '''
        Return a safe table name for this object key. Abandon all pretence of
        speed and use base64 encoding to ensure safety.
        '''
        def escape(s: str) -> str:
            return s.replace(r"_", "__")

        k = base64.b64encode(escape(self.key).encode()).decode()
        if self.instance_id == "":
            i = "NULL"
        else:
            i = base64.b64encode(self.instance_id.encode()).decode()

        return f"object_{k}_{i}"


class Object:
    def __init__(self, key: ObjectKey, bucket: 'Bucket', args: argparse.Namespace) -> None:
        self.key = key
        self.bucket = bucket
        fp = FilePath(args)
        self.dbfile = fp.objectattr_path(bucket)
        self.tablename = self.key.tablename()
        self.attributes = Attributes(self.dbfile, self.tablename)
        self.args = args

    def attrs(self) -> Attributes:
        return self.attributes

    def locate(self) -> str:
        return f"Bucket['{self.bucket.name}'/'{self.bucket.id}'] Key['{self.key}']"

    def purge(self) -> None:
        logging.debug(f"Purging object {self.locate()}")
        self.attributes.purge()


class Bucket:
    def __init__(self, name: str, id: str, args: argparse.Namespace) -> None:
        if not id:
            raise ValueError("Bucket ID cannot be empty")
        if not name:
            raise ValueError("Bucket name cannot be empty")
        if not re.match(r"^[a-z0-9.-]{3,63}$", name):
            raise ValueError(f"Invalid bucket name: {name}")
        self.id = id
        self.name = name
        self.objects: dict[ObjectKey, Object] = {}
        fp = FilePath(args)
        self.dbfile = fp.bucketattr_path()
        self.tablename = f"bucket_{id.replace('-', '_')}"
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
                f"Bucket {self.name}: Creating object {key}")
            self.objects[key] = Object(key, self, self.args)
        return self.objects[key]

    def purge_object(self, key: ObjectKey) -> None:
        if key not in self.objects:
            raise KeyError(f"bucket {self.name}: Object key {key} not found")
        logging.debug(
            f"Bucket {self.name}: Purging object {key}")
        obj = self.objects[key]
        del self.objects[key]
        obj.purge()

    def list(self) -> Generator[tuple[ObjectKey, Object], None, None]:
        return ((k, v) for k, v in self.objects.items())

    def purge(self) -> None:
        objcount = len(self.objects)
        if objcount != 0:
            logging.error(f"Cannot purge non-empty bucket {self.name} containing {objcount} objects")
            raise BucketNotEmptyError(
                f"Cannot purge bucket {self.name} with live objects")
        objcopy = dict(self.objects)  # Inefficient but safe
        logging.debug(f"Purging bucket {self.name}, purging {len(objcopy)} objects")
        for k, v in objcopy.items():
            del self.objects[k]
            v.purge()
        # A Bucket's attributes are in a table, not a separate dbfile. Need to
        # just delete all keys.
        attr = [k for k, _ in self.attributes.list()]
        logging.debug(f"Purging bucket {self.name} id {self.id} attribute count {len(attr)}")
        for k in attr:
            del self.attributes.attrs[k]


class Store:
    def __init__(self, args: argparse.Namespace) -> None:
        self.filepaths = FilePath(args)
        self.buckets: dict[str, Bucket] = {}
        self.args = args

    def get_bucket_by_id(self, bucket_name: str, bucket_id: str) -> Bucket:
        if bucket_id not in self.buckets:
            if not self.args.create_bucket_if_missing:
                raise KeyError(f"Bucket ID {bucket_id} not found")
            self.buckets[bucket_id] = Bucket(bucket_name, bucket_id, self.args)
        return self.buckets[bucket_id]


class MDOffloadServer(mdoffload_pb2_grpc.MDOffloadServiceServicer):
    """MDOffload gRPC server implementation."""

    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__()
        self.store = Store(args)
        self.args = args

    def get_bucket(self, bucket_name: str, bucket_id: str, create_if_missing: bool) -> Bucket | None:
        if bucket_id:
            try:
                return self.store.get_bucket_by_id(bucket_name, bucket_id)
            except KeyError:
                return None
        else:
            logging.error("Bucket ID must be provided to get_bucket")
            return None

    def purge_bucket(self, bucket_name: str, bucket_id: str) -> None:
        if bucket_id in self.store.buckets:
            logging.debug(f"Purging bucket ID {bucket_id} from store")
            bucket = self.store.buckets[bucket_id]
            bucket.purge()
            del self.store.buckets[bucket_id]
        else:
            logging.debug(f"Bucket ID {bucket_id} not in store, cannot purge")
            raise KeyError(f"Bucket ID {bucket_id} not found")

    def get_object(self, bucket: Bucket, object_key: str, instance_id: str, create_if_missing: bool = False) -> Object | None:
        # Allow ValueError to propagate up - this is an protocol error we should return.
        key = ObjectKey(object_key, instance_id)
        try:
            return bucket.get_object(key, create_if_missing=create_if_missing)
        except KeyError:
            return None

    def purge_object(self, bucket: Bucket, object_key: str, instance_id: str) -> None:
        key = ObjectKey(object_key, instance_id)
        logging.debug(f"Deleting object {key} from bucket {bucket.name}")
        bucket.purge_object(key)

    def GetBucketAttributes(
        self,
        request: mdoffload_pb2.GetBucketAttributesRequest,
        context: grpc.ServicerContext,
    ) -> mdoffload_pb2.GetBucketAttributesResponse:
        with tracer.start_as_current_span("GetBucketAttributes"):
            logging.debug(
                f"GetBucketAttributes request: [{msg_to_log(request)}]")
            if not request.bucket_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Bucket ID must be provided")
                logging.error(f"GetBucketAttributes failed: "
                              f"{context.code()} {context.details().decode('UTF-8')}")
                return mdoffload_pb2.SetBucketAttributesResponse()

            bucket = self.get_bucket(
                request.bucket_name, request.bucket_id, True)
            if bucket is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Bucket not found")
                logging.error(
                    f"GetBucketAttributes failed: {request.bucket_name}/{request.bucket_id}: "
                    f"{context.code()} {context.details().decode('UTF-8')}")
                return mdoffload_pb2.GetBucketAttributesResponse()

            response = mdoffload_pb2.GetBucketAttributesResponse(
                attributes=bucket.attrs().list()
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
            if not request.bucket_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Bucket ID must be provided")
                logging.error(f"SetBucketAttributes failed: "
                              f"{context.code()} {context.details().decode('UTF-8')}")
                return mdoffload_pb2.SetBucketAttributesResponse()

            bucket = self.get_bucket(
                request.bucket_name, request.bucket_id, True)
            if bucket is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Bucket not found")
                logging.error(f"SetBucketAttributes failed: Bucket not found: "
                              f"{request.bucket_name}/{request.bucket_id}: "
                              f"{context.code()} {context.details().decode('UTF-8')}")
                return mdoffload_pb2.SetBucketAttributesResponse()

            bucket.attrs().update(
                request.attributes_to_add, request.attributes_to_delete
            )
            logging.debug(
                f"Updated bucket attributes: {','.join([f'{k}={v}' for k,v in bucket.attrs().list()])}")

            response = mdoffload_pb2.SetBucketAttributesResponse()
            logging.debug(
                f"SetBucketAttributes response: {msg_to_log(response)}")
            return response

    def PurgeBucketAttributes(self,
                              request: mdoffload_pb2.PurgeBucketAttributesRequest,
                              context: grpc.ServicerContext,
                              ):
        with tracer.start_as_current_span("PurgeBucketAttributes"):
            logging.debug(
                f"PurgeBucketAttributes request: [{msg_to_log(request)}]")
            if not request.bucket_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Bucket ID must be provided")
                logging.error(f"PurgeBucketAttributes failed: "
                              f"{context.code()} {context.details().decode('UTF-8')}")
                return mdoffload_pb2.PurgeBucketAttributesResponse()

            try:
                self.purge_bucket(
                    request.bucket_name, request.bucket_id)
            except KeyError:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Bucket not found")
                return mdoffload_pb2.PurgeBucketAttributesResponse()
            except BucketNotEmptyError:
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details(
                    "Bucket not empty, cannot purge attributes")
                return mdoffload_pb2.PurgeBucketAttributesResponse()

            response = mdoffload_pb2.PurgeBucketAttributesResponse()
            logging.debug(
                f"PurgeBucketAttributes response: {msg_to_log(response)}")
            return response

    def GetObjectAttributes(
        self,
        request: mdoffload_pb2.GetObjectAttributesRequest,
        context: grpc.ServicerContext,
    ) -> mdoffload_pb2.GetObjectAttributesResponse:
        with tracer.start_as_current_span("GetObjectAttributes"):
            logging.debug(
                f"GetObjectAttributes request: [{msg_to_log(request)}]")
            if not request.bucket_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Bucket ID must be provided")
                logging.error(f"GetObjectAttributes failed: "
                              f"{context.code()} {context.details().decode('UTF-8')}")
                return mdoffload_pb2.SetBucketAttributesResponse()

            bucket = self.get_bucket(
                request.bucket_name, request.bucket_id, args.create_bucket_if_missing)
            if bucket is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Bucket not found")
                logging.error(
                    f"GetObjectAttributes failed: Bucket not found: {request.bucket_name}/{request.bucket_id}: "
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
                logging.error(f"GetObjectAttributes failed: Invalid argument: "
                              f"{request.bucket_name}/{request.bucket_id}/"
                              f"{request.object_key}/{request.object_instance_id}: "
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
            if not request.bucket_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Bucket ID must be provided")
                logging.error(f"SetObjectAttributes failed: "
                              f"{request.bucket_name} / {request.bucket_id}: "
                              f"{context.code()} {context.details().decode('UTF-8')}")
                return mdoffload_pb2.SetBucketAttributesResponse()

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

            if request.new_object_instance:
                logging.debug(
                    f"Creating new object instance, clearing existing attributes on object: {obj.locate()}")
                obj.attrs().update(
                    {}, list(obj.attrs().attrs.keys())
                )  # Clear all existing attributes
            logging.debug(f"Setting attributes on object: {obj.locate()}")
            obj.attrs().update(request.attributes_to_add, request.attributes_to_delete)
            logging.debug(
                f"Updated object attributes: {','.join([f'{k}={v}' for k,v in obj.attrs().list()])}")

            response = mdoffload_pb2.SetObjectAttributesResponse()

            logging.debug(
                f"SetObjectAttributes response: {msg_to_log(response)}")
            return response

    def PurgeObjectAttributes(self,
                              request: mdoffload_pb2.PurgeObjectAttributesRequest,
                              context: grpc.ServicerContext):
        with tracer.start_as_current_span("PurgeObjectAttributes"):
            logging.debug(
                f"PurgeObjectAttributes request: [{msg_to_log(request)}]")
            if not request.bucket_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Bucket ID must be provided")
                logging.error(f"PurgeObjectAttributes failed: "
                              f"{request.bucket_name} / {request.bucket_id}: "
                              f"{context.code()} {context.details().decode('UTF-8')}")
                return mdoffload_pb2.PurgeObjectAttributesResponse()

            bucket = self.get_bucket(
                request.bucket_name, request.bucket_id, False)

            if bucket is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Bucket not found")
                logging.error(
                    f"Bucket not found: {request.bucket_name} / {request.bucket_id}: {context.code()} {context.details()}")
                return mdoffload_pb2.PurgeObjectAttributesResponse()

            try:
                self.purge_object(
                    bucket, request.object_key, request.object_instance_id)
            except KeyError:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Object not found")
                return mdoffload_pb2.PurgeObjectAttributesResponse()

            response = mdoffload_pb2.PurgeObjectAttributesResponse()
            logging.debug(
                f"PurgeObjectAttributes response: {msg_to_log(response)}")
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
            shutil.rmtree(data_dir)
        sys.exit(0)

    if args.create_bucket_if_missing:
        logging.warning("Will create buckets if missing")
    if args.create_object_if_missing:
        logging.warning("Will create objects if missing")

    run(args)
