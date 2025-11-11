#!/usr/bin/env python3
"""
Simple test client for the MDOffload gRPC service.
"""

import argparse
import base64
import coloredlogs
from google.rpc import code_pb2
from google.rpc import error_details_pb2
from google.rpc import status_pb2
from google.protobuf.json_format import MessageToJson
from google.protobuf.timestamp_pb2 import Timestamp
import grpc
from grpc_status import rpc_status
import logging
import os
import sys

from mdoffload_common import msg_to_log

from mdoffload.v1 import mdoffload_pb2_grpc
from mdoffload.v1 import mdoffload_pb2


def gather_attributes(args: argparse.Namespace) -> tuple[dict[str, str], list[str]]:
    attributes_to_add: dict[str, str] = {}
    attributes_to_delete: list[str] = []
    if args.add_attribute:
        for attr in args.add_attribute:
            k, v = attr.split("=", 1)
            attributes_to_add[k] = v
    if args.delete_attribute:
        for attr in args.delete_attribute:
            attributes_to_delete.append(attr)
    return attributes_to_add, attributes_to_delete


def get_bucket_attributes(
    stub: mdoffload_pb2_grpc.MDOffloadServiceStub,
    args: argparse.Namespace,
) -> bool:
    request = mdoffload_pb2.GetBucketAttributesRequest(
        user_id=args.user_id,
        bucket_id=args.bucket_id if args.bucket_id else "",
        bucket_name=args.bucket_name if args.bucket_name else "",
    )
    logging.debug(f"get_bucket_attributes request: [{msg_to_log(str(request))}]")
    response = stub.GetBucketAttributes(request)
    logging.debug(f"get_bucket_attributes response: [{msg_to_log(str(response))}]")
    print(MessageToJson(response))
    return True


def set_bucket_attributes(
    stub: mdoffload_pb2_grpc.MDOffloadServiceStub,
    args: argparse.Namespace,
) -> bool:
    attributes_to_add, attributes_to_delete = gather_attributes(args)

    request = mdoffload_pb2.SetBucketAttributesRequest(
        user_id=args.user_id,
        bucket_id=args.bucket_id if args.bucket_id else "",
        bucket_name=args.bucket_name if args.bucket_name else "",
        attributes_to_add=attributes_to_add,
        attributes_to_delete=attributes_to_delete
    )
    logging.debug(f"set_bucket_attributes request: [{msg_to_log(str(request))}]")
    response = stub.SetBucketAttributes(request)
    logging.debug(f"set_bucket_attributes response: [{msg_to_log(str(response))}]")
    print(MessageToJson(response))
    return True


def get_object_attributes(
    stub: mdoffload_pb2_grpc.MDOffloadServiceStub,
    args: argparse.Namespace,
) -> bool:
    request = mdoffload_pb2.GetObjectAttributesRequest(
        user_id=args.user_id,
        bucket_id=args.bucket_id if args.bucket_id else "",
        bucket_name=args.bucket_name if args.bucket_name else "",
        object_key=args.object_key if args.object_key else "",
        object_instance_id=args.instance_id if args.instance_id else "",
    )
    logging.debug(f"get_object_attributes request: [{msg_to_log(str(request))}]")
    response = stub.GetObjectAttributes(request)
    logging.debug(f"get_object_attributes response: [{msg_to_log(str(response))}]")
    print(MessageToJson(response))
    return True


def set_object_attributes(
    stub: mdoffload_pb2_grpc.MDOffloadServiceStub,
    args: argparse.Namespace,
) -> bool:
    attributes_to_add, attributes_to_delete = gather_attributes(args)

    request = mdoffload_pb2.SetObjectAttributesRequest(
        user_id=args.user_id,
        bucket_id=args.bucket_id if args.bucket_id else "",
        bucket_name=args.bucket_name if args.bucket_name else "",
        object_key=args.object_key,
        object_instance_id=args.instance_id,
        attributes_to_add=attributes_to_add,
        attributes_to_delete=attributes_to_delete,
    )
    logging.debug(f"set_object_attributes request: [{msg_to_log(str(request))}]")
    response = stub.SetObjectAttributes(request)
    logging.debug(f"set_object_attributes response: [{msg_to_log(str(response))}]")
    print(MessageToJson(response))
    return True


def issue(channel: grpc.Channel, args: argparse.Namespace) -> bool:
    """
    Issue the RPC. Factored out so we can use different types of channel.
    """
    stub = mdoffload_pb2_grpc.MDOffloadServiceStub(channel)

    try:
        if args.command == "get-bucket-attributes":
            return get_bucket_attributes(stub, args)
        elif args.command == "set-bucket-attributes":
            return set_bucket_attributes(stub, args)
        elif args.command == "get-object-attributes":
            return get_object_attributes(stub, args)
        elif args.command == "set-object-attributes":
            return set_object_attributes(stub, args)
        else:
            logging.error(f"Unknown command '{args.command}'")
            sys.exit(2)
    except grpc.RpcError as e:
        logging.error(f"gRPC error: {e.code()} - {e.details()}")
        if e.code() == grpc.StatusCode.UNKNOWN:
            status = rpc_status.from_call(e)
            if status is not None:
                for detail in status.details:
                    if detail.Is(error_details_pb2.DebugInfo.DESCRIPTOR):
                        debug_info = error_details_pb2.DebugInfo()
                        detail.Unpack(debug_info)
                        logging.error(f"Debug info: {debug_info.stack_entries}")
        return False


def _load_credential_from_file(filepath: str) -> bytes:
    """https://github.com/grpc/grpc/blob/master/examples/python/auth/_credentials.py"""
    real_path = os.path.join(os.path.dirname(__file__), filepath)
    with open(real_path, "rb") as f:
        return f.read()


def main(argv: list[str]) -> None:
    p = argparse.ArgumentParser(description="AuthService client")
    p.add_argument("command", help="command to run",
                   choices=["get-bucket-attributes", "set-bucket-attributes",
                            "get-object-attributes", "set-object-attributes",])
    p.add_argument("-b", "--bucket-name", help="bucket name", default="")
    p.add_argument("--bucket-id", help="bucket id", default="")
    p.add_argument("-k", "--object-key",
                   help="object key to authorize", default="")
    p.add_argument("-i", "--instance-id", help="instance id", default="")
    p.add_argument("-u", "--user-id", help="user id", default="testid")
    p.add_argument("-A", "--add-attribute",
                   help="set a bucket attribute (k=v)", action="append")
    p.add_argument("-D", "--delete-attribute",
                   help="set an object attribute (k=v)", action="append")
    p.add_argument("-t", "--tls", help="connect to the server using TLS", action="store_true"
                   )
    p.add_argument(
        "--uri", help="server uri (will override address and port!)")
    p.add_argument("-a", "--address", help="server address",
                   default="127.0.0.1")
    p.add_argument("-p", "--port", type=int, default=8004,
                   help="server listen port")
    p.add_argument("-v", "--verbose", action="store_true")
    ptls = p.add_argument_group("TLS arguments")
    ptls.add_argument("--ca-cert", help="CA certificate file")
    ptls.add_argument(
        "--client-cert", help="client certificate file (NOT YET USED)")
    ptls.add_argument("--client-key", help="client key file (NOT YET USED)")

    args = p.parse_args(argv)
    if not args.command:
        p.usage()
        sys.exit(1)

    if args.verbose:
        coloredlogs.install(level=logging.DEBUG)
    else:
        coloredlogs.install(level=logging.INFO)

    if args.tls:
        if not args.ca_cert:
            logging.error("TLS requires a CA certificate")
            sys.exit(1)

    # Set up a channel string first.
    server_address = f"dns:{args.address}:{args.port}"
    if args.uri:
        server_address = args.uri
    logging.debug(f"using server_address {server_address}")
    success = False

    if args.tls:
        root_crt = _load_credential_from_file(args.ca_cert)
        channel_credential = grpc.ssl_channel_credentials(root_crt)
        with grpc.secure_channel(server_address, channel_credential) as channel:
            success = issue(channel, args)
    else:
        with grpc.insecure_channel(server_address) as channel:
            success = issue(channel, args)

    if success:
        logging.info("Success")
        sys.exit(0)
    else:
        logging.error("RPC failed")
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
