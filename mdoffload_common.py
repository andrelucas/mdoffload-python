#!/usr/bin/env python3

"""
Common utilities for the MDOffload server.
"""

import logging

from google.protobuf import message
from google.protobuf.json_format import MessageToJson

def msg_to_log(s: message.Message) -> str:
    '''
    Format a protobuf message to a compact JSON representation suitable for
    logging.

    Complex messages will be hard to read in logs, but newlines in log messages
    just make a mess.
    '''
    j = MessageToJson(s, indent=None)
    return j


if __name__ == "__main__":
    # Simple test
    from mdoffload.v1 import mdoffload_pb2

    test_msg = mdoffload_pb2.GetBucketAttributesRequest(
        user_id="user123",
        bucket_id="bucket456",
        bucket_name="mybucket",
    )
    logging.basicConfig(level=logging.DEBUG)
    logging.debug(f"Original message:\n{test_msg}")
    logging.debug(f"Compact log message: {msg_to_log(test_msg.__str__())}")
