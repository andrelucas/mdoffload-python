#!/usr/bin/env python3

"""
Common utilities for the MDOffload server.
"""

import logging


def msg_to_log(s: str) -> str:
    '''
    Replace protobuf's str() formatting with something more compact, suitable for
    logging.

    This is going to need changes as the messages in play get more complex.
    '''
    items = s.splitlines()
    return ', '.join([i.replace(': ', ':') for i in items])


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
