"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from target_mssql.target import TargetMSSQL

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    # TODO: Initialize minimal tap config
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        TargetMSSQL,
        config=SAMPLE_CONFIG
    )
    for test i`n tests:
        test()


# TODO: Create additional tests as appropriate for your tap.
