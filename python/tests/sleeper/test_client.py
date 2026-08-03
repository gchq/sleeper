#  Copyright 2022-2026 Crown Copyright
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Tests covering SleeperClient
These use moto so no real AWS infrastructure is required. Because
SleeperClient's constructor only needs `load_instance_properties` to
return *something* (it isn't consulted again until other methods are
called), that function is monkeypatched out entirely so the tests can
focus purely on region/account propagation.
"""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import MagicMock

import pytest
from moto import mock_aws

from sleeper import SleeperClient

# Dotted path used for monkeypatching load_instance_properties. Must point at
# the name as it is looked up *inside* the SleeperClient module (i.e.
# wherever `from sleeper.properties import load_instance_properties` was
# imported into), not at sleeper.properties itself.
SLEEPER_CLIENT_MODULE = "sleeper.client"

FAKE_INSTANCE_ID = "test-instance"
FAKE_ACCOUNT_ID = "123456789012"  # moto's default STS account id

REGION_DNS_SUFFIXES = [
    ("eu-west-2", "amazonaws.com"),
    ("us-east-1", "amazonaws.com"),
    ("eusc-de-east-1", "amazonaws.eu"),
]


@pytest.fixture(autouse=True)
def _stub_instance_properties(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Replace load_instance_properties with a stub returning a MagicMock, so
    the constructor doesn't need a real config file seeded in a mocked S3
    bucket. We don't inspect the returned properties in these tests.
    """
    monkeypatch.setattr(
        f"{SLEEPER_CLIENT_MODULE}.load_instance_properties",
        lambda s3_resource, account_name, instance_id: MagicMock(name="InstanceProperties"),
    )


@pytest.fixture(params=REGION_DNS_SUFFIXES, ids=[region for region, _ in REGION_DNS_SUFFIXES])
def region_and_dns_suffix(request: pytest.FixtureRequest) -> tuple[str, str]:
    """Parametrised (region, expected_dns_suffix) pair, one per partition under test."""
    return request.param


@pytest.fixture
def sleeper_client(region_and_dns_suffix: tuple[str, str]) -> Generator[SleeperClient, None, None]:
    """
    Builds a SleeperClient configured for the region under test.

    account_name is supplied explicitly so the constructor never calls
    sts:GetCallerIdentity
    """
    region_name, _ = region_and_dns_suffix
    with mock_aws():
        client = SleeperClient(
            instance_id=FAKE_INSTANCE_ID,
            region_name=region_name,
            account_name=FAKE_ACCOUNT_ID,
        )
        yield client


def should_configure_s3_client_with_requested_region_name(
    sleeper_client: SleeperClient,
    region_and_dns_suffix: tuple[str, str],
) -> None:
    region_name, _ = region_and_dns_suffix
    assert sleeper_client._s3_client.meta.region_name == region_name


def should_configure_s3_client_with_region_specific_dns_suffix(
    sleeper_client: SleeperClient,
    region_and_dns_suffix: tuple[str, str],
) -> None:
    """
    The S3 client's resolved endpoint should end with the DNS suffix
    appropriate to the partition that the given region belongs to.
    """
    _, expected_dns_suffix = region_and_dns_suffix
    endpoint_url = sleeper_client._s3_client.meta.endpoint_url
    assert endpoint_url.endswith(expected_dns_suffix), f"Expected endpoint {endpoint_url!r} to end with {expected_dns_suffix!r}"


def should_configure_s3fs_with_requested_region_name(
    sleeper_client: SleeperClient,
    region_and_dns_suffix: tuple[str, str],
) -> None:
    region_name, _ = region_and_dns_suffix
    assert sleeper_client._s3_fs.client_kwargs["region_name"] == region_name


@pytest.fixture
def sleeper_client_default_region(monkeypatch: pytest.MonkeyPatch) -> Generator[SleeperClient, None, None]:
    """
    A SleeperClient built without an explicit region_name, to exercise the
    `region_name = region_name or os.environ.get("AWS_REGION")` fallback.
    """
    monkeypatch.setenv("AWS_REGION", "eu-west-2")
    with mock_aws():
        client = SleeperClient(
            instance_id=FAKE_INSTANCE_ID,
            account_name=FAKE_ACCOUNT_ID,
        )
        yield client


def should_fall_back_to_aws_region_env_var_when_no_region_given(
    sleeper_client_default_region: SleeperClient,
) -> None:
    assert sleeper_client_default_region._s3_client.meta.region_name == "eu-west-2"


def should_discover_account_id_via_sts_when_not_provided(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    When account_name isn't supplied, SleeperClient should call
    sts:GetCallerIdentity to find it. The easiest externally-observable
    proof of that is the account_name that gets passed through to
    load_instance_properties, which we capture here.
    """
    captured_account_ids: list[str] = []

    def _capture(s3_resource, account_name, instance_id):
        captured_account_ids.append(account_name)
        return MagicMock(name="InstanceProperties")

    monkeypatch.setattr(f"{SLEEPER_CLIENT_MODULE}.load_instance_properties", _capture)

    with mock_aws():
        SleeperClient(instance_id=FAKE_INSTANCE_ID, region_name="eu-west-2")

    assert captured_account_ids == [FAKE_ACCOUNT_ID]
