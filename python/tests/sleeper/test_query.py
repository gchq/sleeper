#  Copyright 2022-2025 Crown Copyright
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

from sleeper.query import Query, Range, Region


def should_build_exact_query_dict():
    # When
    query = Query(query_id="my-query", table_name="my-table", regions=[Region.exact_value("key", "value")])

    # Then
    assert query.to_dict() == {
        "queryId": "my-query",
        "regions": [{"key": {"min": "value", "minInclusive": True, "max": "value", "maxInclusive": True}, "stringsBase64Encoded": False}],
        "tableName": "my-table",
        "type": "Query",
    }


def should_build_range_query_dict():
    # When
    query = Query(query_id="my-query", table_name="my-table", regions=[Region(row_key_field_to_range={"key": Range(min="min-value", max="max-value")})])

    # Then
    assert query.to_dict() == {
        "queryId": "my-query",
        "regions": [{"key": {"min": "min-value", "minInclusive": True, "max": "max-value", "maxInclusive": False}, "stringsBase64Encoded": False}],
        "tableName": "my-table",
        "type": "Query",
    }


def should_read_region_from_field_to_dict():
    # When
    region = Region.from_field_to_dict({"number": {"min": 2, "max": 4}, "string": {"min": "a", "max": "c"}})

    # Then
    assert region.to_dict() == {
        "number": {"min": 2, "minInclusive": True, "max": 4, "maxInclusive": True},
        "string": {"min": "a", "minInclusive": True, "max": "c", "maxInclusive": True},
        "stringsBase64Encoded": False,
    }


def should_read_region_from_field_to_tuple():
    # When
    region = Region.from_field_to_tuple({"number": [2, 4], "string": ["a", False, "c", True]})

    # Then
    assert region.to_dict() == {
        "number": {"min": 2, "minInclusive": True, "max": 4, "maxInclusive": False},
        "string": {"min": "a", "minInclusive": False, "max": "c", "maxInclusive": True},
        "stringsBase64Encoded": False,
    }


def should_list_regions_from_field_to_exact_values():
    # When
    regions = Region.list_from_field_to_exact_values({"number": [2, 4], "string": ["a", "b"]})

    # Then
    assert [region.to_dict() for region in regions] == [
        {
            "number": {"min": 2, "minInclusive": True, "max": 2, "maxInclusive": True},
            "stringsBase64Encoded": False,
        },
        {
            "number": {"min": 4, "minInclusive": True, "max": 4, "maxInclusive": True},
            "stringsBase64Encoded": False,
        },
        {
            "string": {"min": "a", "minInclusive": True, "max": "a", "maxInclusive": True},
            "stringsBase64Encoded": False,
        },
        {
            "string": {"min": "b", "minInclusive": True, "max": "b", "maxInclusive": True},
            "stringsBase64Encoded": False,
        },
    ]
