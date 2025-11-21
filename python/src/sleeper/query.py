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

import json
import uuid
from itertools import chain


class Range:
    def __init__(self, min=None, min_inclusive=True, max=None, max_inclusive=False):
        if min is None:
            raise ValueError("min must be specified")
        if not isinstance(min_inclusive, bool):
            raise Exception("min_inclusive must be a bool")
        if not isinstance(max_inclusive, bool):
            raise Exception("max_inclusive must be a bool")
        self.min = min
        self.min_inclusive = min_inclusive
        self.max = max
        self.max_inclusive = max_inclusive

    @staticmethod
    def exact_value(value):
        return Range(min=value, min_inclusive=True, max=value, max_inclusive=True)

    @staticmethod
    def from_tuple(value):
        if len(value) == 2:
            return Range(min=value[0], max=value[1])
        if len(value) == 4:
            return Range(min=value[0], min_inclusive=value[1], max=value[2], max_inclusive=value[3])
        raise ValueError("Expected a tuple of length 2 or 4")

    @staticmethod
    def from_dict(value, min_inclusive=True, max_inclusive=True):
        return Range(value["min"], min_inclusive, value["max"], max_inclusive)

    def to_dict(self):
        return {"min": self.min, "minInclusive": self.min_inclusive, "max": self.max, "maxInclusive": self.max_inclusive}


class Region:
    def __init__(self, row_key_field_to_range: dict[str, Range] = None, strings_base64_encoded: bool = False):
        if not isinstance(row_key_field_to_range, dict):
            raise ValueError("row_key_field_to_range must be a dictionary")
        if len(row_key_field_to_range) == 0:
            raise Exception("Must provide at least one range")
        self.row_key_field_to_range = row_key_field_to_range
        self.strings_base64_encoded = strings_base64_encoded

    @staticmethod
    def exact_value(field: str, value, strings_base64_encoded: bool = False):
        return Region(row_key_field_to_range={field: Range.exact_value(value)}, strings_base64_encoded=strings_base64_encoded)

    @staticmethod
    def from_field_to_exact_value(field_to_value: dict, strings_base64_encoded: bool = False):
        return Region(row_key_field_to_range={field: Range.exact_value(value) for field, value in field_to_value.items()}, strings_base64_encoded=strings_base64_encoded)

    @staticmethod
    def list_from_field_to_exact_values(field_to_values: dict, strings_base64_encoded: bool = False):
        return list(chain.from_iterable(map(lambda item: map(lambda value: Region.exact_value(item[0], value, strings_base64_encoded), item[1]), field_to_values.items())))

    @staticmethod
    def from_field_to_tuple(field_to_tuple: dict, strings_base64_encoded: bool = False):
        return Region(row_key_field_to_range={field: Range.from_tuple(value) for field, value in field_to_tuple.items()}, strings_base64_encoded=strings_base64_encoded)

    @staticmethod
    def from_field_to_dict(field_to_dict: dict, min_inclusive: bool = True, max_inclusive: bool = True, strings_base64_encoded: bool = False):
        return Region(row_key_field_to_range={field: Range.from_dict(value, min_inclusive, max_inclusive) for field, value in field_to_dict.items()}, strings_base64_encoded=strings_base64_encoded)

    def to_dict(self):
        out = {field: range.to_dict() for field, range in self.row_key_field_to_range.items()}
        out["stringsBase64Encoded"] = self.strings_base64_encoded
        return out


class Query:
    def __init__(self, query_id: str = None, table_name: str = None, regions: list[Region] = None):
        if query_id is None:
            query_id = str(uuid.uuid4())
        if table_name is None:
            raise ValueError("table_name must be specified")
        if regions is None:
            raise ValueError("regions must be specified")
        self.query_id = query_id
        self.table_name = table_name
        self.regions = regions

    def to_dict(self):
        regions = [region.to_dict() for region in self.regions]
        value = {"tableName": self.table_name, "queryId": self.query_id, "type": "Query", "regions": regions}
        return value

    def to_json(self):
        return json.dumps(self.to_dict())
