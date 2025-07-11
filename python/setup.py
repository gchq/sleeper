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
from setuptools import setup

setup(
    name="sleeper",
    version="0.32.0.dev1",
    description="Python client for Sleeper",
    install_requires=[
        "pyarrow",
        "boto3",
        "s3fs",
        "boto3-stubs[essential]",
        "jproperties",
        "ruff",
        "pytest",
        "testcontainers[localstack]",
    ],
    package_dir={"": "src"},
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3 :: Only",
        "Typing :: Typed",
    ],
)
