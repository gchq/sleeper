#/bin/bash
# Copyright 2022 Crown Copyright
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Locate where the Arrow cmake file is, but ignore the one in the Miniconda pkgs directory
export Arrow_DIR=$(find /opt/conda/envs/rapids -path '*pkgs*' -prune -o -name "ArrowConfig.cmake" -printf "%h\n" -quit)
# Locate where the Parquet cmake file is
export Parquet_DIR=$(find /opt/conda/envs/rapids -path '*pkgs*' -prune -o -name "ParquetConfig.cmake" -printf "%h\n" -quit)

# Run CMake
rm -rf build
cmake -S . -B build -DBUILD_SHARED_LIBS=1
