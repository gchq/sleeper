/*
 * Copyright 2022 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <cudf/column/column_view.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <memory>
#include <rmm/mr/device/device_memory_resource.hpp>
#include <string>

#include "range_filter.cuh"

template std::unique_ptr<cudf::table>
filter_table_by_range(cudf::table_view const& input,
                      cudf::column_view const& sort_col,
                      std::string const& low,
                      std::string const& high,
                      rmm::mr::device_memory_resource* mr);
                      