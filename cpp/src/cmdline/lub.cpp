/*
 * Copyright 2022-2025 Crown Copyright
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
#include "lub.hpp"

#include <cudf/copying.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/utilities/error.hpp>
#include <cudf/utilities/traits.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <spdlog/spdlog.h>

#include <type_traits>
#include <utility>

::size_t findLeastUpperBound(std::vector<std::unique_ptr<cudf::table>> const &tables, ::size_t const colNo) {

    auto action = [&tables, &colNo]<typename T>() {
        using CudfScalarType = cudf::scalar_type_t<T>;
        ::size_t lubTableIndex = 0;
        std::unique_ptr<cudf::scalar> currentLub;
        // Loop over each table view, grab the last element in the sort column and find the lowest
        for (::size_t idx = 0; std::unique_ptr<cudf::table> const &table : tables) {
            if (table->num_rows() > 0) {
                std::unique_ptr<cudf::scalar> lastElement =
                  cudf::get_element(table->view().column(colNo), table->view().column(colNo).size() - 1);
                // Skip rest of checks if this is the first table with data
                if (!currentLub) {
                    SPDLOG_INFO("Table {:d} is first with data", idx);
                    currentLub = std::move(lastElement);
                    lubTableIndex = idx;
                } else {
                    auto const lub_ptr = static_cast<CudfScalarType *>(currentLub.get());
                    auto const lastElement_ptr = static_cast<CudfScalarType *>(lastElement.get());

                    // Branch on template type if it's a string column or numeric column
                    if constexpr (std::is_same_v<T, cudf::string_view>) {
                        auto const lub = lub_ptr->to_string();
                        auto const last = lastElement_ptr->to_string();
                        // Perform string compare
                        if (last < lub) {
                            currentLub = std::move(lastElement);
                            lubTableIndex = idx;
                            SPDLOG_INFO("Current least bound '{}' candidate '{}' is lower", lub, last);
                        } else {
                            SPDLOG_INFO("Current least bound '{}' candidate '{}'", lub, last);
                        }
                    } else if constexpr (cudf::is_integral_not_bool<T>()) {
                        auto const lub = lub_ptr->value();
                        auto const last = lastElement_ptr->value();
                        // Perform numeric compare
                        if (std::cmp_less(last, lub)) {
                            currentLub = std::move(lastElement);
                            lubTableIndex = idx;
                            SPDLOG_INFO("Current least bound '{}' candidate '{}' is lower", lub, last);
                        } else {
                            SPDLOG_INFO("Current least bound '{}' candidate '{}'", lub, last);
                        }
                    } else {
                        CUDF_FAIL("Column type not supported");
                    }
                }
            }
            idx++;
        }
        SPDLOG_INFO("Found least upper bound on file no {:d}", lubTableIndex);
        CUDF_EXPECTS(currentLub, "No table contained any data!");
        return lubTableIndex;
    };

    CUDF_EXPECTS(!tables.empty(), "vector of tables cannot be empty");
    return cudf::type_dispatcher(tables.front()->get_column(colNo).type(), action);
}
