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
#pragma once

#include <cudf/column/column_device_view.cuh>
#include <cudf/column/column_view.hpp>
#include <cudf/detail/copy_if.cuh>
#include <cudf/scalar/scalar.hpp>
#include <cudf/strings/string_view.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/types.hpp>
#include <memory>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/mr/device/device_memory_resource.hpp>
#include <string>
#include <type_traits>

// functor for filter_table_by_range()
template <typename ElemType>
struct range_filter {
    range_filter(cudf::column_device_view const& in_sort_col,
                 ElemType const& in_low,
                 ElemType const& in_high) : sort_col{in_sort_col}, low(in_low), high(in_high) {}

    __device__ inline bool operator()(cudf::size_type i) {
        // test element[i] between low and high
        ElemType elem = sort_col.element<ElemType>(i);
        if constexpr (std::is_same_v<cudf::string_view, ElemType>) {
            return low.compare(elem) <= 0 && high.compare(elem) > 0;
        } else {
            return (low <= elem) && (high > elem);
        }
    }

   protected:
    cudf::column_device_view sort_col;
    ElemType low;
    ElemType high;
};

// Picks the correct cudf scalar type or void (deliberately cause compile error) if we don't know the type
template <typename T>
using cudf_scalar_t = std::conditional_t<std::is_integral_v<T> && std::is_signed_v<T>, cudf::numeric_scalar<T>,
                                         std::conditional_t<std::is_convertible_v<T, std::string>, cudf::string_scalar, void> >;

template <typename ColType>
std::unique_ptr<cudf::table>
filter_table_by_range(cudf::table_view const& input,
                      cudf::column_view const& sort_col,
                      ColType const& low,
                      ColType const& high,
                      rmm::mr::device_memory_resource* mr) {
    // creating lov and hiv inside the functor ctor doesn't work.  have to do it here.
    cudf_scalar_t<ColType> los(low, true, rmm::cuda_stream_default, mr);
    cudf_scalar_t<ColType> his(high, true, rmm::cuda_stream_default, mr);
    auto lov = los.value();  // not necessary for numeric scalar, but gets a cudf::string_view for a cudf::string_scalar
    auto hiv = his.value();
    auto sort_col_p = cudf::column_device_view::create(sort_col, rmm::cuda_stream_default);
    // Could use Class Template Auto Deduction below, but explicit is clearer
    return cudf::detail::copy_if(input, range_filter<decltype(lov)>{*sort_col_p, lov, hiv},
                                 rmm::cuda_stream_default, mr);
}
