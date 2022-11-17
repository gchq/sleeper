
#ifndef _filters_h
#define _filters_h

#include <cstdint>
#include <cudf/column/column_view.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <memory>
#include <rmm/mr/device/device_memory_resource.hpp>
#include <string>

template <typename ColType>
std::unique_ptr<cudf::table>
filter_table_by_range(cudf::table_view const& input,
                      cudf::column_view const& sort_col,
                      ColType const& low,
                      ColType const& high,
                      rmm::mr::device_memory_resource* mr = rmm::mr::get_current_device_resource());

std::unique_ptr<cudf::table>
ageoff_table(cudf::table_view const& input,
             cudf::column_view const& ts_col,
             int64_t cutoff,
             rmm::mr::device_memory_resource* mr = rmm::mr::get_current_device_resource());

std::unique_ptr<cudf::table>
filter_table_by_range_and_age(cudf::table_view const& input,
                              cudf::column_view const& sort_col,
                              cudf::column_view const& ts_col,
                              std::string const& low,
                              std::string const& high,
                              int64_t cutoff,
                              rmm::mr::device_memory_resource* mr = rmm::mr::get_current_device_resource());
#endif
