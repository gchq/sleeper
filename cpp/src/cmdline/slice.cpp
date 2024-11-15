#include "slice.hpp"

#include <cudf/column/column.hpp>
#include <cudf/copying.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/search.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/error.hpp>
#include <cudf/utilities/traits.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <spdlog/spdlog.h>

#include <cstddef>
#include <memory>

int convertInteger(cudf::scalar const &scalar) {

    auto action = [&scalar]<typename T>() {
        using CudfScalarType = cudf::scalar_type_t<T>;
        if constexpr (cudf::is_integral_not_bool<T>()) {
            auto const value = static_cast<CudfScalarType const &>(scalar);
            return static_cast<int>(value.value());
        } else {
            CUDF_FAIL("Column type not integer");
            return 0;
        }
    };

    return cudf::type_dispatcher(scalar.type(), action);
}

std::pair<std::vector<cudf::table_view>, std::vector<cudf::table_view>> splitAtNeedle(cudf::table_view const &needle,
  std::vector<std::unique_ptr<cudf::table>> const &haystacks) {
    std::pair<std::vector<cudf::table_view>, std::vector<cudf::table_view>> lists;
    std::vector<cudf::table_view> tablesToMerge;
    std::vector<cudf::table_view> remainingFragments;
    tablesToMerge.reserve(haystacks.size());
    remainingFragments.reserve(haystacks.size());

    // Split each table at the point of that needle
    for (::size_t idx = 0; auto const &table : haystacks) {
        // Empty table? Just push the empty table and skip
        if (table->num_rows() == 0) {
            lists.first.push_back(table->view());
            lists.second.push_back(table->view());
            SPDLOG_INFO("File {:d} Table size after split {:d} and {:d}", idx, 0, 0);
        } else {
            // Find needle in each table view, table is "haystack"
            std::unique_ptr<cudf::column> splitPoint =
              cudf::upper_bound(table->select({ 0 }), needle, { cudf::order::ASCENDING }, { cudf::null_order::AFTER });
            CUDF_EXPECTS(splitPoint->size() == 1, "Split result should be single row");
            // Get this index back to host
            std::unique_ptr<cudf::scalar> splitIndex = cudf::get_element(*splitPoint, 0);
            int const splitPos = convertInteger(*splitIndex);
            // Now split this table at that index
            std::vector<cudf::table_view> splitTables = cudf::split(*table, { splitPos });
            CUDF_EXPECTS(splitTables.size() == 2, "Should be two tables from split");
            SPDLOG_INFO("File {:d} Table size after split {:d} and {:d}",
              idx,
              splitTables[0].num_rows(),
              splitTables[1].num_rows());
            lists.first.push_back(std::move(splitTables[0]));
            lists.second.push_back(std::move(splitTables[1]));
        }
        idx++;
    }
    return lists;
}