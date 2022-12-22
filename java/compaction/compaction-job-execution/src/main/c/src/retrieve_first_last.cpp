#include "retrieve_first_last.hpp"

#include <arrow/api.h>

#include <cstdint>
#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/copying.hpp>
#include <cudf/interop.hpp>
#include <cudf/table/table.hpp>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Woverloaded-virtual" //these trigger here unnecessarily

template <typename T>
struct ScalarVisit : public arrow::ScalarVisitor {
    ScalarVisit(std::vector<T>& ref) : items(ref) {}

    std::vector<T>& items;

    arrow::Status Visit(const arrow::Scalar& s) {
        return arrow::Status::NotImplemented("No valid visitor for",
                                             s.type->ToString());
    }

    template <typename V>
    arrow::enable_if_t<arrow::is_integer_type<typename V::TypeClass>::value &&
                           std::is_integral_v<T>,
                       arrow::Status>
    Visit(V const& scalar) {
        items.push_back(static_cast<T>(scalar.value));
        return arrow::Status::OK();
    }

    template <typename V>
    arrow::enable_if_t<arrow::is_string_like_type<typename V::TypeClass>::value &&
                           std::is_convertible_v<T, std::string>,
                       arrow::Status>
    Visit(V const& scalar) {
        items.push_back(scalar.ToString());
        return arrow::Status::OK();
    }
};

#pragma GCC diagnostic pop

template <typename T>
std::vector<T> getFirstLast(cudf::column_view const& firstColumn) {
    std::vector<cudf::column_view> sliced = cudf::slice(firstColumn, {0, 1, firstColumn.size() - 1, firstColumn.size()});
    std::vector<std::unique_ptr<cudf::column>> column;
    column.push_back(cudf::concatenate({sliced}));
    cudf::table tabled{std::move(column)};
    // pull this back to cpu
    std::shared_ptr<arrow::Table> cpuTable = cudf::to_arrow(tabled.view(), {{""}});
    std::shared_ptr<arrow::ChunkedArray> cpuColumn = cpuTable->column(0);
    //now get first and second items of this 2 value table
    std::vector<T> items;
    ScalarVisit<T> visitor{items};
    arrow::Result<std::shared_ptr<arrow::Scalar>> firstVal = cpuColumn->GetScalar(0);
    arrow::Result<std::shared_ptr<arrow::Scalar>> secondVal = cpuColumn->GetScalar(1);
    arrow::VisitScalarInline(*firstVal.ValueOrDie(), &visitor);
    arrow::VisitScalarInline(*secondVal.ValueOrDie(), &visitor);
    return items;
}

//instantiate this template for the types we need
template std::vector<std::string> getFirstLast(cudf::column_view const& firstColumn);
template std::vector<std::int32_t> getFirstLast(cudf::column_view const& firstColumn);
template std::vector<std::int64_t> getFirstLast(cudf::column_view const& firstColumn);