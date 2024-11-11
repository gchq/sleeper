#include "lub.hpp"
#include <cudf/copying.hpp>
#include <cudf/scalar/scalar.hpp>
#include <memory>

void findLeastUpperBound(std::vector<cudf::table_view> const &views) noexcept {
    for (auto const &v : views) {
        std::unique_ptr<cudf::scalar> lastElement = cudf::get_element(v.column(0), v.column(0).size() - 1);
        }
}
