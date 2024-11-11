#include "lub.hpp"

#include <cudf/copying.hpp>
#include <cudf/lists/list_view.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/strings/string_view.hpp>
#include <cudf/structs/struct_view.hpp>
#include <cudf/utilities/traits.hpp>
#include <cudf/utilities/type_dispatcher.hpp>

#include <memory>
#include <string>
#include <type_traits>

::size_t findLeastUpperBound(std::vector<cudf::table_view> const &views) noexcept {

    auto action = []<typename T>(auto &&val) {
        using CudfScalarType = cudf::scalar_type_t<T>;
        CudfScalarType *scalar_ptr = static_cast<CudfScalarType *>(val.get());
        if constexpr (std::is_same_v<T, cudf::string_view>) {
            std::string stringValue = scalar_ptr->to_string();

        } else if constexpr (cudf::is_numeric<T>()) {
            typename CudfScalarType::value_type value = scalar_ptr->value();

        } else {
            CUDF_FAIL("Unexpected column type found.");
        }
    };

    for (::size_t t = 0; auto const &v : views) {
        std::unique_ptr<cudf::scalar> lastElement = cudf::get_element(v.column(0), v.column(0).size() - 1);
        cudf::type_dispatcher(lastElement->type(), action, lastElement);
        t++;
    }
}
