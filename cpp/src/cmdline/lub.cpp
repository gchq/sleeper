#include "lub.hpp"
#include <cudf/copying.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <memory>
#include <type_info>

void findLeastUpperBound(std::vector<cudf::table_view> const &views) noexcept {
    auto action = [](auto &&t) { std::cout << "Type test " << typeid(t).name() << " t\n"; };

    for (::size_t c = 0, auto const &v : views) {
        std::unique_ptr<cudf::scalar> lastElement = cudf::get_element(v.column(0), v.column(0).size() - 1);
        cudf::type_dispatcher(lastElement->type, action, lastElement);
        c++;
    }
}
