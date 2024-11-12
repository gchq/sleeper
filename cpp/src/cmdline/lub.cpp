#include "lub.hpp"

#include <cudf/copying.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/utilities/error.hpp>
#include <cudf/utilities/traits.hpp>
#include <cudf/utilities/type_dispatcher.hpp>

#include <iostream>
#include <memory>
#include <type_traits>
#include <utility>

::size_t findLeastUpperBound(std::vector<cudf::table_view> const &views, ::size_t const colNo) {

    auto action = [&views, &colNo]<typename T>() {
        using CudfScalarType = cudf::scalar_type_t<T>;
        ::size_t lubTableIndex = 0;
        std::unique_ptr<cudf::scalar> currentLub =
          cudf::get_element(views.front().column(colNo), views.front().column(colNo).size() - 1);
        // Loop over each table view, grab the last element in the sort column and find the lowest
        for (::size_t idx = 0; cudf::table_view const &view : views) {

            std::unique_ptr<cudf::scalar> lastElement =
              cudf::get_element(view.column(colNo), view.column(colNo).size() - 1);
            auto const lub_ptr = static_cast<CudfScalarType *>(currentLub.get());
            auto const lastElement_ptr = static_cast<CudfScalarType *>(lastElement.get());

            // Branch on template type if it's a string column or numeric column
            if constexpr (std::is_same_v<T, cudf::string_view>) {
                auto const lub = lub_ptr->to_string();
                auto const last = lastElement_ptr->to_string();
                std::cout << "Current least bound '" << lub << "' testing '" << last << "' ";
                // Perform string compare
                if (last < lub) {
                    currentLub = std::move(lastElement);
                    lubTableIndex = idx;
                    std::cout << "is lower so updating bound";
                }

            } else if constexpr (cudf::is_integral_not_bool<T>()) {
                auto const lub = lub_ptr->value();
                auto const last = lastElement_ptr->value();
                std::cout << "Current least bound '" << lub << "' testing '" << last << "' ";
                // Perform numeric compare
                if (std::cmp_less(last, lub)) {
                    currentLub = std::move(lastElement);
                    lubTableIndex = idx;
                    std::cout << "is lower so updating bound";
                }
            } else {
                CUDF_FAIL("Column type not supported");
            }

            std::cout << std::endl;
            idx++;
        }
        return lubTableIndex;
    };

    CUDF_EXPECTS(!views.empty(), "vector of tables cannot be empty");
    return cudf::type_dispatcher(views.front().column(colNo).type(), action);
}
