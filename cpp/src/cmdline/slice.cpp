#include "slice.hpp"

#include <cudf/utilities/error.hpp>
#include <cudf/utilities/traits.hpp>
#include <cudf/utilities/type_dispatcher.hpp>

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