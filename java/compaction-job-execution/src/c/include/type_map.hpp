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

#include <cstddef>
#include <type_traits>
#include <variant>

/** This is a helper struct that will always fail a static_assertion. 
The template type stops the code failing on all compilations, since the
compiler must synthesize this with a type.
*/
template <typename T>
struct assert_false {
    static inline constexpr bool value = false;
};

/** A wrapper type that can be constant expression created without needing to create
 * a instance of T.
 */ 
template <typename T>
struct map_wrap {
    using type = T;
};

/**
 * @brief Find the index of a given type within a variant. The defaulted index parameter
 * should generally be left at zero when calling this function.
 * 
 * @tparam L the type to find in the variant
 * @tparam H head of variant type list
 * @tparam T tail types parameter pack
 * @param index internal counter for the parameter being examined
 * @return constexpr ::size_t the zero-based index of Type L in variant
 */
template <typename L, typename H, typename ...T>
static constexpr ::size_t indexLookup(std::variant<H,T...> const &, ::size_t const index = 0) {
    //if the type we're looking for matches the wrapped type at the head of the variant
    if constexpr (std::is_same_v<L, typename H::type>) {
        //then we've found the match
        return index;
    } else if constexpr (sizeof...(T) == 0) {
        //if there is no more tail and no match, then the type isn't present
        static_assert(assert_false<H>::value, "Type not present in variant");
        return 0; //never reached
    } else {
        //else recurse
        return indexLookup<L>(std::variant<T...> {}, index+1);
    }
}

/**
 * @brief Allows creation of a compile-time map from a list of types to another list of types.
 * The list of types must be equal in length.
 *
 * @code {.cpp}
 * using my_map = TypeMap<float,int>::To<double,std::string>;
 * ...
 * my_map::convert<float> type{}; //type = double
 * @endcode
 *
 * Code will not compile if the parameter packs are not equal in length, or if a type
 * not in the map is requested.
 *
 * @tparam Src List of types to map from
 */
template <typename ...Src>
struct TypeMap {
    template <typename ...Dest>
    struct To {
        static_assert(sizeof...(Src) == sizeof...(Dest), "List of types must have equal length");
        //this is the declaration that makes everything work. We use a parameter pack expansion to
        //wrap all the types. Because the invocation of indexLookup will create a variant that will
        //default construct it's types, it fails when it comes across a non-constexpr constructor
        //like std::string (C++17). To allow these types to be mapped, we use a wrapper type that
        //is constant expression constructible, but retains knowledge of the type it's wrapping.
        //
        // It works by taking the type L and looking up its index in a variant of the source types
        // and then uses variant_alternative_t to get the type from the destination types.
        template <typename L>
        using convert = std::variant_alternative_t<indexLookup<L>(std::variant<map_wrap<Src>...> {}), std::variant<Dest...>>;
    };
};
