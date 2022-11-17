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
#include "histo.hpp"

#include <gmp.h>

#include <algorithm>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>

void debase(mpz_t ret, std::string const& input) {
    mpz_set_ui(ret, 0);
    mpz_t power;
    mpz_init_set_ui(power, 1);
    mpz_t multiple;
    mpz_init(multiple);
    for (auto it = input.crbegin(); it != input.crend(); ++it) {
        auto const& s = *it;
        // get index into alphabet of this character type
        decltype(ALPHABET)::const_iterator loc = std::find(ALPHABET.cbegin(), ALPHABET.cend(), s);
        if (loc == ALPHABET.cend()) {
            mpz_clears(power, multiple, NULL);
            std::cerr << "Looking for " << static_cast<int>(s) << ".....\n";
            throw std::invalid_argument("couldn't find character in alphabet");
        }
        ::size_t distance = static_cast<::size_t>(std::distance(ALPHABET.cbegin(), loc));
        mpz_addmul_ui(ret, power, distance);
        mpz_mul_ui(power, power, ALPHABET_LENGTH);
    }
    mpz_clears(power, multiple, NULL);
}
