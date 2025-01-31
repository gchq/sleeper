/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.core.testutils;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.testutils.SupplierTestHelper.exampleUUID;
import static sleeper.core.testutils.SupplierTestHelper.numberedUUID;
import static sleeper.core.testutils.SupplierTestHelper.supplyNumberedIdsWithPrefix;

public class SupplierTestHelperTest {

    @Test
    void shouldGenerateUUIDRepeatingCharacter() {
        assertThat(exampleUUID("test", 1))
                .isEqualTo("test1111-1111-1111-1111-111111111111");
    }

    @Test
    void shouldGenerateUUIDWithNumberAtEnd() {
        assertThat(numberedUUID("test", 12345))
                .isEqualTo("test0000-0000-0000-0000-000000012345");
    }

    @Test
    void shouldSupplyUUIDsWithNumberAtEnd() {
        Supplier<String> supplier = supplyNumberedIdsWithPrefix("object");
        assertThat(List.of(supplier.get(), supplier.get(), supplier.get()))
                .containsExactly(
                        "object00-0000-0000-0000-000000000001",
                        "object00-0000-0000-0000-000000000002",
                        "object00-0000-0000-0000-000000000003");
    }

}
