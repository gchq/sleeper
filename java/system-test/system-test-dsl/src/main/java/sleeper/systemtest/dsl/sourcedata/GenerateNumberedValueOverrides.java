/*
 * Copyright 2022-2025 Crown Copyright
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

package sleeper.systemtest.dsl.sourcedata;

import sleeper.core.schema.Field;
import sleeper.core.schema.type.Type;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Stream;

public interface GenerateNumberedValueOverrides {
    Optional<GenerateNumberedValue> getGenerator(KeyType keyType, Field field);

    static GenerateNumberedValueOverrides none() {
        return (keyType, field) -> Optional.empty();
    }

    static GenerateNumberedValueOverrides overrides(GenerateNumberedValueOverrides... overrides) {
        return (keyType, field) -> Stream.of(overrides)
                .flatMap(override -> override.getGenerator(keyType, field).stream())
                .findFirst();
    }

    static GenerateNumberedValueOverrides overrideKeyAndFieldType(
            KeyType keyType, Class<? extends Type> fieldType, GenerateNumberedValue generator) {
        return overrideIf((foundKeyType, field) -> keyType == foundKeyType && fieldType.isInstance(field.getType()),
                generator);
    }

    static GenerateNumberedValueOverrides overrideField(String fieldName, GenerateNumberedValue generator) {
        return overrideIf((keyType, field) -> Objects.equals(fieldName, field.getName()),
                generator);
    }

    static GenerateNumberedValueOverrides overrideIf(BiPredicate<KeyType, Field> condition, GenerateNumberedValue generator) {
        return (keyType, field) -> {
            if (condition.test(keyType, field)) {
                return Optional.of(generator);
            } else {
                return Optional.empty();
            }
        };
    }
}
