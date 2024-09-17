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
package sleeper.configuration.properties.validation;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.SleeperPropertiesInvalidException;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.validation.OptionalStack.CompactionStack;
import static sleeper.configuration.properties.validation.OptionalStack.GarbageCollectorStack;

public class OptionalStackTest {

    @Test
    void shouldGenerateListOfDefaultValueForOptionalStack() {
        InstanceProperties properties = new InstanceProperties();
        assertThat(properties.get(OPTIONAL_STACKS))
                .isEqualTo("CompactionStack,GarbageCollectorStack,IngestStack,IngestBatcherStack," +
                        "PartitionSplittingStack,QueryStack,AthenaStack,EmrServerlessBulkImportStack," +
                        "EmrStudioStack,DashboardStack,TableMetricsStack");
    }

    @Test
    void shouldValidateWithOneValidOptionalStack() {
        InstanceProperties properties = createTestInstanceProperties();
        properties.setEnumList(OPTIONAL_STACKS, List.of(CompactionStack));

        assertThatCode(properties::validate).doesNotThrowAnyException();
        assertThat(properties.getEnumList(OPTIONAL_STACKS, OptionalStack.class))
                .containsExactly(CompactionStack);
    }

    @Test
    void shouldValidateWithTwoValidOptionalStacks() {
        InstanceProperties properties = createTestInstanceProperties();
        properties.setEnumList(OPTIONAL_STACKS, List.of(CompactionStack, GarbageCollectorStack));

        assertThatCode(properties::validate).doesNotThrowAnyException();
        assertThat(properties.getEnumList(OPTIONAL_STACKS, OptionalStack.class))
                .containsExactly(CompactionStack, GarbageCollectorStack);
    }

    @Test
    void shouldValidateWithEmptyProperty() {
        InstanceProperties properties = createTestInstanceProperties();
        properties.setEnumList(OPTIONAL_STACKS, List.of());

        assertThatCode(properties::validate).doesNotThrowAnyException();
        assertThat(properties.getEnumList(OPTIONAL_STACKS, OptionalStack.class))
                .isEmpty();
    }

    @Test
    void shouldFailValidationWithMisspeltOptionalStack() {
        InstanceProperties properties = createTestInstanceProperties();
        properties.setList(OPTIONAL_STACKS, List.of("CompacctionStack"));

        assertThatThrownBy(properties::validate)
                .isInstanceOf(SleeperPropertiesInvalidException.class)
                .hasMessageContaining("sleeper.optional.stacks")
                .hasMessageContaining("CompacctionStack");
    }
}
