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
package sleeper.core.properties.validation;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.PropertiesUtils;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.instance.InstanceProperties;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.validation.OptionalStack.CompactionStack;
import static sleeper.core.properties.validation.OptionalStack.GarbageCollectorStack;

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
    void shouldValidateWithOneStackNotMatchingCase() {
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(OPTIONAL_STACKS, "compactionstack");

        assertThatCode(properties::validate).doesNotThrowAnyException();
        assertThat(properties.getEnumList(OPTIONAL_STACKS, OptionalStack.class))
                .containsExactly(CompactionStack);
    }

    @Test
    void shouldValidateWithEmptyProperty() {
        InstanceProperties properties = createTestInstanceProperties();
        properties.setEnumList(OPTIONAL_STACKS, List.<OptionalStack>of());

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

    @Test
    void shouldReadNoOptionalStacksAfterPrintingAndLoading() {
        // Given
        InstanceProperties properties = InstanceProperties.createWithoutValidation(
                PropertiesUtils.loadProperties("sleeper.optional.stacks="));

        // When
        StringWriter writer = new StringWriter();
        InstanceProperties.createPrettyPrinter(new PrintWriter(writer))
                .print(properties);
        String written = writer.toString();

        // Then
        InstanceProperties found = InstanceProperties.createWithoutValidation(
                PropertiesUtils.loadProperties(written));
        assertThat(found.getEnumList(OPTIONAL_STACKS, OptionalStack.class)).isEmpty();
    }
}
