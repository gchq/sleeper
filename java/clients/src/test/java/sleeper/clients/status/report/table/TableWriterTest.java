/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.clients.status.report.table;

import org.junit.jupiter.api.Test;

import sleeper.clients.testutil.ToStringPrintStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.testutil.ClientTestUtils.example;

public class TableWriterTest {

    @Test
    public void shouldOutputSingleField() throws Exception {
        // Given
        TableWriterFactory.Builder factoryBuilder = TableWriterFactory.builder();
        TableField field = factoryBuilder.addField("Field");
        TableWriterFactory factory = factoryBuilder.build();
        ToStringPrintStream output = new ToStringPrintStream();

        // When
        factory.tableBuilder()
                .row(row -> row.value(field, "Value"))
                .build().write(output.getPrintStream());

        // Then
        assertThat(output).hasToString(example("reports/table/singleField.txt"));
    }

    @Test
    public void shouldOutputEmptyField() throws Exception {
        // Given
        TableWriterFactory.Builder factoryBuilder = TableWriterFactory.builder();
        factoryBuilder.addField("Field");
        TableWriterFactory factory = factoryBuilder.build();
        ToStringPrintStream output = new ToStringPrintStream();

        // When
        factory.tableBuilder()
                .row(row -> {
                })
                .build().write(output.getPrintStream());

        // Then
        assertThat(output).hasToString(example("reports/table/emptyField.txt"));
    }

    @Test
    public void shouldOutputTwoRowsWithDifferentLengths() throws Exception {
        // Given
        TableWriterFactory.Builder factoryBuilder = TableWriterFactory.builder();
        TableField field = factoryBuilder.addField("Field");
        TableWriterFactory factory = factoryBuilder.build();
        ToStringPrintStream output = new ToStringPrintStream();

        // When
        factory.tableBuilder()
                .row(row -> row.value(field, "Tiny"))
                .row(row -> row.value(field, "Very very long"))
                .build().write(output.getPrintStream());

        // Then
        assertThat(output).hasToString(example("reports/table/twoRows.txt"));
    }

    @Test
    public void shouldOutputTwoFieldsWithDifferentLengths() throws Exception {
        // Given
        TableWriterFactory.Builder factoryBuilder = TableWriterFactory.builder();
        factoryBuilder.addField("Tiny");
        factoryBuilder.addField("Very very long");
        TableWriterFactory factory = factoryBuilder.build();
        ToStringPrintStream output = new ToStringPrintStream();

        // When
        factory.tableBuilder()
                .build().write(output.getPrintStream());

        // Then
        assertThat(output).hasToString(example("reports/table/twoFields.txt"));
    }
}
