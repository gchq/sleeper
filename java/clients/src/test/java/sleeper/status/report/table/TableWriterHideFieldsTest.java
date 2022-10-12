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
package sleeper.status.report.table;

import org.junit.Test;
import sleeper.ToStringPrintStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ClientTestUtils.example;

public class TableWriterHideFieldsTest {

    @Test
    public void shouldHideFirstField() throws Exception {
        // Given
        TableWriterFactory.Builder factoryBuilder = TableWriterFactory.builder();
        TableField field1 = factoryBuilder.addField("First");
        TableField field2 = factoryBuilder.addField("Field");
        TableWriterFactory factory = factoryBuilder.build();
        ToStringPrintStream output = new ToStringPrintStream();

        // When
        factory.tableBuilder()
                .row(row -> row
                        .value(field1, "First")
                        .value(field2, "Value"))
                .showField(field1, false)
                .build().write(output.getPrintStream());

        // Then
        assertThat(output).hasToString(example("reports/table/singleField.txt"));
    }

    @Test
    public void shouldHideSecondField() throws Exception {
        // Given
        TableWriterFactory.Builder factoryBuilder = TableWriterFactory.builder();
        TableField field1 = factoryBuilder.addField("Field");
        TableField field2 = factoryBuilder.addField("Other");
        TableWriterFactory factory = factoryBuilder.build();
        ToStringPrintStream output = new ToStringPrintStream();

        // When
        factory.tableBuilder()
                .row(row -> row
                        .value(field1, "Value")
                        .value(field2, "Other"))
                .showField(field2, false)
                .build().write(output.getPrintStream());

        // Then
        assertThat(output).hasToString(example("reports/table/singleField.txt"));
    }
}
