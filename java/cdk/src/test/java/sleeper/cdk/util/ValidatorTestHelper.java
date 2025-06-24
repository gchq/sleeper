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

package sleeper.cdk.util;

import org.apache.commons.io.FileUtils;

import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;

public class ValidatorTestHelper {
    private ValidatorTestHelper() {
    }

    static void setupTablesPropertiesFile(Path temporaryFolder, String tableName) throws IOException {
        String tableSchema = "{\n" +
                "  \"rowKeyFields\": [ \n" +
                "    {\n" +
                "      \"name\": \"key\",\n" +
                "      \"type\": \"StringType\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"sortKeyFields\": [\n" +
                "    {\n" +
                "      \"name\": \"timestamp\",\n" +
                "      \"type\": \"LongType\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"valueFields\": [\n" +
                "    {\n" +
                "      \"name\": \"value\",\n" +
                "      \"type\": \"StringType\"\n" +
                "    }\n" +
                "  ]\n" +
                "}\n";

        File tableSchemaFile = new File(temporaryFolder.toString(), "schema.json");
        FileUtils.write(tableSchemaFile, tableSchema, Charset.defaultCharset());

        String tableConfiguration = "" +
                String.format("sleeper.table.name=%s\n", tableName) +
                String.format("sleeper.table.statestore.classname=%s\n", DynamoDBTransactionLogStateStore.class.getSimpleName());

        File tablePropertiesFile = new File(temporaryFolder.toString(), "table.properties");
        FileUtils.write(tablePropertiesFile, tableConfiguration, Charset.defaultCharset());
    }
}
