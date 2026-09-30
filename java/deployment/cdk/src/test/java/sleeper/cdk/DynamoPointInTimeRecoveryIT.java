/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.cdk;

import org.junit.jupiter.api.Test;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.assertions.Template;
import software.amazon.awscdk.services.dynamodb.CfnTable;

import sleeper.cdk.testutil.SleeperStackTestBase;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_RELEASE;
import static sleeper.core.properties.instance.TableStateProperty.DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY;
import static sleeper.core.properties.instance.TableStateProperty.TABLE_INDEX_DYNAMO_POINT_IN_TIME_RECOVERY;

class DynamoPointInTimeRecoveryIT extends SleeperStackTestBase {

    @Test
    void shouldEnableRecoveryOnEveryDynamoTable() {
        instanceProperties.set(DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY, "true");
        for (CfnTable table : createTables()) {
            assertRecovery(table, true);
        }
    }

    @Test
    void shouldKeepRecoveryDisabledByDefault() {
        for (CfnTable table : createTables()) {
            assertRecovery(table, false);
        }
    }

    @Test
    void shouldRespectTableIndexOverride() {
        instanceProperties.set(DEFAULT_DYNAMO_POINT_IN_TIME_RECOVERY, "true");
        instanceProperties.set(TABLE_INDEX_DYNAMO_POINT_IN_TIME_RECOVERY, "false");
        List<CfnTable> tables = createTables();
        List<CfnTable> indexTables = tables.stream()
                .filter(table -> table.getNode().getPath().contains("TableIndex"))
                .toList();
        assertThat(indexTables).hasSize(3);
        for (CfnTable table : tables) {
            assertRecovery(table, !indexTables.contains(table));
        }
    }

    private List<CfnTable> createTables() {
        instanceProperties.unset(OPTIONAL_STACKS);
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_RELEASE, "emr-1.2.3");
        SleeperInstance.create(rootStack, instanceProps());
        List<CfnTable> tables = rootStack.getNode().findAll().stream()
                .filter(CfnTable.class::isInstance)
                .map(CfnTable.class::cast)
                .toList();
        assertThat(tables).hasSize(15);
        return tables;
    }

    private void assertRecovery(CfnTable table, boolean expected) {
        Template.fromStack(Stack.of(table)).hasResourceProperties("AWS::DynamoDB::Table", Map.of(
                "TableName", table.getTableName(),
                "PointInTimeRecoverySpecification", Map.of("PointInTimeRecoveryEnabled", expected)));
    }
}
