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
package sleeper.configuration.properties.table;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_FILES_COMMIT_ASYNC;
import static sleeper.configuration.properties.table.TableProperty.INGEST_FILES_COMMIT_ASYNC;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_ASYNC_COMMITS_ENABLED;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;

public class TablePropertiesAsyncCommitsTest {

    @Test
    void shouldEnableAsyncCommitsByDefaultForTransactionLogStateStore() {
        // Given
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.set(STATESTORE_CLASSNAME, "sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore");

        // When / Then
        assertThat(tableProperties.getBoolean(STATESTORE_ASYNC_COMMITS_ENABLED))
                .isEqualTo(true);
        assertThat(tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC))
                .isEqualTo(true);
    }

    @Test
    void shouldDisableAsyncCommitsByDefaultForDynamoDBStateStore() {
        // Given
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.set(STATESTORE_CLASSNAME, "sleeper.statestore.dynamodb.DynamoDBStateStore");

        // When / Then
        assertThat(tableProperties.getBoolean(STATESTORE_ASYNC_COMMITS_ENABLED))
                .isEqualTo(false);
        assertThat(tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC))
                .isEqualTo(false);
    }

    @Test
    void shouldDisableAsyncCommitsByTypeInInstanceProperty() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(DEFAULT_INGEST_FILES_COMMIT_ASYNC, "false");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(STATESTORE_CLASSNAME, "sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore");

        // When / Then
        assertThat(tableProperties.getBoolean(STATESTORE_ASYNC_COMMITS_ENABLED))
                .isEqualTo(true);
        assertThat(tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC))
                .isEqualTo(false);
    }

    @Test
    void shouldEnableAsyncCommitsInTableProperty() {
        // Given
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.set(STATESTORE_CLASSNAME, "sleeper.statestore.dynamodb.DynamoDBStateStore");
        tableProperties.set(STATESTORE_ASYNC_COMMITS_ENABLED, "true");

        // When / Then
        assertThat(tableProperties.getBoolean(STATESTORE_ASYNC_COMMITS_ENABLED))
                .isEqualTo(true);
        assertThat(tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC))
                .isEqualTo(true);
    }

    @Test
    void shouldOverrideCommitTypePropertyWhenCommitsDisabledForTable() {
        // Given
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.set(STATESTORE_ASYNC_COMMITS_ENABLED, "false");
        tableProperties.set(INGEST_FILES_COMMIT_ASYNC, "true");

        // When / Then
        assertThat(tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC))
                .isEqualTo(false);
    }
}
