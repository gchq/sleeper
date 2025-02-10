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
package sleeper.core.properties.table;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.DefaultAsyncCommitBehaviour;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_ASYNC_COMMIT_BEHAVIOUR;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_COMPACTION_JOB_COMMIT_ASYNC;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_FILES_COMMIT_ASYNC;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_PARTITION_SPLIT_ASYNC_COMMIT;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.INGEST_FILES_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_ASYNC_COMMIT;
import static sleeper.core.properties.table.TableProperty.STATESTORE_ASYNC_COMMITS_ENABLED;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;

public class TablePropertiesAsyncCommitsTest {

    @Test
    void shouldEnableAsyncCommitsByDefaultForTransactionLogStateStore() {
        // Given
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.set(STATESTORE_CLASSNAME, "sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore");

        // When / Then
        assertThat(tableProperties.getBoolean(STATESTORE_ASYNC_COMMITS_ENABLED))
                .isTrue();
        assertThat(tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC))
                .isTrue();
    }

    @Test
    void shouldDisableAsyncCommitsInInstanceProperty() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.setEnum(DEFAULT_ASYNC_COMMIT_BEHAVIOUR, DefaultAsyncCommitBehaviour.DISABLED);
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(STATESTORE_CLASSNAME, "sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore");

        // When / Then
        assertThat(tableProperties.getBoolean(STATESTORE_ASYNC_COMMITS_ENABLED))
                .isFalse();
        assertThat(tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC))
                .isFalse();
    }

    @Test
    void shouldDisableAsyncCommitsByDefaultForS3tateStore() {
        // Given
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.set(STATESTORE_CLASSNAME, "sleeper.statestore.s3.S3StateStore");

        // When / Then
        assertThat(tableProperties.getBoolean(STATESTORE_ASYNC_COMMITS_ENABLED))
                .isFalse();
        assertThat(tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC))
                .isFalse();
    }

    @Test
    void shouldDisableAsyncCommitsByTypeInInstanceProperty() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.setEnum(DEFAULT_ASYNC_COMMIT_BEHAVIOUR, DefaultAsyncCommitBehaviour.ALL_IMPLEMENTATIONS);
        instanceProperties.set(DEFAULT_INGEST_FILES_COMMIT_ASYNC, "false");
        TableProperties tableProperties = new TableProperties(instanceProperties);

        // When / Then
        assertThat(tableProperties.getBoolean(STATESTORE_ASYNC_COMMITS_ENABLED))
                .isTrue();
        assertThat(tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC))
                .isFalse();
    }

    @Test
    void shouldSetAsyncCommitByTypeFromDefaultPropertiesWhenTableIsEnabled() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.setEnum(DEFAULT_ASYNC_COMMIT_BEHAVIOUR, DefaultAsyncCommitBehaviour.DISABLED);
        instanceProperties.set(DEFAULT_INGEST_FILES_COMMIT_ASYNC, "false");
        instanceProperties.set(DEFAULT_COMPACTION_JOB_COMMIT_ASYNC, "true");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(STATESTORE_ASYNC_COMMITS_ENABLED, "true");

        // When / Then
        assertThat(tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC))
                .isFalse();
        assertThat(tableProperties.getBoolean(COMPACTION_JOB_COMMIT_ASYNC))
                .isTrue();
    }

    @Test
    void shouldIgnoreAsyncCommitTypeEnabledForTableWhenTableIsDisabled() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.setEnum(DEFAULT_ASYNC_COMMIT_BEHAVIOUR, DefaultAsyncCommitBehaviour.DISABLED);
        instanceProperties.set(DEFAULT_INGEST_FILES_COMMIT_ASYNC, "false");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(STATESTORE_ASYNC_COMMITS_ENABLED, "false");
        tableProperties.set(INGEST_FILES_COMMIT_ASYNC, "true");

        // When / Then
        assertThat(tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC))
                .isFalse();
    }

    @Test
    void shouldDisableAsyncCommitForOneTypeWhenTableIsEnabledAndTypeIsEnabledByDefault() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.setEnum(DEFAULT_ASYNC_COMMIT_BEHAVIOUR, DefaultAsyncCommitBehaviour.DISABLED);
        instanceProperties.set(DEFAULT_INGEST_FILES_COMMIT_ASYNC, "true");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(STATESTORE_ASYNC_COMMITS_ENABLED, "true");
        tableProperties.set(INGEST_FILES_COMMIT_ASYNC, "false");

        // When / Then
        assertThat(tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC))
                .isFalse();
    }

    @Test
    void shouldEnableAsyncCommitForOneTypeWhenTableIsEnabledAndTypeIsDisabledByDefault() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.setEnum(DEFAULT_ASYNC_COMMIT_BEHAVIOUR, DefaultAsyncCommitBehaviour.DISABLED);
        instanceProperties.set(DEFAULT_INGEST_FILES_COMMIT_ASYNC, "false");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(STATESTORE_ASYNC_COMMITS_ENABLED, "true");
        tableProperties.set(INGEST_FILES_COMMIT_ASYNC, "true");

        // When / Then
        assertThat(tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC))
                .isTrue();
    }

    @Test
    void shouldEnableAsyncCommitForOneTypeWhenTableIsDisabledByDefaultButNotSetOnTable() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.setEnum(DEFAULT_ASYNC_COMMIT_BEHAVIOUR, DefaultAsyncCommitBehaviour.DISABLED);
        instanceProperties.set(DEFAULT_INGEST_FILES_COMMIT_ASYNC, "false");
        instanceProperties.set(DEFAULT_COMPACTION_JOB_COMMIT_ASYNC, "false");
        instanceProperties.set(DEFAULT_PARTITION_SPLIT_ASYNC_COMMIT, "true");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(INGEST_FILES_COMMIT_ASYNC, "true");

        // When / Then
        assertThat(tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC))
                .isTrue();
        assertThat(tableProperties.getBoolean(COMPACTION_JOB_COMMIT_ASYNC))
                .isFalse();
        assertThat(tableProperties.getBoolean(PARTITION_SPLIT_ASYNC_COMMIT))
                .isFalse();
    }
}
