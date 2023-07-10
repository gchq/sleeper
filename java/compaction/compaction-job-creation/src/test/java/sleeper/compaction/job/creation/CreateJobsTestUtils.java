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
package sleeper.compaction.job.creation;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.statestore.FileInfo;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.CommonProperties.FILE_SYSTEM;
import static sleeper.configuration.properties.CommonProperties.ID;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.SIZE_RATIO_COMPACTION_STRATEGY_RATIO;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class CreateJobsTestUtils {

    private CreateJobsTestUtils() {
    }

    public static Schema createSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new LongType()))
                .valueFields(
                        new Field("value1", new LongType()),
                        new Field("value2", new LongType()))
                .build();
    }

    public static InstanceProperties createInstanceProperties() {

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, UUID.randomUUID().toString());
        instanceProperties.set(CONFIG_BUCKET, UUID.randomUUID().toString());
        instanceProperties.set(FILE_SYSTEM, "");
        return instanceProperties;
    }

    public static TableProperties createTableProperties(Schema schema, InstanceProperties instanceProperties) {
        String tableName = "test-table";
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setSchema(schema);
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        tableProperties.set(SIZE_RATIO_COMPACTION_STRATEGY_RATIO, "1");
        return tableProperties;
    }

    public static String assertAllFilesHaveJobId(List<FileInfo> files) {
        assertThat(files).isNotEmpty();
        String jobId = files.get(0).getJobId();
        assertThat(jobId).isNotNull();
        assertThat(files).extracting(FileInfo::getJobId)
                .allMatch(jobId::equals);
        return jobId;
    }
}
