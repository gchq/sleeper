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

package sleeper.bulkimport.starter.executor;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.properties.instance.InstanceProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;

public class BulkImportArgumentsTest {

    private final InstanceProperties instanceProperties = new InstanceProperties();

    @Test
    void shouldConstructArgs() {
        // Given
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        instanceProperties.set(JARS_BUCKET, "jarsBucket");
        instanceProperties.set(CONFIG_BUCKET, "configBucket");
        instanceProperties.set(VERSION, "1.2.3");
        BulkImportArguments arguments = BulkImportArguments.builder()
                .instanceProperties(instanceProperties)
                .bulkImportJob(new BulkImportJob.Builder()
                        .tableName("my-table")
                        .id("my-job")
                        .files(Lists.newArrayList("file1.parquet"))
                        .build())
                .jobRunId("test-run")
                .build();

        // When / Then
        assertThat(arguments.sparkSubmitCommandForEMRCluster("test-task", "s3a://jarsBucket/bulk-import-runner-1.2.3.jar"))
                .containsExactly("spark-submit",
                        "--deploy-mode",
                        "cluster",
                        "--class",
                        "sleeper.bulkimport.runner.dataframelocalsort.BulkImportDataframeLocalSortDriver",
                        "s3a://jarsBucket/bulk-import-runner-1.2.3.jar",
                        "configBucket",
                        "my-job",
                        "test-task",
                        "test-run",
                        "EMR");
    }
}
