/*
 * Copyright 2022-2023 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;

public interface PlatformExecutor {
    String PLATFORM_ENV_VARIABLE = "BULK_IMPORT_PLATFORM";

    String getJarLocation();

    void runJobOnPlatform(BulkImportExecutor bulkImportExecutor, BulkImportJob bulkImportJob, String jobRunId);

    static PlatformExecutor fromEnvironment(InstanceProperties instanceProperties,
                                            TablePropertiesProvider tablePropertiesProvider) {
        String platform = System.getenv(PLATFORM_ENV_VARIABLE);
        switch (platform) {
            case "NonPersistentEMR":
                return new EmrPlatformExecutor(
                        AmazonElasticMapReduceClientBuilder.defaultClient(),
                        instanceProperties, tablePropertiesProvider);
            case "EKS":
                return new StateMachinePlatformExecutor(
                        AWSStepFunctionsClientBuilder.defaultClient(),
                        instanceProperties, tablePropertiesProvider);
            case "PersistentEMR":
                return new PersistentEmrPlatformExecutor(
                        AmazonElasticMapReduceClientBuilder.defaultClient(),
                        instanceProperties);
            default:
                throw new IllegalArgumentException("Invalid platform: " + platform);
        }
    }
}
