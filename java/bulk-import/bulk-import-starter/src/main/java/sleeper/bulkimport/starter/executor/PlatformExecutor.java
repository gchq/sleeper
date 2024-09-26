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

import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;

public interface PlatformExecutor {
    String PLATFORM_ENV_VARIABLE = "BULK_IMPORT_PLATFORM";

    void runJobOnPlatform(BulkImportArguments arguments);

    static PlatformExecutor fromEnvironment(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider) {
        String platform = System.getenv(PLATFORM_ENV_VARIABLE);
        switch (platform) {
            case "NonPersistentEMR":
                return new EmrPlatformExecutor(
                        EmrClient.create(),
                        instanceProperties, tablePropertiesProvider);
            case "EKS":
                return new StateMachinePlatformExecutor(
                        AWSStepFunctionsClientBuilder.defaultClient(),
                        instanceProperties);
            case "PersistentEMR":
                return new PersistentEmrPlatformExecutor(
                        EmrClient.create(),
                        instanceProperties);
            case "EMRServerless":
                return new EmrServerlessPlatformExecutor(EmrServerlessClient.create(), instanceProperties);
            default:
                throw new IllegalArgumentException("Invalid platform: " + platform);
        }
    }
}
