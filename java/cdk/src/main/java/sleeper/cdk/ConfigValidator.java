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
package sleeper.cdk;

import com.amazonaws.services.s3.internal.BucketNameUtils;

import sleeper.configuration.properties.InstanceProperties;

import java.nio.file.Path;

import static sleeper.configuration.properties.CommonProperty.ID;
import static sleeper.configuration.properties.local.LoadLocalProperties.loadTablesFromInstancePropertiesFile;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

class ConfigValidator {
    void validate(InstanceProperties instanceProperties, Path instancePropertyPath) {
        checkForValidInstanceId(instanceProperties);
        checkTableConfiguration(instanceProperties, instancePropertyPath);
    }

    private void checkForValidInstanceId(InstanceProperties instanceProperties) {
        if (!BucketNameUtils.isValidV2BucketName(instanceProperties.get(ID))) {
            throw new IllegalArgumentException("Sleeper instance id is illegal: " + instanceProperties.get(ID));
        }
    }

    private void checkTableConfiguration(InstanceProperties instanceProperties, Path instancePropertyPath) {
        String instanceName = instanceProperties.get(ID);

        loadTablesFromInstancePropertiesFile(instanceProperties, instancePropertyPath).forEach(tableProperties -> {
            String tableName = tableProperties.get(TABLE_NAME);

            checkBucketConfigurationForTable(instanceName, tableName);
        });
    }

    private void checkBucketConfigurationForTable(String instanceName, String tableName) {
        String bucketName = String.join("-", "sleeper", instanceName, "table", tableName);
        if (!BucketNameUtils.isValidV2BucketName(bucketName)) {
            throw new IllegalArgumentException("Sleeper table bucket name is illegal: " + bucketName);
        }
    }
}
