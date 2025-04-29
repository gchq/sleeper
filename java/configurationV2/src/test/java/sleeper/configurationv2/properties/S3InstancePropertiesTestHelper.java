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
package sleeper.configurationv2.properties;

import software.amazon.awssdk.services.s3.S3Client;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.testutils.InstancePropertiesTestHelper;

import java.util.function.Consumer;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * Helpers to create instance properties in S3.
 */
public class S3InstancePropertiesTestHelper {

    private S3InstancePropertiesTestHelper() {
    }

    /**
     * Creates properties for a Sleeper instance and saves them to S3. Generates a random instance ID and pre-populates
     * various properties set during deployment.
     *
     * @param  s3 the S3 client
     * @return    the instance properties
     */
    public static InstanceProperties createTestInstanceProperties(S3Client s3) {
        return S3InstancePropertiesTestHelper.createTestInstanceProperties(s3, properties -> {
        });
    }

    /**
     * Creates properties for a Sleeper instance and saves them to S3. Generates a random instance ID and pre-populates
     * various properties set during deployment.
     *
     * @param  s3              the S3 client
     * @param  extraProperties extra configuration to apply before saving to S3
     * @return                 the instance properties
     */
    public static InstanceProperties createTestInstanceProperties(
            S3Client s3, Consumer<InstanceProperties> extraProperties) {
        InstanceProperties instanceProperties = InstancePropertiesTestHelper.createTestInstanceProperties();
        extraProperties.accept(instanceProperties);
        try {
            s3.createBucket(builder -> builder.bucket(instanceProperties.get(CONFIG_BUCKET)));
            S3InstanceProperties.saveToS3(s3, instanceProperties);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to save instance properties", e);
        }
        return instanceProperties;
    }

}
