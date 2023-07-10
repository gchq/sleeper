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
package sleeper.configuration.properties;

import com.amazonaws.services.s3.AmazonS3;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;

import static sleeper.configuration.properties.InstanceProperties.getConfigBucketFromInstanceId;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.commonProperties.ACCOUNT;
import static sleeper.configuration.properties.commonProperties.ID;
import static sleeper.configuration.properties.commonProperties.JARS_BUCKET;
import static sleeper.configuration.properties.commonProperties.REGION;
import static sleeper.configuration.properties.commonProperties.SUBNETS;
import static sleeper.configuration.properties.commonProperties.VPC_ID;

public class InstancePropertiesTestHelper {

    private InstancePropertiesTestHelper() {
    }

    public static InstanceProperties createTestInstanceProperties(AmazonS3 s3) {
        return createTestInstanceProperties(s3, properties -> {
        });
    }

    public static InstanceProperties createTestInstanceProperties(
            AmazonS3 s3, Consumer<InstanceProperties> extraProperties) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        extraProperties.accept(instanceProperties);
        try {
            s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
            instanceProperties.saveToS3(s3);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to save instance properties", e);
        }
        return instanceProperties;
    }

    public static InstanceProperties createTestInstanceProperties() {
        String id = UUID.randomUUID().toString();
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, id);
        instanceProperties.set(CONFIG_BUCKET, getConfigBucketFromInstanceId(id));
        instanceProperties.set(JARS_BUCKET, "");
        instanceProperties.set(ACCOUNT, "");
        instanceProperties.set(REGION, "");
        instanceProperties.set(VERSION, "");
        instanceProperties.set(VPC_ID, "");
        instanceProperties.set(SUBNETS, "");
        return instanceProperties;
    }

    public static String propertiesString(Properties properties) throws IOException {
        StringWriter writer = new StringWriter();
        properties.store(writer, "");
        return writer.toString();
    }
}
