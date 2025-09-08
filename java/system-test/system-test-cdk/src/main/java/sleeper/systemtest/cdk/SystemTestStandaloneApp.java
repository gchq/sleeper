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

package sleeper.systemtest.cdk;

import software.amazon.awscdk.App;
import software.amazon.awscdk.AppProps;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.stack.core.AutoDeleteS3ObjectsStack;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.nio.file.Path;

import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_ACCOUNT;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_CLUSTER_ENABLED;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_ID;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REGION;

public class SystemTestStandaloneApp extends Stack {

    public SystemTestStandaloneApp(
            App app, String id, StackProps props, SystemTestStandaloneProperties properties, BuiltJars jars) {
        super(app, id, props);

        AutoDeleteS3ObjectsStack autoDeleteS3ObjectsStack = new AutoDeleteS3ObjectsStack(this, "AutoDeleteS3Objects", properties.toInstancePropertiesForCdkUtils(), jars);

        SystemTestBucketStack bucketStack = new SystemTestBucketStack(this, "SystemTestBucket", properties, jars, autoDeleteS3ObjectsStack);
        if (properties.getBoolean(SYSTEM_TEST_CLUSTER_ENABLED)) {
            new SystemTestClusterStack(this, "SystemTestCluster", properties, bucketStack);
        }
        new SystemTestPropertiesStack(this, "SystemTestProperties", properties, bucketStack, jars);
    }

    public static void main(String[] args) {
        App app = new App(AppProps.builder()
                .analyticsReporting(false)
                .build());

        Path propertiesFile = Path.of((String) app.getNode().tryGetContext("propertiesfile"));
        SystemTestStandaloneProperties systemTestProperties = SystemTestStandaloneProperties.fromFile(propertiesFile);
        systemTestProperties.getPropertiesIndex().getCdkDefined().forEach(systemTestProperties::unset);

        try (S3Client s3Client = S3Client.create()) {
            BuiltJars jars = BuiltJars.from(s3Client, systemTestProperties.toInstancePropertiesForCdkUtils());

            String id = systemTestProperties.get(SYSTEM_TEST_ID);
            Environment environment = Environment.builder()
                    .account(systemTestProperties.get(SYSTEM_TEST_ACCOUNT))
                    .region(systemTestProperties.get(SYSTEM_TEST_REGION))
                    .build();
            new SystemTestStandaloneApp(app, id,
                    StackProps.builder().stackName(id).env(environment).build(),
                    systemTestProperties, jars);
            app.synth();
        }
    }
}
