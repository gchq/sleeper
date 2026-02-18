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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.cdk.SleeperInstanceProps;
import sleeper.cdk.artefacts.SleeperArtefacts;
import sleeper.cdk.networking.SleeperNetworking;
import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.stack.SleeperOptionalStacks;
import sleeper.cdk.stack.core.AutoDeleteS3ObjectsStack;
import sleeper.cdk.stack.core.LoggingStack;
import sleeper.cdk.stack.core.PropertiesStack;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.systemtest.configuration.SystemTestProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACCOUNT;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_CLUSTER_ENABLED;

/**
 * Deploys Sleeper and additional stacks used for large-scale system tests.
 */
public class SystemTestApp extends Stack {
    private final SleeperInstanceProps props;
    private final SleeperArtefacts artefacts;
    private final SystemTestProperties instanceProperties;

    public SystemTestApp(App app, String id, StackProps stackProps, SleeperInstanceProps sleeperProps) {
        super(app, id, stackProps);
        this.props = setProps(sleeperProps);
        this.artefacts = sleeperProps.getArtefacts();
        this.instanceProperties = SystemTestProperties.from(sleeperProps.getInstanceProperties());
    }

    public void create() {
        SleeperNetworking networking = props.getNetworkingProvider().getNetworking(this);
        props.prepareProperties(this, networking);
        LoggingStack loggingStack = new LoggingStack(this, "Logging", instanceProperties);
        AutoDeleteS3ObjectsStack autoDeleteS3Stack = new AutoDeleteS3ObjectsStack(this, "AutoDeleteS3Objects", instanceProperties, artefacts, loggingStack);

        // System test bucket needs to be created first so that it's listed as an ingest source bucket before the ingest
        // stacks are created.
        SystemTestBucketStack bucketStack = new SystemTestBucketStack(this, "SystemTestIngestBucket", instanceProperties, autoDeleteS3Stack);

        // Sleeper instance
        SleeperCoreStacks coreStacks = SleeperCoreStacks.create(this, props, networking, loggingStack, autoDeleteS3Stack);
        SleeperOptionalStacks.create(this, props, coreStacks);

        coreStacks.createRoles();

        // Stack for writing random data
        if (instanceProperties.getBoolean(SYSTEM_TEST_CLUSTER_ENABLED)) {
            new SystemTestClusterStack(this, "SystemTest", instanceProperties, networking, bucketStack, coreStacks.getEcsClusterTasksStack());
        }

        // Delay writing properties to include data generation cluster
        new PropertiesStack(this, "Properties", instanceProperties, artefacts, coreStacks);
    }

    public static void main(String[] args) {
        App app = new App(AppProps.builder()
                .analyticsReporting(false)
                .build());

        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create()) {
            SleeperInstanceProps props = SleeperInstanceProps.fromContext(app, s3Client, dynamoClient);
            InstanceProperties instanceProperties = props.getInstanceProperties();

            String id = instanceProperties.get(ID);
            Environment environment = Environment.builder()
                    .account(System.getenv("CDK_DEFAULT_ACCOUNT"))
                    .region(System.getenv("CDK_DEFAULT_REGION"))
                    .build();

            new SystemTestApp(app, id, StackProps.builder()
                    .stackName(id)
                    .env(environment)
                    .build(),
                    props).create();

            app.synth();
        }
    }

    private SleeperInstanceProps setProps(SleeperInstanceProps props) {
        props.getInstanceProperties().set(ACCOUNT, getAccount());
        props.getInstanceProperties().set(REGION, getRegion());
        return props;
    }
}
