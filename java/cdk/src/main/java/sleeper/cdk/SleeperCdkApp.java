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
package sleeper.cdk;

import software.amazon.awscdk.App;
import software.amazon.awscdk.AppProps;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.StackProps;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.cdk.jars.SleeperJarsInBucket;
import sleeper.cdk.stack.SleeperInstanceStack;
import sleeper.cdk.util.CdkContext;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.REGION;

/**
 * Deploys an instance of Sleeper, including any configured optional stacks.
 */
public class SleeperCdkApp {

    private SleeperCdkApp() {
    }

    public static void main(String[] args) {
        App app = new App(AppProps.builder()
                .analyticsReporting(false)
                .build());

        DeployInstanceConfiguration configuration = Utils.loadDeployInstanceConfiguration(CdkContext.from(app));
        InstanceProperties instanceProperties = configuration.getInstanceProperties();

        String id = instanceProperties.get(ID);
        Environment environment = Environment.builder()
                .account(instanceProperties.get(ACCOUNT))
                .region(instanceProperties.get(REGION))
                .build();
        try (S3Client s3Client = S3Client.create()) {
            SleeperJarsInBucket jars = SleeperJarsInBucket.from(s3Client, instanceProperties);

            new SleeperInstanceStack(app, id, StackProps.builder()
                    .stackName(id)
                    .env(environment)
                    .build(),
                    configuration, jars).create();

            app.synth();
        }
    }
}
