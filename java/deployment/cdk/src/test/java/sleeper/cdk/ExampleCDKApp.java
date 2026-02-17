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
import software.amazon.awscdk.Tags;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CommonProperty.ID;

/**
 * Deploys an example of Sleeper CDK, presently for defining own tables.
 */
public class ExampleCDKApp {

    private ExampleCDKApp() {
    }

    public static void main(String[] args) {
        createCDKApp();
    }

    public static App createCDKApp() {
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
            SleeperTable.createAsRootStack(app, id,
                    StackProps.builder()
                            .stackName(id)
                            .env(environment)
                            .build(),
                    props);
            instanceProperties.getTags()
                    .forEach((key, value) -> Tags.of(app).add(key, value));

            app.synth();
        }
        return app;
    }

}
