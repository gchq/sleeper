/*
 * Copyright 2022 Crown Copyright
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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.io.File;
import java.io.FileNotFoundException;
import sleeper.cdk.ConfigValidator;
import sleeper.cdk.SleeperCdkApp;
import sleeper.cdk.stack.IngestStack;
import sleeper.cdk.stack.bulkimport.EmrBulkImportStack;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import sleeper.systemtest.SystemTestProperties;
import software.amazon.awscdk.App;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.Tags;

/**
 * An {@link App} to deploy the {@link SleeperCdkApp} and the additional stacks
 * needed for the system tests.
 */
public class SystemTestApp extends SleeperCdkApp {
    private boolean readyToGenerateProperties = false;

    public SystemTestApp(App app, String id, SystemTestProperties sleeperProperties, StackProps props) {
        super(app, id, sleeperProperties, props);
    }

    public void create() {
        super.create();
        SystemTestProperties systemTestProperties = getInstanceProperties();

        // Stack for writing random data
        IngestStack ingestStack = getIngestStack();
        EmrBulkImportStack emrBulkImportStack = getEmrBulkImportStack();
        SystemTestStack systemTestStack = new SystemTestStack(this,
                "SystemTest",
                getTableStack().getDataBuckets(),
                getTableStack().getStateStoreStacks(),
                systemTestProperties,
                ingestStack == null ? null : ingestStack.getIngestJobQueue(),
                emrBulkImportStack == null ? null : emrBulkImportStack.getEmrBulkImportJobQueue());
        systemTestProperties.getTags()
                .forEach((key, value) -> Tags.of(systemTestStack).add(key, value));

        readyToGenerateProperties = true;
        generateProperties();
    }

    @Override
    protected void generateProperties() {
        if (readyToGenerateProperties) {
            super.generateProperties();
        }
    }

    @Override
    protected SystemTestProperties getInstanceProperties() {
        return (SystemTestProperties) super.getInstanceProperties();
    }

    public static void main(String[] args) throws FileNotFoundException {
        App app = new App();

        String systemTestPropertiesFile = (String) app.getNode().tryGetContext("testpropertiesfile");
        String validate = (String) app.getNode().tryGetContext("validate");
        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.load(new File(systemTestPropertiesFile));
        if ("true".equalsIgnoreCase(validate)) {
            new ConfigValidator(AmazonS3ClientBuilder.defaultClient(),
                    AmazonDynamoDBClientBuilder.defaultClient()).validate(systemTestProperties);
        }

        String id = systemTestProperties.get(ID);
        Environment environment = Environment.builder()
                .account(systemTestProperties.get(ACCOUNT))
                .region(systemTestProperties.get(REGION))
                .build();
        
        new SystemTestApp(app, id, systemTestProperties, StackProps.builder()
            .stackName(id)
            .env(environment)
            .build()).create();
        app.synth();
    }
}
