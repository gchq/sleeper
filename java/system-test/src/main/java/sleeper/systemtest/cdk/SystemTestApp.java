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
package sleeper.systemtest.cdk;

import software.amazon.awscdk.App;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.Tags;

import sleeper.cdk.SleeperCdkApp;
import sleeper.cdk.Utils;
import sleeper.cdk.stack.IngestStack;
import sleeper.cdk.stack.bulkimport.EmrBulkImportStack;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.systemtest.SystemTestProperties;

import java.io.IOException;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;

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
    protected SystemTestProperties getInstanceProperties() throws RuntimeException {
        InstanceProperties properties = super.getInstanceProperties();
        if (properties instanceof SystemTestProperties) {
            return (SystemTestProperties) properties;
        }
        throw new RuntimeException("Error when retrieving instance properties");
    }

    public static void main(String[] args) throws IOException {
        App app = new App();

        SystemTestProperties systemTestProperties = Utils.loadInstanceProperties(new SystemTestProperties(), app);

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
