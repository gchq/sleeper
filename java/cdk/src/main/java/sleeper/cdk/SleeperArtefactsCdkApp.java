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

import sleeper.cdk.stack.SleeperArtefactsStack;
import sleeper.cdk.util.CdkContext;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.LoadLocalProperties;

import java.nio.file.Path;

import static sleeper.core.properties.instance.CommonProperty.ID;

/**
 * A CDK app to deploy AWS resources that will hold artefacts used to deploy Sleeper.
 *
 * @see SleeperArtefactsStack
 */
public class SleeperArtefactsCdkApp {

    private SleeperArtefactsCdkApp() {
    }

    public static void main(String[] args) {
        App app = new App(AppProps.builder()
                .analyticsReporting(false)
                .build());
        Environment environment = Environment.builder()
                .account(System.getenv("CDK_DEFAULT_ACCOUNT"))
                .region(System.getenv("CDK_DEFAULT_REGION"))
                .build();
        String deploymentId = getDeploymentId(CdkContext.from(app));
        new SleeperArtefactsStack(app, "SleeperArtefacts", deploymentId, StackProps.builder()
                .stackName(String.join("-", "sleeper", deploymentId, "artefacts"))
                .env(environment)
                .build());
        app.synth();
    }

    public static String getDeploymentId(CdkContext context) {
        String deploymentId = context.tryGetContext("id");
        if (deploymentId != null) {
            return deploymentId;
        }
        String propertiesFile = context.tryGetContext("propertiesfile");
        if (propertiesFile == null) {
            throw new IllegalArgumentException("ID for artefacts deployment not found in context. Please set \"id\" or \"propertiesfile\".");
        }
        InstanceProperties instanceProperties = LoadLocalProperties.loadInstancePropertiesNoValidation(Path.of(propertiesFile));
        return instanceProperties.get(ID);
    }

}
