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
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;

import sleeper.cdk.artefacts.SleeperArtefactRepositories;
import sleeper.cdk.util.CdkContext;

/**
 * A CDK app to deploy AWS resources that will hold artefacts used to deploy Sleeper.
 *
 * @see SleeperArtefactRepositories
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
        CdkContext context = CdkContext.from(app);
        String deploymentId = context.tryGetContext("id");
        Stack stack = new Stack(app, "SleeperArtefacts", StackProps.builder()
                .stackName(deploymentId + "-artefacts")
                .env(environment)
                .build());
        SleeperArtefactRepositories.Builder.create(stack, deploymentId)
                .extraEcrImages(context.getList("extraEcrImages"))
                .build();
        app.synth();
    }

}
