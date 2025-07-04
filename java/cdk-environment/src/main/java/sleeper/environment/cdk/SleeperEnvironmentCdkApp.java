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
package sleeper.environment.cdk;

import software.amazon.awscdk.App;
import software.amazon.awscdk.AppProps;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.events.IRule;

import sleeper.environment.cdk.buildec2.BuildEC2Deployment;
import sleeper.environment.cdk.builduptime.AutoShutdownSchedule;
import sleeper.environment.cdk.builduptime.BuildUptimeDeployment;
import sleeper.environment.cdk.config.AppContext;
import sleeper.environment.cdk.networking.NetworkingDeployment;
import sleeper.environment.cdk.nightlytests.NightlyTestDeployment;

import java.util.List;

import static sleeper.environment.cdk.config.AppParameters.BUILD_UPTIME_LAMBDA_JAR;
import static sleeper.environment.cdk.config.AppParameters.DEPLOY_EC2;
import static sleeper.environment.cdk.config.AppParameters.INSTANCE_ID;

/**
 * Deploys an environment suitable for Sleeper, including a VPC and an EC2 instance to run the deployment from.
 */
public class SleeperEnvironmentCdkApp {

    private SleeperEnvironmentCdkApp() {
    }

    public static void main(String[] args) {
        App app = new App(AppProps.builder()
                .analyticsReporting(false)
                .build());
        Environment environment = Environment.builder()
                .account(System.getenv("CDK_DEFAULT_ACCOUNT"))
                .region(System.getenv("CDK_DEFAULT_REGION"))
                .build();
        AppContext context = AppContext.of(app);
        String environmentId = context.get(INSTANCE_ID);
        String stackName = getStackNameForEnvironment(environmentId);
        Stack stack = new Stack(app, stackName, StackProps.builder()
                .stackName(stackName)
                .env(environment)
                .build());
        NightlyTestDeployment nightlyTests = new NightlyTestDeployment(stack);
        NetworkingDeployment networking = new NetworkingDeployment(stack);
        BuildEC2Deployment buildEc2 = null;
        if (context.get(DEPLOY_EC2)) {
            buildEc2 = new BuildEC2Deployment(stack, networking.getVpc(), nightlyTests);
        }
        if (context.get(BUILD_UPTIME_LAMBDA_JAR).isPresent()) {
            BuildUptimeDeployment buildUptime = new BuildUptimeDeployment(stack);
            List<IRule> autoStopRules = nightlyTests.automateUptimeGetAutoStopRules(buildEc2, buildUptime);
            AutoShutdownSchedule.create(stack, buildUptime, buildEc2, autoStopRules);
        }
        app.synth();
    }

    public static List<String> getStackNamesForEnvironment(String environmentId) {
        return List.of(getStackNameForEnvironment(environmentId));
    }

    private static String getStackNameForEnvironment(String environmentId) {
        return environmentId + "-SleeperEnvironment";
    }
}
