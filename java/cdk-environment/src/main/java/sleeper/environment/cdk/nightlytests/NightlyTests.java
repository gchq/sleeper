/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.environment.cdk.nightlytests;

import software.amazon.awscdk.App;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.events.IRule;
import software.amazon.awscdk.services.s3.IBucket;

import sleeper.environment.cdk.buildec2.BuildEC2Stack;
import sleeper.environment.cdk.builduptime.BuildUptimeStack;
import sleeper.environment.cdk.config.AppContext;

import java.util.List;

import static sleeper.environment.cdk.config.AppParameters.INSTANCE_ID;
import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_BUCKET;
import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_RUN_ENABLED;

public class NightlyTests {

    private final App app;
    private final Environment environment;
    private final boolean enabled;
    private final String instanceId;
    private final String contextBucketName;
    private final IBucket testBucket;

    public NightlyTests(App app, Environment environment) {
        this.app = app;
        this.environment = environment;
        AppContext context = AppContext.of(app);
        enabled = context.get(NIGHTLY_TEST_RUN_ENABLED);
        instanceId = context.get(INSTANCE_ID);
        contextBucketName = context.get(NIGHTLY_TEST_BUCKET).orElse(null);
        if (enabled && contextBucketName == null) {
            testBucket = new NightlyTestBucketStack(app,
                    StackProps.builder().stackName(instanceId + "-NightlyTestBucket").env(environment).build())
                    .getBucket();
        } else {
            testBucket = null;
        }
    }

    public String getTestBucketName() {
        if (testBucket != null) {
            return testBucket.getBucketName();
        } else {
            return contextBucketName;
        }
    }

    public List<IRule> automateUptimeGetAutoStopRules(BuildEC2Stack buildEc2, BuildUptimeStack buildUptime) {
        if (enabled) {
            NightlyTestUptimeStack uptimeStack = new NightlyTestUptimeStack(app,
                    StackProps.builder().stackName(instanceId + "-NightlyTests").env(environment).build(),
                    buildUptime.getFunction(), buildEc2.getInstance(), getTestBucketName());
            return List.of(uptimeStack.getStopAfterTestsRule());
        } else {
            return List.of();
        }
    }

}
