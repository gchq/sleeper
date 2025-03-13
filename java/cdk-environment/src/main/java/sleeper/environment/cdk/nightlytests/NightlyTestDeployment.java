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
package sleeper.environment.cdk.nightlytests;

import software.amazon.awscdk.services.events.IRule;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.environment.cdk.buildec2.BuildEC2Deployment;
import sleeper.environment.cdk.builduptime.BuildUptimeDeployment;
import sleeper.environment.cdk.config.AppContext;

import java.util.List;

import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_RUN_ENABLED;

public class NightlyTestDeployment {

    private final Construct scope;
    private final boolean enabled;
    private final IBucket testBucket;

    public NightlyTestDeployment(Construct scope) {
        this.scope = scope;
        AppContext context = AppContext.of(scope);
        enabled = context.get(NIGHTLY_TEST_RUN_ENABLED);
        if (enabled) {
            testBucket = new NightlyTestBucket(scope).getBucket();
        } else {
            testBucket = null;
        }
    }

    public String getTestBucketName() {
        if (enabled) {
            return testBucket.getBucketName();
        } else {
            return null;
        }
    }

    public List<IRule> automateUptimeGetAutoStopRules(BuildEC2Deployment buildEc2, BuildUptimeDeployment buildUptime) {
        if (enabled && buildEc2 != null) {
            testBucket.grantRead(buildUptime.getFunction());
            NightlyTestUptimeSchedules uptimeStack = new NightlyTestUptimeSchedules(scope,
                    buildUptime.getFunction(), buildEc2.getInstance(), testBucket.getBucketName());
            return List.of(uptimeStack.getStopAfterTestsRule());
        } else {
            return List.of();
        }
    }

}
