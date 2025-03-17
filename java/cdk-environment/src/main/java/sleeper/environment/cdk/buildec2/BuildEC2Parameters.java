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
package sleeper.environment.cdk.buildec2;

import software.amazon.awscdk.services.ec2.ISubnet;
import software.amazon.awscdk.services.ec2.IVpc;

import sleeper.environment.cdk.config.AppContext;
import sleeper.environment.cdk.config.AppParameters;
import sleeper.environment.cdk.config.StringParameter;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_BUCKET;
import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_DEPLOY_ID;
import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_RUN_ENABLED;
import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_RUN_HOUR_UTC;
import static sleeper.environment.cdk.config.AppParameters.NIGHTLY_TEST_SUBNETS;
import static sleeper.environment.cdk.config.AppParameters.VPC_ID;

public class BuildEC2Parameters {

    public static final StringParameter REPOSITORY = AppParameters.BUILD_REPOSITORY;
    public static final StringParameter FORK = AppParameters.BUILD_FORK;
    public static final StringParameter BRANCH = AppParameters.BUILD_BRANCH;

    private final String repository;
    private final String fork;
    private final String branch;
    private final BuildEC2Image image;
    private final boolean nightlyTestEnabled;
    private final String nightlyTestDeployId;
    private final String testHour;
    private final String testBucket;
    private final String vpc;
    private final String subnets;

    private BuildEC2Parameters(Builder builder) {
        AppContext context = Objects.requireNonNull(builder.context, "context must not be null");
        repository = context.get(REPOSITORY);
        fork = context.get(FORK);
        branch = context.get(BRANCH);
        image = BuildEC2Image.from(context);
        nightlyTestEnabled = context.get(NIGHTLY_TEST_RUN_ENABLED);
        if (nightlyTestEnabled) {
            nightlyTestDeployId = context.get(NIGHTLY_TEST_DEPLOY_ID)
                    .orElseThrow(() -> new IllegalArgumentException("nightlyTestDeployId must be set (up to 2 characters)"));
            if (nightlyTestDeployId.length() > 2) {
                throw new IllegalArgumentException("nightlyTestDeployId must be at most 2 characters long");
            }
            testHour = "" + context.get(NIGHTLY_TEST_RUN_HOUR_UTC);
            testBucket = context.get(NIGHTLY_TEST_BUCKET)
                    .orElseGet(() -> Objects.requireNonNull(builder.testBucket, "testBucket must not be null"));
            vpc = context.get(VPC_ID).orElseGet(() -> Objects.requireNonNull(builder.inheritVpc, "inheritVpc must not be null"));
            List<String> subnetsList = context.get(NIGHTLY_TEST_SUBNETS);
            if (subnetsList.isEmpty()) {
                subnetsList = Objects.requireNonNull(builder.inheritSubnets, "inheritSubnets must not be null");
            }
            subnets = String.join(",", subnetsList);
        } else {
            nightlyTestDeployId = null;
            testHour = null;
            testBucket = null;
            vpc = null;
            subnets = null;
        }
    }

    static BuildEC2Parameters from(AppContext context) {
        return builder().context(context).build();
    }

    static Builder builder() {
        return new Builder();
    }

    boolean isNightlyTestEnabled() {
        return nightlyTestEnabled;
    }

    String fillUserDataTemplate(String template) {
        String noNightlyTests = template
                .replace("${repository}", repository)
                .replace("${fork}", fork)
                .replace("${branch}", branch)
                .replace("${loginUser}", image.loginUser());
        if (!nightlyTestEnabled) {
            return noNightlyTests;
        }
        return noNightlyTests
                .replace("${deployId}", nightlyTestDeployId)
                .replace("${testHour}", testHour)
                .replace("${testBucket}", testBucket)
                .replace("${vpc}", vpc)
                .replace("${subnets}", subnets);
    }

    BuildEC2Image image() {
        return image;
    }

    public static class Builder {
        private AppContext context;
        private String testBucket;
        private String inheritVpc;
        private List<String> inheritSubnets;

        private Builder() {
        }

        public Builder context(AppContext context) {
            this.context = context;
            return this;
        }

        public Builder testBucket(String testBucket) {
            this.testBucket = testBucket;
            return this;
        }

        public Builder inheritVpc(String vpc, List<String> subnetIds) {
            inheritVpc = vpc;
            inheritSubnets = subnetIds;
            return this;
        }

        public Builder inheritVpc(IVpc inheritVpc) {
            return inheritVpc(inheritVpc.getVpcId(),
                    inheritVpc.getPrivateSubnets().stream()
                            .map(ISubnet::getSubnetId)
                            .collect(toUnmodifiableList()));
        }

        public BuildEC2Parameters build() {
            return new BuildEC2Parameters(this);
        }
    }

}
