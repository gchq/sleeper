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
package sleeper.environment.cdk.config;

public class AppParameters {

    private AppParameters() {
    }

    public static final RequiredStringParameter INSTANCE_ID = RequiredStringParameter.key("instanceId");
    public static final OptionalStringParameter VPC_ID = OptionalStringParameter.key("vpcId");
    public static final BooleanParameter DEPLOY_EC2 = BooleanParameter.keyAndDefault("deployEc2", true);

    public static final StringParameter BUILD_REPOSITORY = StringParameter.keyAndDefault("repository", "sleeper");
    public static final StringParameter BUILD_FORK = StringParameter.keyAndDefault("fork", "gchq");
    public static final StringParameter BUILD_BRANCH = StringParameter.keyAndDefault("branch", "develop");

    public static final StringParameter BUILD_IMAGE_NAME = StringParameter.keyAndDefault("buildImageName", "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*");
    public static final StringParameter BUILD_IMAGE_OWNER = StringParameter.keyAndDefault("buildImageOwner", "099720109477");
    public static final StringParameter BUILD_IMAGE_LOGIN_USER = StringParameter.keyAndDefault("buildImageLoginUser", "ubuntu");
    public static final StringParameter BUILD_IMAGE_ROOT_DEVICE_NAME = StringParameter.keyAndDefault("buildImageRootDeviceName", "/dev/sda1");
    public static final IntParameter BUILD_ROOT_VOLUME_SIZE_GIB = IntParameter.keyAndDefault("buildRootVolumeSizeGiB", 350);

    public static final OptionalStringParameter LOG_RETENTION_DAYS = OptionalStringParameter.key("logRetentionDays");
    public static final OptionalStringParameter BUILD_UPTIME_LAMBDA_JAR = OptionalStringParameter.key("buildUptimeLambdaJar");
    public static final StringListParameter AUTO_SHUTDOWN_EXISTING_EC2_IDS = StringListParameter.key("autoShutdownExistingEc2Ids");
    public static final IntParameter AUTO_SHUTDOWN_HOUR_UTC = IntParameter.keyAndDefault("autoShutdownHourUtc", 19);
    public static final BooleanParameter NIGHTLY_TEST_RUN_ENABLED = BooleanParameter.keyAndDefault("nightlyTestsEnabled", false);
    public static final OptionalStringParameter NIGHTLY_TEST_DEPLOY_ID = OptionalStringParameter.key("nightlyTestDeployId");
    public static final IntParameter NIGHTLY_TEST_RUN_HOUR_UTC = IntParameter.keyAndDefault("nightlyTestHourUtc", 3);
    public static final OptionalStringParameter NIGHTLY_TEST_BUCKET = OptionalStringParameter.key("nightlyTestBucket");
    public static final StringListParameter NIGHTLY_TEST_SUBNETS = StringListParameter.key("subnetIds");
}
