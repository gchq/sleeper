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
package sleeper.environment.cdk.config;

public class AppParameters {

    public static StringParameter INSTANCE_ID = StringParameter.keyAndDefault("instanceId", "SleeperEnvironment");
    public static OptionalStringParameter VPC_ID = OptionalStringParameter.key("vpcId");

    public static StringParameter BUILD_REPOSITORY = StringParameter.keyAndDefault("repository", "sleeper");
    public static StringParameter BUILD_FORK = StringParameter.keyAndDefault("fork", "gchq");
    public static StringParameter BUILD_BRANCH = StringParameter.keyAndDefault("branch", "main");

    public static StringParameter BUILD_IMAGE_NAME = StringParameter.keyAndDefault("buildImageName", "ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*");
    public static StringParameter BUILD_IMAGE_OWNER = StringParameter.keyAndDefault("buildImageOwner", "099720109477");
    public static StringParameter BUILD_IMAGE_LOGIN_USER = StringParameter.keyAndDefault("buildImageLoginUser", "ubuntu");
    public static StringParameter BUILD_IMAGE_ROOT_DEVICE_NAME = StringParameter.keyAndDefault("buildImageRootDeviceName", "/dev/sda1");
    public static IntParameter BUILD_ROOT_VOLUME_SIZE_GIB = IntParameter.keyAndDefault("buildRootVolumeSizeGiB", 200);
}
