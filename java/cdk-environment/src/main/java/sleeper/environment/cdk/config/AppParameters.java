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
package sleeper.environment.cdk.config;

public class AppParameters {

    private AppParameters() {
    }

    public static final RequiredStringParameter INSTANCE_ID = RequiredStringParameter.key("instanceId");
    public static final OptionalStringParameter VPC_ID = OptionalStringParameter.key("vpcId");

    public static final StringParameter BUILD_REPOSITORY = StringParameter.keyAndDefault("repository", "sleeper");
    public static final StringParameter BUILD_FORK = StringParameter.keyAndDefault("fork", "gchq");
    public static final StringParameter BUILD_BRANCH = StringParameter.keyAndDefault("branch", "develop");

    public static final StringParameter BUILD_IMAGE_NAME = StringParameter.keyAndDefault("buildImageName", "ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*");
    public static final StringParameter BUILD_IMAGE_OWNER = StringParameter.keyAndDefault("buildImageOwner", "099720109477");
    public static final StringParameter BUILD_IMAGE_LOGIN_USER = StringParameter.keyAndDefault("buildImageLoginUser", "ubuntu");
    public static final StringParameter BUILD_IMAGE_ROOT_DEVICE_NAME = StringParameter.keyAndDefault("buildImageRootDeviceName", "/dev/sda1");
    public static final IntParameter BUILD_ROOT_VOLUME_SIZE_GIB = IntParameter.keyAndDefault("buildRootVolumeSizeGiB", 200);
}
