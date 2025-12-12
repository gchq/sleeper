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

import software.amazon.awscdk.services.ec2.BlockDevice;
import software.amazon.awscdk.services.ec2.BlockDeviceVolume;
import software.amazon.awscdk.services.ec2.EbsDeviceOptions;
import software.amazon.awscdk.services.ec2.EbsDeviceVolumeType;
import software.amazon.awscdk.services.ec2.IMachineImage;
import software.amazon.awscdk.services.ec2.LookupMachineImageProps;
import software.amazon.awscdk.services.ec2.MachineImage;

import sleeper.environment.cdk.config.AppContext;
import sleeper.environment.cdk.config.AppParameters;
import sleeper.environment.cdk.config.IntParameter;
import sleeper.environment.cdk.config.StringParameter;

import java.util.Collections;

public class BuildEC2Image {

    public static final StringParameter NAME = AppParameters.BUILD_IMAGE_NAME;
    public static final StringParameter OWNER = AppParameters.BUILD_IMAGE_OWNER;
    public static final StringParameter LOGIN_USER = AppParameters.BUILD_IMAGE_LOGIN_USER;
    public static final StringParameter ROOT_DEVICE_NAME = AppParameters.BUILD_IMAGE_ROOT_DEVICE_NAME;
    public static final IntParameter ROOT_VOLUME_SIZE_GIB = AppParameters.BUILD_ROOT_VOLUME_SIZE_GIB;

    private final String name;
    private final String owner;
    private final String loginUser;
    private final String rootDeviceName;
    private final int rootVolumeSizeGiB;

    private BuildEC2Image(AppContext context) {
        name = context.get(NAME);
        owner = context.get(OWNER);
        loginUser = context.get(LOGIN_USER);
        rootDeviceName = context.get(ROOT_DEVICE_NAME);
        rootVolumeSizeGiB = context.get(ROOT_VOLUME_SIZE_GIB);
    }

    public static BuildEC2Image from(AppContext context) {
        return new BuildEC2Image(context);
    }

    IMachineImage machineImage() {
        return MachineImage.lookup(LookupMachineImageProps.builder()
                .name(name)
                .owners(Collections.singletonList(owner))
                .build());
    }

    BlockDevice rootBlockDevice() {
        return BlockDevice.builder()
                .deviceName(rootDeviceName)
                .volume(BlockDeviceVolume.ebs(rootVolumeSizeGiB,
                        EbsDeviceOptions.builder().volumeType(EbsDeviceVolumeType.GP3).encrypted(true).build()))
                .build();
    }

    String loginUser() {
        return loginUser;
    }

}
