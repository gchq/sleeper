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
package sleeper.environment.cdk.buildec2;

import sleeper.environment.cdk.util.AppContext;
import sleeper.environment.cdk.util.MyIpUtil;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.ec2.*;
import software.constructs.Construct;

import java.security.KeyPair;
import java.util.Collections;
import java.util.UUID;

public class BuildEC2Stack extends NestedStack {

    public BuildEC2Stack(Construct scope, IVpc vpc, AppContext app) {
        super(scope, "BuildEC2");
        BuildEC2Params params = BuildEC2Params.from(app);

        SecurityGroup allowSsh = SecurityGroup.Builder.create(this, "AllowSsh")
                .vpc(vpc)
                .description("Allow SSH inbound traffic")
                .allowAllOutbound(true)
                .build();
        allowSsh.addIngressRule(Peer.ipv4(MyIpUtil.findMyIp() + "/32"), Port.tcp(22));

        // Create a new SSH key every time the CDK is run
        KeyPair keyPair = KeyPairUtil.generate();
        KeyPairUtil.writePrivateToFile(keyPair, "BuildEC2.pem");
        CfnKeyPair key = CfnKeyPair.Builder.create(this, "KeyPair")
                .keyName("BuildEC2-" + UUID.randomUUID())
                .publicKeyMaterial(KeyPairUtil.publicBase64(keyPair))
                .build();

        Instance instance = Instance.Builder.create(this, "EC2")
                .vpc(vpc)
                .securityGroup(allowSsh)
                .machineImage(MachineImage.lookup(LookupMachineImageProps.builder()
                        .name("ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*")
                        .owners(Collections.singletonList("099720109477"))
                        .build()))
                .instanceType(InstanceType.of(InstanceClass.T3, InstanceSize.LARGE))
                .vpcSubnets(SubnetSelection.builder().subnetType(SubnetType.PUBLIC).build())
                .userData(UserData.custom(LoadUserDataUtil.userData(params)))
                .userDataCausesReplacement(true)
                .keyName(key.getKeyName())
                .blockDevices(Collections.singletonList(BlockDevice.builder()
                        .deviceName("/dev/sda1")
                        .volume(BlockDeviceVolume.ebs(200,
                                EbsDeviceOptions.builder().volumeType(EbsDeviceVolumeType.GP3).build()))
                        .build()))
                .build();
        instance.getInstance().addDependsOn(key);

        CfnOutput.Builder.create(this, "ConnectCommand")
                .value("ssh -i BuildEC2.pem ubuntu@" + instance.getInstancePublicIp())
                .description("Command to connect to EC2")
                .exportName("connectCommand")
                .build();
    }

}
