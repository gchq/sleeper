/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.services.elasticmapreduce.model.EbsBlockDeviceConfig;
import com.amazonaws.services.elasticmapreduce.model.EbsConfiguration;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.MarketType;
import com.amazonaws.services.elasticmapreduce.model.VolumeSpecification;
import com.google.common.collect.Lists;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperty;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.IntUnaryOperator;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_EBS_VOLUME_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_EC2_KEYPAIR_NAME;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP;

public class EmrInstanceGroups implements EmrInstanceConfiguration {

    private final InstanceProperties instanceProperties;
    private final IntUnaryOperator randomSubnet;

    public EmrInstanceGroups(InstanceProperties instanceProperties) {
        this(instanceProperties, new Random()::nextInt);
    }

    public EmrInstanceGroups(InstanceProperties instanceProperties,
                             IntUnaryOperator randomSubnet) {
        this.instanceProperties = instanceProperties;
        this.randomSubnet = randomSubnet;
    }

    public JobFlowInstancesConfig createJobFlowInstancesConfig(BulkImportPlatformSpec platformSpec) {
        JobFlowInstancesConfig config = new JobFlowInstancesConfig()
                .withEc2SubnetId(randomSubnet());

        String driverInstanceType = platformSpec.get(TableProperty.BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE);
        String executorInstanceType = platformSpec.get(TableProperty.BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE);
        Integer initialNumberOfExecutors = platformSpec.getInt(TableProperty.BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS);

        String marketTypeOfExecutors = platformSpec.get(TableProperty.BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE);
        if (marketTypeOfExecutors == null) {
            marketTypeOfExecutors = "SPOT";
        }

        VolumeSpecification volumeSpecification = new VolumeSpecification()
//                .withIops(null) // TODO Add property to control this
                .withSizeInGB(instanceProperties.getInt(BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB))
                .withVolumeType(instanceProperties.get(BULK_IMPORT_EMR_EBS_VOLUME_TYPE));
        EbsBlockDeviceConfig ebsBlockDeviceConfig = new EbsBlockDeviceConfig()
                .withVolumeSpecification(volumeSpecification)
                .withVolumesPerInstance(instanceProperties.getInt(BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE));
        EbsConfiguration ebsConfiguration = new EbsConfiguration()
                .withEbsBlockDeviceConfigs(ebsBlockDeviceConfig)
                .withEbsOptimized(true);

        config.setInstanceGroups(Lists.newArrayList(
                new InstanceGroupConfig()
                        .withName("Executors")
                        .withInstanceType(executorInstanceType)
                        .withInstanceRole(InstanceRoleType.CORE)
                        .withInstanceCount(initialNumberOfExecutors)
                        .withEbsConfiguration(ebsConfiguration)
                        .withMarket(MarketType.fromValue(marketTypeOfExecutors)),
                new InstanceGroupConfig()
                        .withName("Driver")
                        .withInstanceType(driverInstanceType)
                        .withInstanceRole(InstanceRoleType.MASTER)
                        .withInstanceCount(1)
                        .withEbsConfiguration(ebsConfiguration)
        ));

        String ec2KeyName = instanceProperties.get(BULK_IMPORT_EMR_EC2_KEYPAIR_NAME);
        if (null != ec2KeyName && !ec2KeyName.isEmpty()) {
            config.setEc2KeyName(ec2KeyName);
        }
        String additionalSecurityGroup = instanceProperties.get(BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP);
        if (null != additionalSecurityGroup && !additionalSecurityGroup.isEmpty()) {
            config.setAdditionalMasterSecurityGroups(Collections.singletonList(additionalSecurityGroup));
        }

        return config;
    }

    private String randomSubnet() {
        List<String> subnets = instanceProperties.getList(UserDefinedInstanceProperty.SUBNETS);
        return subnets.get(randomSubnet.applyAsInt(subnets.size()));
    }
}
