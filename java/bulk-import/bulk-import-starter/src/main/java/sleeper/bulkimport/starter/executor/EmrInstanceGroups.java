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

import com.amazonaws.services.elasticmapreduce.model.EbsConfiguration;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.MarketType;
import com.google.common.collect.Lists;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperty;

import java.util.List;
import java.util.Random;
import java.util.function.IntUnaryOperator;

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

    public JobFlowInstancesConfig createJobFlowInstancesConfig(
            EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {

        JobFlowInstancesConfig config = new JobFlowInstancesConfig()
                .withEc2SubnetId(randomSubnet());

        String driverInstanceType = platformSpec.get(TableProperty.BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE);
        String executorInstanceType = platformSpec.get(TableProperty.BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE);
        Integer initialNumberOfExecutors = platformSpec.getInt(TableProperty.BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS);

        String marketTypeOfExecutors = platformSpec.get(TableProperty.BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE);
        if (marketTypeOfExecutors == null) {
            marketTypeOfExecutors = "SPOT";
        }

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

        return config;
    }

    private String randomSubnet() {
        List<String> subnets = instanceProperties.getList(UserDefinedInstanceProperty.SUBNETS);
        return subnets.get(randomSubnet.applyAsInt(subnets.size()));
    }
}
