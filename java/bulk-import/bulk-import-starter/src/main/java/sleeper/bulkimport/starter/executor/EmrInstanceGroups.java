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

import com.amazonaws.services.elasticmapreduce.model.ComputeLimits;
import com.amazonaws.services.elasticmapreduce.model.ComputeLimitsUnitType;
import com.amazonaws.services.elasticmapreduce.model.EbsConfiguration;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.MarketType;

import sleeper.configuration.properties.InstanceProperties;

import java.util.List;
import java.util.Random;
import java.util.function.IntUnaryOperator;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNETS;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS;

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

        return new JobFlowInstancesConfig()
                .withEc2SubnetId(randomSubnet())
                .withInstanceGroups(
                        new InstanceGroupConfig()
                                .withName("Executors")
                                .withInstanceType(platformSpec.getList(BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE).get(0))
                                .withInstanceRole(InstanceRoleType.CORE)
                                .withInstanceCount(platformSpec.getInt(BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS))
                                .withEbsConfiguration(ebsConfiguration)
                                .withMarket(MarketType.fromValue(platformSpec.getOrDefault(
                                        BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE, "SPOT"))),
                        new InstanceGroupConfig()
                                .withName("Driver")
                                .withInstanceType(platformSpec.getList(BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE).get(0))
                                .withInstanceRole(InstanceRoleType.MASTER)
                                .withInstanceCount(1)
                                .withEbsConfiguration(ebsConfiguration));
    }

    @Override
    public ComputeLimits createComputeLimits(BulkImportPlatformSpec platformSpec) {

        Integer maxNumberOfExecutors = Integer.max(
                platformSpec.getInt(BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS),
                platformSpec.getInt(BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS));
        return new ComputeLimits()
                .withUnitType(ComputeLimitsUnitType.Instances)
                .withMinimumCapacityUnits(1)
                .withMaximumCapacityUnits(maxNumberOfExecutors);
    }

    private String randomSubnet() {
        List<String> subnets = instanceProperties.getList(SUBNETS);
        return subnets.get(randomSubnet.applyAsInt(subnets.size()));
    }
}
