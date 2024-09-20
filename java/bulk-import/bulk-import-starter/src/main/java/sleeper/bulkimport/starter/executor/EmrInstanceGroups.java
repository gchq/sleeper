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
package sleeper.bulkimport.starter.executor;

import software.amazon.awssdk.services.emr.model.ComputeLimits;
import software.amazon.awssdk.services.emr.model.ComputeLimitsUnitType;
import software.amazon.awssdk.services.emr.model.EbsConfiguration;
import software.amazon.awssdk.services.emr.model.InstanceGroupConfig;
import software.amazon.awssdk.services.emr.model.InstanceRoleType;
import software.amazon.awssdk.services.emr.model.JobFlowInstancesConfig;
import software.amazon.awssdk.services.emr.model.MarketType;

import sleeper.bulkimport.configuration.BulkImportPlatformSpec;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Random;
import java.util.function.IntUnaryOperator;

import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY;

public class EmrInstanceGroups implements EmrInstanceConfiguration {

    private final InstanceProperties instanceProperties;
    private final IntUnaryOperator randomSubnet;

    public EmrInstanceGroups(InstanceProperties instanceProperties) {
        this(instanceProperties, new Random()::nextInt);
    }

    public EmrInstanceGroups(InstanceProperties instanceProperties, IntUnaryOperator randomSubnet) {
        this.instanceProperties = instanceProperties;
        this.randomSubnet = randomSubnet;
    }

    public JobFlowInstancesConfig createJobFlowInstancesConfig(
            EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {

        return JobFlowInstancesConfig.builder()
                .ec2SubnetId(randomSubnet())
                .instanceGroups(
                        InstanceGroupConfig.builder()
                                .name("Executors")
                                .instanceType(platformSpec.getList(BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES).get(0))
                                .instanceRole(InstanceRoleType.CORE)
                                .instanceCount(platformSpec.getInt(BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY))
                                .ebsConfiguration(ebsConfiguration)
                                .market(MarketType.fromValue(platformSpec.getOrDefault(
                                        BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE, "SPOT")))
                                .build(),
                        InstanceGroupConfig.builder()
                                .name("Driver")
                                .instanceType(platformSpec.getList(BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES).get(0))
                                .instanceRole(InstanceRoleType.MASTER)
                                .instanceCount(1)
                                .ebsConfiguration(ebsConfiguration)
                                .build())
                .build();
    }

    @Override
    public ComputeLimits createComputeLimits(BulkImportPlatformSpec platformSpec) {

        Integer maxNumberOfExecutors = Integer.max(
                platformSpec.getInt(BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY),
                platformSpec.getInt(BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY));
        return ComputeLimits.builder()
                .unitType(ComputeLimitsUnitType.INSTANCES)
                .minimumCapacityUnits(1)
                .maximumCapacityUnits(maxNumberOfExecutors)
                .build();
    }

    private String randomSubnet() {
        List<String> subnets = instanceProperties.getList(SUBNETS);
        return subnets.get(randomSubnet.applyAsInt(subnets.size()));
    }
}
