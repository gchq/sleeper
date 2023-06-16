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
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetType;
import com.amazonaws.services.elasticmapreduce.model.InstanceTypeConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;

import sleeper.configuration.properties.InstanceProperties;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNETS;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE;

public class EmrInstanceFleets implements EmrInstanceConfiguration {

    private final InstanceProperties instanceProperties;

    public EmrInstanceFleets(InstanceProperties instanceProperties) {
        this.instanceProperties = instanceProperties;
    }

    public JobFlowInstancesConfig createJobFlowInstancesConfig(
            EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {

        return new JobFlowInstancesConfig()
                .withEc2SubnetIds(instanceProperties.getList(SUBNETS))
                .withInstanceFleets(
                        executorFleet(ebsConfiguration, platformSpec),
                        driverFleet(ebsConfiguration, platformSpec));
    }

    private InstanceFleetConfig executorFleet(
            EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {
        String executorInstanceType = platformSpec.get(BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE);
        InstanceFleetConfig config = new InstanceFleetConfig()
                .withName("Executors")
                .withInstanceFleetType(InstanceFleetType.CORE)
                .withInstanceTypeConfigs(new InstanceTypeConfig()
                        .withInstanceType(executorInstanceType)
                        .withEbsConfiguration(ebsConfiguration));

        String marketTypeOfExecutors = platformSpec.get(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE);
        int initialNumberOfExecutors = platformSpec.getInt(BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS);
        if ("ON_DEMAND".equals(marketTypeOfExecutors)) {
            config.setTargetOnDemandCapacity(initialNumberOfExecutors);
        } else {
            config.setTargetSpotCapacity(initialNumberOfExecutors);
        }
        return config;
    }

    private InstanceFleetConfig driverFleet(
            EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {
        String driverInstanceType = platformSpec.get(BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE);
        return new InstanceFleetConfig()
                .withName("Driver")
                .withInstanceFleetType(InstanceFleetType.MASTER)
                .withInstanceTypeConfigs(new InstanceTypeConfig()
                        .withInstanceType(driverInstanceType)
                        .withEbsConfiguration(ebsConfiguration))
                .withTargetOnDemandCapacity(1);
    }
}
