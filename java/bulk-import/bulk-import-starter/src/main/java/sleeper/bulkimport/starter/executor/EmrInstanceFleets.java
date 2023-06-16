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
import sleeper.configuration.properties.table.TableProperty;

import java.util.List;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNETS;

public class EmrInstanceFleets implements EmrInstanceConfiguration {

    private final InstanceProperties instanceProperties;

    public EmrInstanceFleets(InstanceProperties instanceProperties) {
        this.instanceProperties = instanceProperties;
    }

    public JobFlowInstancesConfig createJobFlowInstancesConfig(
            EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {

        JobFlowInstancesConfig config = new JobFlowInstancesConfig()
                .withEc2SubnetIds(instanceProperties.getList(SUBNETS));

        String driverInstanceType = platformSpec.get(TableProperty.BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE);
        String executorInstanceType = platformSpec.get(TableProperty.BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE);
        Integer initialNumberOfExecutors = platformSpec.getInt(TableProperty.BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS);

        config.setInstanceFleets(List.of(
                new InstanceFleetConfig()
                        .withName("Executors")
                        .withInstanceFleetType(InstanceFleetType.CORE)
                        .withInstanceTypeConfigs(new InstanceTypeConfig()
                                .withInstanceType(executorInstanceType)
                                .withEbsConfiguration(ebsConfiguration))
                        .withTargetOnDemandCapacity(initialNumberOfExecutors),
                new InstanceFleetConfig()
                        .withName("Driver")
                        .withInstanceFleetType(InstanceFleetType.MASTER)
                        .withInstanceTypeConfigs(new InstanceTypeConfig()
                                .withInstanceType(driverInstanceType)
                                .withEbsConfiguration(ebsConfiguration))
                        .withTargetOnDemandCapacity(1)
        ));

        return config;
    }
}
