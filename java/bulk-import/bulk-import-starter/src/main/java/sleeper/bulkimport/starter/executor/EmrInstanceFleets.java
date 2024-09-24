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
import software.amazon.awssdk.services.emr.model.Configuration;
import software.amazon.awssdk.services.emr.model.EbsConfiguration;
import software.amazon.awssdk.services.emr.model.InstanceFleetConfig;
import software.amazon.awssdk.services.emr.model.InstanceFleetType;
import software.amazon.awssdk.services.emr.model.InstanceTypeConfig;
import software.amazon.awssdk.services.emr.model.JobFlowInstancesConfig;

import sleeper.bulkimport.configuration.BulkImportPlatformSpec;
import sleeper.bulkimport.configuration.ConfigurationUtils;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.EmrInstanceArchitecture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_ARM_INSTANCE_TYPES;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_INSTANCE_ARCHITECTURE;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_MASTER_ARM_INSTANCE_TYPES;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY;
import static sleeper.core.properties.validation.EmrInstanceTypeConfig.readInstanceTypes;

public class EmrInstanceFleets implements EmrInstanceConfiguration {

    private final InstanceProperties instanceProperties;

    public EmrInstanceFleets(InstanceProperties instanceProperties) {
        this.instanceProperties = instanceProperties;
    }

    public JobFlowInstancesConfig createJobFlowInstancesConfig(
            EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {

        return JobFlowInstancesConfig.builder()
                .ec2SubnetIds(instanceProperties.getList(SUBNETS))
                .instanceFleets(
                        executorFleet(ebsConfiguration, platformSpec),
                        driverFleet(ebsConfiguration, platformSpec))
                .build();
    }

    @Override
    public ComputeLimits createComputeLimits(BulkImportPlatformSpec platformSpec) {

        return ComputeLimits.builder()
                .unitType(ComputeLimitsUnitType.INSTANCE_FLEET_UNITS)
                .minimumCapacityUnits(platformSpec.getInt(BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY))
                .maximumCapacityUnits(platformSpec.getInt(BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY))
                .build();
    }

    private InstanceFleetConfig executorFleet(
            EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {
        InstanceFleetConfig config = InstanceFleetConfig.builder()
                .name("Executors")
                .instanceFleetType(InstanceFleetType.CORE)
                .instanceTypeConfigs(readExecutorInstanceTypes(instanceProperties, ebsConfiguration, platformSpec))
                .build();

        int initialExecutorCapacity = platformSpec.getInt(BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY);
        if ("ON_DEMAND".equals(platformSpec.get(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE))) {
            config = config.toBuilder().targetOnDemandCapacity(initialExecutorCapacity).build();
        } else {
            config = config.toBuilder().targetSpotCapacity(initialExecutorCapacity).build();
        }
        return config;
    }

    private static List<InstanceTypeConfig> readExecutorInstanceTypes(
            InstanceProperties instanceProperties, EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {
        return readInstanceTypes(platformSpec.getTableProperties(), BULK_IMPORT_EMR_INSTANCE_ARCHITECTURE,
                BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES, BULK_IMPORT_EMR_EXECUTOR_ARM_INSTANCE_TYPES)
                .map(config -> InstanceTypeConfig.builder()
                        .instanceType(config.getInstanceType())
                        .weightedCapacity(config.getWeightedCapacity())
                        .ebsConfiguration(ebsConfiguration)
                        .configurations(getConfigurations(instanceProperties, config.getArchitecture()))
                        .build())
                .collect(Collectors.toList());
    }

    private InstanceFleetConfig driverFleet(
            EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {
        return InstanceFleetConfig.builder()
                .name("Driver")
                .instanceFleetType(InstanceFleetType.MASTER)
                .instanceTypeConfigs(readMasterInstanceTypes(ebsConfiguration, platformSpec))
                .targetOnDemandCapacity(1)
                .build();
    }

    private List<InstanceTypeConfig> readMasterInstanceTypes(
            EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {
        return readInstanceTypes(platformSpec.getTableProperties(), BULK_IMPORT_EMR_INSTANCE_ARCHITECTURE,
                BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES, BULK_IMPORT_EMR_MASTER_ARM_INSTANCE_TYPES)
                .map(config -> InstanceTypeConfig.builder()
                        .instanceType(config.getInstanceType())
                        .ebsConfiguration(ebsConfiguration)
                        .configurations(getConfigurations(instanceProperties, config.getArchitecture()))
                        .build())
                .collect(Collectors.toList());
    }

    private static List<Configuration> getConfigurations(
            InstanceProperties instanceProperties, EmrInstanceArchitecture architecture) {
        List<Configuration> configurations = new ArrayList<>();

        Map<String, String> emrSparkProps = ConfigurationUtils.getSparkEMRConfiguration();
        Configuration emrConfiguration = Configuration.builder()
                .classification("spark")
                .properties(emrSparkProps)
                .build();
        configurations.add(emrConfiguration);

        Map<String, String> yarnConf = ConfigurationUtils.getYarnConfiguration();
        Configuration yarnConfiguration = Configuration.builder()
                .classification("yarn-site")
                .properties(yarnConf)
                .build();
        configurations.add(yarnConfiguration);

        Map<String, String> sparkConf = ConfigurationUtils.getSparkConfigurationFromInstanceProperties(
                instanceProperties, architecture);
        Configuration sparkDefaultsConfigurations = Configuration.builder()
                .classification("spark-defaults")
                .properties(sparkConf)
                .build();
        configurations.add(sparkDefaultsConfigurations);

        Map<String, String> sparkExecutorJavaHome = new HashMap<>();
        sparkExecutorJavaHome.put("JAVA_HOME", ConfigurationUtils.getJavaHome(architecture));
        Configuration sparkEnvExportConfigurations = Configuration.builder()
                .classification("export")
                .properties(sparkExecutorJavaHome)
                .build();
        Configuration sparkEnvConfigurations = Configuration.builder()
                .classification("spark-env")
                .configurations(sparkEnvExportConfigurations)
                .build();
        configurations.add(sparkEnvConfigurations);

        Map<String, String> mapReduceSiteConf = ConfigurationUtils.getMapRedSiteConfiguration();
        Configuration mapRedSiteConfigurations = Configuration.builder()
                .classification("mapred-site")
                .properties(mapReduceSiteConf)
                .build();
        configurations.add(mapRedSiteConfigurations);

        Map<String, String> javaHomeConf = ConfigurationUtils.getJavaHomeConfiguration(architecture);

        Configuration hadoopEnvExportConfigurations = Configuration.builder()
                .classification("export")
                .properties(javaHomeConf)
                .build();
        Configuration hadoopEnvConfigurations = Configuration.builder()
                .classification("hadoop-env")
                .configurations(hadoopEnvExportConfigurations)
                .build();
        configurations.add(hadoopEnvConfigurations);

        return configurations;
    }
}
