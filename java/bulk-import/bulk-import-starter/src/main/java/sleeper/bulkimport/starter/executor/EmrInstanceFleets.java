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
import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.EbsConfiguration;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetType;
import com.amazonaws.services.elasticmapreduce.model.InstanceTypeConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;

import sleeper.bulkimport.configuration.BulkImportPlatformSpec;
import sleeper.bulkimport.configuration.ConfigurationUtils;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.bulkimport.configuration.ConfigurationUtils.Architecture;
import static sleeper.bulkimport.configuration.EmrInstanceTypeConfig.readInstanceTypes;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_ARM_INSTANCE_TYPES;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_INSTANCE_ARCHITECTURE;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_MASTER_ARM_INSTANCE_TYPES;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY;

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

    @Override
    public ComputeLimits createComputeLimits(BulkImportPlatformSpec platformSpec) {

        return new ComputeLimits()
                .withUnitType(ComputeLimitsUnitType.InstanceFleetUnits)
                .withMinimumCapacityUnits(platformSpec.getInt(BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY))
                .withMaximumCapacityUnits(platformSpec.getInt(BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY));
    }

    private InstanceFleetConfig executorFleet(
            EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {
        InstanceFleetConfig config = new InstanceFleetConfig()
                .withName("Executors")
                .withInstanceFleetType(InstanceFleetType.CORE)
                .withInstanceTypeConfigs(readExecutorInstanceTypes(instanceProperties, ebsConfiguration, platformSpec));

        int initialExecutorCapacity = platformSpec.getInt(BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY);
        if ("ON_DEMAND".equals(platformSpec.get(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE))) {
            config.setTargetOnDemandCapacity(initialExecutorCapacity);
        } else {
            config.setTargetSpotCapacity(initialExecutorCapacity);
        }
        return config;
    }

    private static List<InstanceTypeConfig> readExecutorInstanceTypes(
            InstanceProperties instanceProperties, EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {
        return readInstanceTypes(platformSpec.getTableProperties(), BULK_IMPORT_EMR_INSTANCE_ARCHITECTURE,
                BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES, BULK_IMPORT_EMR_EXECUTOR_ARM_INSTANCE_TYPES)
                .map(config -> new InstanceTypeConfig()
                        .withInstanceType(config.getInstanceType())
                        .withWeightedCapacity(config.getWeightedCapacity())
                        .withEbsConfiguration(ebsConfiguration)
                        .withConfigurations(getConfigurations(instanceProperties, Architecture.from(config.getArchitecture()))))
                .collect(Collectors.toList());
    }

    private InstanceFleetConfig driverFleet(
            EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {
        return new InstanceFleetConfig()
                .withName("Driver")
                .withInstanceFleetType(InstanceFleetType.MASTER)
                .withInstanceTypeConfigs(readMasterInstanceTypes(ebsConfiguration, platformSpec))
                .withTargetOnDemandCapacity(1);
    }

    private List<InstanceTypeConfig> readMasterInstanceTypes(
            EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {
        return readInstanceTypes(platformSpec.getTableProperties(), BULK_IMPORT_EMR_INSTANCE_ARCHITECTURE,
                BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES, BULK_IMPORT_EMR_MASTER_ARM_INSTANCE_TYPES)
                .map(config -> new InstanceTypeConfig()
                        .withInstanceType(config.getInstanceType())
                        .withEbsConfiguration(ebsConfiguration)
                        .withConfigurations(getConfigurations(instanceProperties, Architecture.from(config.getArchitecture()))))
                .collect(Collectors.toList());
    }

    private static List<Configuration> getConfigurations(
            InstanceProperties instanceProperties, Architecture architecture) {
        List<Configuration> configurations = new ArrayList<>();

        Map<String, String> emrSparkProps = ConfigurationUtils.getSparkEMRConfiguration();
        Configuration emrConfiguration = new Configuration()
                .withClassification("spark")
                .withProperties(emrSparkProps);
        configurations.add(emrConfiguration);

        Map<String, String> yarnConf = ConfigurationUtils.getYarnConfiguration();
        Configuration yarnConfiguration = new Configuration()
                .withClassification("yarn-site")
                .withProperties(yarnConf);
        configurations.add(yarnConfiguration);

        Map<String, String> sparkConf = ConfigurationUtils.getSparkConfigurationFromInstanceProperties(
                instanceProperties, architecture);
        Configuration sparkDefaultsConfigurations = new Configuration()
                .withClassification("spark-defaults")
                .withProperties(sparkConf);
        configurations.add(sparkDefaultsConfigurations);

        Map<String, String> sparkExecutorJavaHome = new HashMap<>();
        sparkExecutorJavaHome.put("JAVA_HOME", ConfigurationUtils.getJavaHome(architecture));
        Configuration sparkEnvExportConfigurations = new Configuration()
                .withClassification("export")
                .withProperties(sparkExecutorJavaHome);
        Configuration sparkEnvConfigurations = new Configuration()
                .withClassification("spark-env")
                .withConfigurations(sparkEnvExportConfigurations);
        configurations.add(sparkEnvConfigurations);

        Map<String, String> mapReduceSiteConf = ConfigurationUtils.getMapRedSiteConfiguration();
        Configuration mapRedSiteConfigurations = new Configuration()
                .withClassification("mapred-site")
                .withProperties(mapReduceSiteConf);
        configurations.add(mapRedSiteConfigurations);

        Map<String, String> javaHomeConf = ConfigurationUtils.getJavaHomeConfiguration(architecture);

        Configuration hadoopEnvExportConfigurations = new Configuration()
                .withClassification("export")
                .withProperties(javaHomeConf);
        Configuration hadoopEnvConfigurations = new Configuration()
                .withClassification("hadoop-env")
                .withConfigurations(hadoopEnvExportConfigurations);
        configurations.add(hadoopEnvConfigurations);

        return configurations;
    }
}
