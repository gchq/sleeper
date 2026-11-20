/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.bulkimport.core.configuration;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.EmrInstanceArchitecture;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_DYNAMIC_ALLOCATION_ENABLED;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_EXECUTOR_INSTANCES;

class SparkConfigurationUtilsTest {

    InstanceProperties instanceProperties = new InstanceProperties();

    @Test
    void shouldSetDynamicAllocationForNonPersistentEmr() {
        // Given
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_DYNAMIC_ALLOCATION_ENABLED, "true");

        // When / Then
        assertThat(SparkConfigurationUtils.getSparkConfigurationForEMRFromInstanceProperties(instanceProperties, EmrInstanceArchitecture.X86_64))
                .containsEntry("spark.dynamicAllocation.enabled", "true");
    }

    @Test
    void shouldNotSetDynamicAllocationForPersistentEmrCluster() {
        // Given
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_DYNAMIC_ALLOCATION_ENABLED, "true");
        instanceProperties.set(BULK_IMPORT_EMR_SPARK_EXECUTOR_INSTANCES, "29");

        // When / Then
        assertThat(SparkConfigurationUtils.getSparkConfigurationForPersistentEMRFromInstanceProperties(instanceProperties, EmrInstanceArchitecture.X86_64))
                .doesNotContainKey("spark.dynamicAllocation.enabled")
                .containsEntry("spark.executor.instances", "29");
    }

    @Test
    void shouldOnlyDifferInDynamicAllocationBetweenPersistentAndNonPersistentEmr() {
        // When
        var nonPersistent = SparkConfigurationUtils.getSparkConfigurationForEMRFromInstanceProperties(instanceProperties, EmrInstanceArchitecture.X86_64);
        var persistent = SparkConfigurationUtils.getSparkConfigurationForPersistentEMRFromInstanceProperties(instanceProperties, EmrInstanceArchitecture.X86_64);

        // Then
        nonPersistent.remove("spark.dynamicAllocation.enabled");
        assertThat(persistent).isEqualTo(nonPersistent);
    }
}
