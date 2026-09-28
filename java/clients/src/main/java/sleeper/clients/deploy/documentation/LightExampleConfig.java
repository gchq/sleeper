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
package sleeper.clients.deploy.documentation;

import sleeper.core.properties.instance.InstanceProperties;

import java.io.PrintWriter;
import java.io.Writer;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_DRIVER_CORES;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_DRIVER_MEMORY;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_EXECUTOR_CORES;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_EXECUTOR_EPHEMERAL_STORAGE;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_EXECUTOR_INSTANCES;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_EKS_SPARK_EXECUTOR_MEMORY;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_DRIVER_CORES;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_DRIVER_MEMORY;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_DISK;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_BATCHER_MAX_FILE_AGE_SECONDS;
import static sleeper.core.properties.model.OptionalStack.DEFAULT_STACKS;

/**
 * Generates the light example instance configuration.
 */
public class LightExampleConfig {

    private LightExampleConfig() {
    }

    public static void writeExampleLightInstanceProperties(Writer out) {
        PrintWriter writer = new PrintWriter(out);
        writer.println("""
                #################################################################################
                #                    Properties set below are designed for an                   #
                #                  instance aimed towards reducing running costs                #
                #               and will apply to any bulk import stacks you enable             #
                #################################################################################""");
        writer.println();
        InstanceProperties.createPrettyPrinterBuilder().writer(writer)
                .printTemplate(true)
                .hideUnsetProperties(true)
                .build().print(createLightInstanceProperties());
    }

    private static InstanceProperties createLightInstanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        // Emr Serverless properties
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES, "2");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY, "8G");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_DISK, "60G");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES, "2");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_DRIVER_CORES, "2");
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_DRIVER_MEMORY, "8G");

        // EKS properties
        instanceProperties.set(BULK_IMPORT_EKS_SPARK_EXECUTOR_CORES, "2");
        instanceProperties.set(BULK_IMPORT_EKS_SPARK_EXECUTOR_MEMORY, "8G");
        instanceProperties.set(BULK_IMPORT_EKS_SPARK_EXECUTOR_EPHEMERAL_STORAGE, "60Gi");
        instanceProperties.set(BULK_IMPORT_EKS_SPARK_EXECUTOR_INSTANCES, "2");
        instanceProperties.set(BULK_IMPORT_EKS_SPARK_DRIVER_CORES, "2");
        instanceProperties.set(BULK_IMPORT_EKS_SPARK_DRIVER_MEMORY, "8G");

        // Default table values
        instanceProperties.set(DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "8");
        instanceProperties.set(DEFAULT_INGEST_BATCHER_MAX_FILE_AGE_SECONDS, "1200");

        // Stack
        instanceProperties.set(OPTIONAL_STACKS, DEFAULT_STACKS.stream().map(stack -> stack.name()).collect(Collectors.joining(",")));
        return instanceProperties;
    }
}
