/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.util;

import sleeper.core.properties.instance.InstanceProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.LoggingLevelsProperty.APACHE_LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.AWS_LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.PARQUET_LOGGING_LEVEL;
import static sleeper.core.properties.instance.LoggingLevelsProperty.ROOT_LOGGING_LEVEL;

/**
 * Utilities to set environment variables during deployment.
 */
public class EnvironmentUtils {

    private EnvironmentUtils() {
        // Prevents instantiation
    }

    /**
     * Creates the default environment for a deployed process running in a Sleeper instance.
     *
     * @param  instanceProperties the instance properties
     * @return                    the environment variables
     */
    public static Map<String, String> createDefaultEnvironment(InstanceProperties instanceProperties) {
        Map<String, String> environmentVariables = createDefaultEnvironmentNoConfigBucket(instanceProperties);
        environmentVariables.put(CONFIG_BUCKET.toEnvironmentVariable(),
                instanceProperties.get(CONFIG_BUCKET));
        return environmentVariables;
    }

    /**
     * Creates the default environment for a deployed process running in a Sleeper instance, without the config bucket
     * environment variable.
     *
     * @param  instanceProperties the instance properties
     * @return                    the environment variables
     */
    public static Map<String, String> createDefaultEnvironmentNoConfigBucket(InstanceProperties instanceProperties) {
        Map<String, String> environmentVariables = new HashMap<>();
        environmentVariables.put("JAVA_TOOL_OPTIONS", createToolOptions(instanceProperties));
        return environmentVariables;
    }

    private static String createToolOptions(InstanceProperties instanceProperties) {
        StringBuilder sb = new StringBuilder();
        Stream.of(LOGGING_LEVEL,
                ROOT_LOGGING_LEVEL,
                APACHE_LOGGING_LEVEL,
                PARQUET_LOGGING_LEVEL,
                AWS_LOGGING_LEVEL)
                .filter(instanceProperties::isSet)
                .forEach(s -> sb.append("-D").append(s.getPropertyName())
                        .append("=").append(instanceProperties.get(s)).append(" "));
        Stream.of("java.base/java.nio=ALL-UNNAMED",
                "java.base/sun.nio.ch=ALL-UNNAMED",
                "java.base/java.util=ALL-UNNAMED",
                "java.base/java.lang.invoke=ALL-UNNAMED")
                .forEach(s -> sb.append("--add-opens=").append(s).append(" "));
        return sb.toString();
    }
}
