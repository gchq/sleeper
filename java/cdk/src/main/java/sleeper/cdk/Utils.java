/*
 * Copyright 2022 Crown Copyright
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
package sleeper.cdk;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperties;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.Tags;
import software.amazon.awscdk.services.logs.RetentionDays;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.APACHE_LOGGING_LEVEL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.AWS_LOGGING_LEVEL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.LOGGING_LEVEL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.PARQUET_LOGGING_LEVEL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ROOT_LOGGING_LEVEL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.STACK_TAG_NAME;

/**
 * Collection of utility methods related to the CDK deployment
 */
public class Utils {

    private Utils() {
        // Prevents instantiation
    }

    public static Map<String, String> createDefaultEnvironment(InstanceProperties instanceProperties) {
        Map<String, String> environmentVariables = new HashMap<>();
        environmentVariables.put(CONFIG_BUCKET.toEnvironmentVariable(),
                instanceProperties.get(CONFIG_BUCKET));

        environmentVariables.put("JAVA_TOOL_OPTIONS", createToolOptions(instanceProperties,
                LOGGING_LEVEL,
                ROOT_LOGGING_LEVEL,
                APACHE_LOGGING_LEVEL,
                PARQUET_LOGGING_LEVEL,
                AWS_LOGGING_LEVEL));

        return environmentVariables;
    }

    private static String createToolOptions(InstanceProperties instanceProperties, InstanceProperty... propertyNames) {
        StringBuilder sb = new StringBuilder();
        Arrays.stream(propertyNames)
                .filter(s -> instanceProperties.get(s) != null)
                .forEach(s -> {
                    sb.append("-D").append(s.getPropertyName()).append("=").append(instanceProperties.get(s)).append(" ");
                });

        return sb.toString();
    }

    public static String truncateToMaxSize(String input, int maxSize) {
        if (input.length() > maxSize) {
            return input.substring(0, maxSize);
        }
        return input;
    }

    public static String truncateTo64Characters(String input) {
        return truncateToMaxSize(input, 64);
    }

    /**
     * Valid values are taken from https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-logs-loggroup.html
     * A value of -1 represents an infinite number of days.
     *
     * @param numberOfDays number of days you want to retain the logs
     * @return The RetentionDays equivalent
     */
    public static RetentionDays getRetentionDays(int numberOfDays) {
        switch (numberOfDays) {
            case -1:
                return RetentionDays.INFINITE;
            case 1:
                return RetentionDays.ONE_DAY;
            case 3:
                return RetentionDays.THREE_DAYS;
            case 5:
                return RetentionDays.FIVE_DAYS;
            case 7:
                return RetentionDays.ONE_WEEK;
            case 14:
                return RetentionDays.TWO_WEEKS;
            case 30:
                return RetentionDays.ONE_MONTH;
            case 60:
                return RetentionDays.TWO_MONTHS;
            case 90:
                return RetentionDays.THREE_MONTHS;
            case 120:
                return RetentionDays.FOUR_MONTHS;
            case 150:
                return RetentionDays.FIVE_MONTHS;
            case 180:
                return RetentionDays.SIX_MONTHS;
            case 365:
                return RetentionDays.ONE_YEAR;
            case 400:
                return RetentionDays.THIRTEEN_MONTHS;
            case 545:
                return RetentionDays.EIGHTEEN_MONTHS;
            case 731:
                return RetentionDays.TWO_YEARS;
            case 1827:
                return RetentionDays.FIVE_YEARS;
            case 3653:
                return RetentionDays.TEN_YEARS;
            default:
                throw new IllegalArgumentException("Invalid number of days; see https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-logs-loggroup.html for valid options");
        }
    }

    public static Stream<TableProperties> getAllTableProperties(InstanceProperties instanceProperties) {
        return instanceProperties.getList(UserDefinedInstanceProperty.TABLE_PROPERTIES).stream()
                .map(File::new)
                .flatMap(f -> {
                    if (!f.exists()) {
                        throw new RuntimeException("There was a problem with the table configuration. " +
                                f.getAbsolutePath() + " doesn't exist");
                    }
                    if (f.isDirectory()) {
                        return Arrays.stream(f.listFiles())
                                .map(file -> {
                                    try {
                                        return new FileInputStream(file);
                                    } catch (FileNotFoundException e) {
                                        // This should never happen
                                        throw new RuntimeException("Failed to open stream to file: " + file.getAbsolutePath());
                                    }
                                });
                    }
                    try {
                        return Stream.of(new FileInputStream(f));
                    } catch (FileNotFoundException e) {
                        // this should never happen
                        throw new RuntimeException("Failed to open stream to file: " + f.getAbsolutePath());
                    }
                })
                .map(fis -> {
                    TableProperties tableProperties = new TableProperties(instanceProperties);
                    tableProperties.load(fis);
                    return tableProperties;
                });
    }

    public static void addStackTagIfSet(Stack stack, InstanceProperties properties) {
        Optional.ofNullable(properties.get(STACK_TAG_NAME))
                .ifPresent(tagName -> Tags.of(stack).add(tagName, stack.getNode().getId()));
    }
}
