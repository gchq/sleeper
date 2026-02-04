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
package sleeper.core.properties.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.EnumUtils;

import sleeper.core.properties.SleeperProperty;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.DoublePredicate;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility methods for validating & reading Sleeper configuration properties.
 */
public class SleeperPropertyValueUtils {

    private SleeperPropertyValueUtils() {
    }

    private static final Set<String> VALID_LOG_RETENTION_VALUES = Sets.newHashSet("-1", "1", "3", "5", "7", "14",
            "30", "60", "90", "120", "150", "180", "365", "400", "545", "731", "1827", "3653");

    private static final Set<String> VALID_FADVISE_VALUES = Sets.newHashSet("normal", "sequential", "random");

    private static final Set<String> VALID_EBS_VOLUME_TYPES = Sets.newHashSet("gp2", "gp3", "io1", "io2");

    /**
     * Checks if a property value is a positive integer.
     *
     * @param  integer the value
     * @return         true if the value meets the requirement
     */
    public static boolean isPositiveInteger(String integer) {
        return parseAndCheckInteger(integer, num -> num > 0);
    }

    /**
     * Checks if a property value is unset or a positive integer.
     *
     * @param  integer the value
     * @return         true if the value meets the requirement
     */
    public static boolean isPositiveIntegerOrNull(String integer) {
        return integer == null || isPositiveInteger(integer);
    }

    /**
     * Checks if a property value is within the maximum batch size for processing messages from an AWS SQS FIFO queue.
     *
     * @param  integer the value
     * @return         true if the value meets the requirement
     */
    public static boolean isPositiveIntegerLtEq10(String integer) {
        return parseAndCheckInteger(integer, num -> num > 0 && num <= 10);
    }

    /**
     * Checks if a property value is an integer greater than or equal to 0.
     *
     * @param  integer the value
     * @return         true if the value meets the requirement
     */
    public static boolean isNonNegativeInteger(String integer) {
        return parseAndCheckInteger(integer, num -> num >= 0);
    }

    /**
     * Checks if a property value is an integer greater than or equal to 0 or unset.
     *
     * @param  integer the value
     * @return         true if the value meets the requirement
     */
    public static boolean isNonNegativeIntegerOrNull(String integer) {
        return integer == null || isNonNegativeInteger(integer);
    }

    /**
     * Checks if a property value is an integer.
     *
     * @param  integer the value
     * @return         true if the value meets the requirement
     */
    public static boolean isInteger(String integer) {
        return parseAndCheckInteger(integer, num -> true);
    }

    /**
     * Checks if a property value is a long integer greater than zero.
     *
     * @param  value the value
     * @return       true if the value meets the requirement
     */
    public static boolean isPositiveLong(String value) {
        return parseAndCheckLong(value, num -> num > 0);
    }

    /**
     * Checks if a property value is a long integer.
     *
     * @param  value the value
     * @return       true if the value meets the requirement
     */
    public static boolean isLong(String value) {
        return parseAndCheckLong(value, num -> true);
    }

    /**
     * Checks if a property value is a positive double precision decimal number.
     *
     * @param  value the value
     * @return       true if the value meets the requirement
     */
    public static boolean isPositiveDouble(String value) {
        return parseAndCheckDouble(value, num -> num > 0);
    }

    /**
     * Checks if a property value is a non-empty string.
     *
     * @param  string the value
     * @return        true if the value meets the requirement
     */
    public static boolean isNonNullNonEmptyString(String string) {
        return null != string && !string.isEmpty();
    }

    /**
     * Checks if a property value is a non-empty string with a maximum length.
     *
     * @param  string the value
     * @param  length the maximum length
     * @return        true if the value meets the requirement
     */
    public static boolean isNonNullNonEmptyStringWithMaxLength(String string, int length) {
        return isNonNullNonEmptyString(string) && string.length() <= length;
    }

    /**
     * Checks if a property value is a boolean. Must be true or false in lower case.
     *
     * @param  string the value
     * @return        true if the value meets the requirement
     */
    public static boolean isTrueOrFalse(String string) {
        return "true".equalsIgnoreCase(string) || "false".equalsIgnoreCase(string);
    }

    /**
     * Checks if a property value is a whole positive number of seconds within the maximum timeout for an invocation of
     * AWS Lambda.
     *
     * @param  timeout the value
     * @return         true if the value meets the requirement
     */
    public static boolean isValidLambdaTimeout(String timeout) {
        return parseAndCheckInteger(timeout, num -> num <= 900 && num > 0);
    }

    /**
     * Checks if a property value is a valid value for the maximum concurrency for an AWS Lambda SQS event source.
     * Also see the AWS documentation:
     * <p>
     * https://aws.amazon.com/blogs/compute/introducing-maximum-concurrency-of-aws-lambda-functions-when-using-amazon-sqs-as-an-event-source/
     * <p>
     * https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-scaling.html#events-sqs-max-concurrency
     *
     * @param  string the value
     * @return        true if the value meets the requirement
     */
    public static boolean isValidSqsLambdaMaximumConcurrency(String string) {
        return string == null || parseAndCheckInteger(string, num -> num >= 2);
    }

    /**
     * Checks if a property value is a valid setting for the Hadoop configuration property
     * `fs.s3a.experimental.input.fadvise`.
     *
     * @param  fadvise the value
     * @return         true if the value meets the requirement
     */
    public static boolean isValidFadvise(String fadvise) {
        return VALID_FADVISE_VALUES.contains(fadvise);
    }

    /**
     * Checks if a property value is a valid setting for log retention in days for an AWS CloudWatch log group.
     *
     * @param  logRetention the value
     * @return              true if the value meets the requirement
     */
    public static boolean isValidLogRetention(String logRetention) {
        return VALID_LOG_RETENTION_VALUES.contains(logRetention);
    }

    /**
     * Checks if a property value is a valid number of bytes to configure the Hadoop AWS integration.
     *
     * @param  numberOfBytes the value
     * @return               true if the value meets the requirement
     */
    public static boolean isValidHadoopLongBytes(String numberOfBytes) {
        if (!isNonNullNonEmptyString(numberOfBytes)) {
            return false;
        }
        return numberOfBytes.matches("\\d+[KMGTPE]?");
    }

    private static final Pattern BYTES_PATTERN = Pattern.compile("(\\d+)([KMGTPE]?)");

    /**
     * Checks if a property value is a valid number of bytes to configure Sleeper.
     *
     * @param  numberOfBytes the value
     * @return               true if the value meets the requirement
     */
    public static boolean isValidNumberOfBytes(String numberOfBytes) {
        if (!isNonNullNonEmptyString(numberOfBytes)) {
            return false;
        }
        return BYTES_PATTERN.matcher(numberOfBytes).matches();
    }

    /**
     * Reads a Sleeper property value that sets a number of bytes within Sleeper.
     *
     * @param  value the value
     * @return       the number of bytes
     */
    public static long readBytes(String value) {
        Matcher matcher = BYTES_PATTERN.matcher(value);
        matcher.matches();
        return Long.parseLong(matcher.group(1)) * readFileSizeUnits(matcher.group(2));
    }

    private static long readFileSizeUnits(String units) {
        switch (units) {
            case "":
                return 1;
            case "K":
                return 1024L;
            case "M":
                return 1024L * 1024L;
            case "G":
                return 1024L * 1024L * 1024L;
            case "T":
                return 1024L * 1024L * 1024L * 1024L;
            case "P":
                return 1024L * 1024L * 1024L * 1024L * 1024L;
            case "E":
                return 1024L * 1024L * 1024L * 1024L * 1024L * 1024L;
            default:
                throw new IllegalArgumentException("Unrecognised file size units: " + units);
        }
    }

    /**
     * Checks if a property value is a valid size for an AWS EBS volume for AWS EMR.
     *
     * @param  ebsSizeInGb the value
     * @return             true if the value meets the requirement
     */
    public static boolean isValidEbsSize(String ebsSizeInGb) {
        if (!isNonNullNonEmptyString(ebsSizeInGb)) {
            return false;
        }
        // From source code to software.amazon.awscdk.services.emr.CfnCluster.VolumeSpecificationProperty.Builder:
        // "This can be a number from 1 - 1024. If the volume type is EBS-optimized, the minimum value is 10."
        return parseAndCheckInteger(ebsSizeInGb, num -> num >= 10 && num <= 1024);
    }

    /**
     * Checks if a property value is a valid type of an AWS EBS volume for AWS EMR.
     *
     * @param  ebsVolumeType the value
     * @return               true if the value meets the requirement
     */
    public static boolean isValidEbsVolumeType(String ebsVolumeType) {
        return VALID_EBS_VOLUME_TYPES.contains(ebsVolumeType);
    }

    /**
     * Checks if a property value is an integer between one and a maximum value (inclusive).
     *
     * @param  string   the value
     * @param  maxValue the maximum value
     * @return          true if the value meets the requirement
     */
    public static boolean isPositiveIntLtEqValue(String string, int maxValue) {
        if (!isNonNullNonEmptyString(string)) {
            return false;
        }
        return parseAndCheckInteger(string, num -> num >= 1 && num <= maxValue);
    }

    /**
     * Checks if a property value is an integer between zero and a maximum value (inclusive).
     *
     * @param  string   the value
     * @param  maxValue the maximum value
     * @return          true if the value meets the requirement
     */
    public static boolean isNonNegativeIntLtEqValue(String string, int maxValue) {
        if (!isNonNullNonEmptyString(string)) {
            return false;
        }
        return parseAndCheckInteger(string, num -> num >= 0 && num <= maxValue);
    }

    /**
     * Checks if a property value is a comma-separated list of strings, with a maximum number of values.
     *
     * @param  input   the value
     * @param  maxSize the maximum number of values
     * @return         true if the value meets the requirement
     */
    public static boolean isListWithMaxSize(String input, int maxSize) {
        List<String> values = SleeperPropertyValueUtils.readList(input);
        return values.size() <= maxSize;
    }

    /**
     * Checks if a property value is a comma-separated list with a matching number of key and value pairs.
     *
     * @param  input the value
     * @return       true if the value meets the requirement
     */
    public static boolean isListInKeyValueFormat(String input) {
        List<String> values = SleeperPropertyValueUtils.readList(input);
        return values.size() % 2 == 0; //Expect even amount of items when in key-value format
    }

    private static boolean parseAndCheckInteger(String string, IntPredicate check) {
        try {
            return check.test(Integer.parseInt(string));
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean parseAndCheckLong(String string, LongPredicate check) {
        try {
            return check.test(Long.parseLong(string));
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean parseAndCheckDouble(String string, DoublePredicate check) {
        try {
            return check.test(Double.parseDouble(string));
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Generates a list of valid values for an enum, to be used in a Sleeper property description. Converts each value
     * to lower case.
     *
     * @param  <T> the enum type
     * @param  cls the num class
     * @return     the valid values description, in lower case
     */
    public static <T extends Enum<T>> String describeEnumValuesInLowerCase(Class<T> cls) {
        return Stream.of(cls.getEnumConstants()).map(Enum::toString)
                .map(String::toLowerCase).collect(Collectors.toList()).toString();
    }

    /**
     * Generates a list of valid values for an enum, to be used in a Sleeper property description. Uses each value as it
     * appears in the enum.
     *
     * @param  <T> the enum type
     * @param  cls the num class
     * @return     the valid values description
     */
    public static <T extends Enum<T>> String describeEnumValues(Class<T> cls) {
        return Stream.of(cls.getEnumConstants()).map(Enum::toString).collect(Collectors.toList()).toString();
    }

    /**
     * Streams the values of a property for a list of an enum type.
     *
     * @param  <E>       the enum type representing valid values of the property
     * @param  property  the property
     * @param  value     the raw value of the property
     * @param  enumClass the class of the enum representing valid values of the property
     * @return           the list of enum values
     */
    public static <E extends Enum<E>> Stream<E> streamEnumList(SleeperProperty property, String value, Class<E> enumClass) {
        return readList(value).stream()
                .map(item -> Optional.ofNullable(EnumUtils.getEnumIgnoreCase(enumClass, item))
                        .orElseThrow(() -> new IllegalArgumentException("Unrecognised value for " + property + ": " + item)));
    }

    /**
     * Reads the value of a property for a list of strings.
     *
     * @param  value the value
     * @return       the list of strings
     */
    public static List<String> readList(String value) {
        if (value == null || value.length() < 1) {
            return List.of();
        } else {
            return Lists.newArrayList(value.split(","));
        }
    }

}
