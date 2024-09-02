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
package sleeper.configuration;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.EnumUtils;

import sleeper.configuration.properties.SleeperPropertyValues;
import sleeper.configuration.properties.validation.EmrInstanceArchitecture;

import java.util.List;
import java.util.Set;
import java.util.function.DoublePredicate;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility methods for interacting with SQS queues.
 */
public class Utils {

    private Utils() {
    }

    private static final Set<String> VALID_LOG_RETENTION_VALUES = Sets.newHashSet("-1", "1", "3", "5", "7", "14",
            "30", "60", "90", "120", "150", "180", "365", "400", "545", "731", "1827", "3653");

    private static final Set<String> VALID_FADVISE_VALUES = Sets.newHashSet("normal", "sequential", "random");

    private static final Set<String> VALID_EBS_VOLUME_TYPES = Sets.newHashSet("gp2", "gp3", "io1", "io2");

    public static boolean isPositiveInteger(String integer) {
        return parseAndCheckInteger(integer, num -> num > 0);
    }

    public static boolean isPositiveIntegerLtEq10(String integer) {
        return parseAndCheckInteger(integer, num -> num > 0 && num <= 10);
    }

    public static boolean isPositiveIntegerLtEq900(String integer) {
        return parseAndCheckInteger(integer, num -> num > 0 && num <= 900);
    }

    public static boolean isNonNegativeInteger(String integer) {
        return parseAndCheckInteger(integer, num -> num >= 0);
    }

    public static boolean isInteger(String integer) {
        return parseAndCheckInteger(integer, num -> true);
    }

    public static boolean isPositiveLong(String value) {
        return parseAndCheckLong(value, num -> num > 0);
    }

    public static boolean isLong(String value) {
        return parseAndCheckLong(value, num -> true);
    }

    public static boolean isPositiveDouble(String value) {
        return parseAndCheckDouble(value, num -> num > 0);
    }

    public static boolean isNonNullNonEmptyString(String string) {
        return null != string && !string.isEmpty();
    }

    public static boolean isNonNullNonEmptyStringWithMaxLength(String string, int length) {
        return isNonNullNonEmptyString(string) && string.length() <= length;
    }

    public static boolean isTrueOrFalse(String string) {
        return "true".equalsIgnoreCase(string) || "false".equalsIgnoreCase(string);
    }

    public static boolean isValidLambdaTimeout(String timeout) {
        return parseAndCheckInteger(timeout, num -> num <= 900 && num > 0);
    }

    public static boolean isValidFadvise(String fadvise) {
        return VALID_FADVISE_VALUES.contains(fadvise);
    }

    public static boolean isValidLogRetention(String logRetention) {
        return VALID_LOG_RETENTION_VALUES.contains(logRetention);
    }

    public static boolean isValidHadoopLongBytes(String numberOfBytes) {
        if (!isNonNullNonEmptyString(numberOfBytes)) {
            return false;
        }
        return numberOfBytes.matches("\\d+[KMGTPE]?");
    }

    private static final Pattern BYTES_PATTERN = Pattern.compile("(\\d+)([KMGTPE]?)");

    public static boolean isValidNumberOfBytes(String numberOfBytes) {
        if (!isNonNullNonEmptyString(numberOfBytes)) {
            return false;
        }
        return BYTES_PATTERN.matcher(numberOfBytes).matches();
    }

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

    public static boolean isValidEbsSize(String ebsSizeInGb) {
        if (!isNonNullNonEmptyString(ebsSizeInGb)) {
            return false;
        }
        // From source code to software.amazon.awscdk.services.emr.CfnCluster.VolumeSpecificationProperty.Builder:
        // "This can be a number from 1 - 1024. If the volume type is EBS-optimized, the minimum value is 10."
        return parseAndCheckInteger(ebsSizeInGb, num -> num >= 10 && num <= 1024);
    }

    public static boolean isValidEbsVolumeType(String ebsVolumeType) {
        return VALID_EBS_VOLUME_TYPES.contains(ebsVolumeType);
    }

    public static boolean isPositiveIntLtEqValue(String string, int maxValue) {
        if (!isNonNullNonEmptyString(string)) {
            return false;
        }
        return parseAndCheckInteger(string, num -> num >= 1 && num <= maxValue);
    }

    public static boolean isValidArchitecture(String input) {
        if (input == null) {
            return false;
        }
        return SleeperPropertyValues.readList(input).stream()
                .allMatch(architecture -> EnumUtils.isValidEnumIgnoreCase(EmrInstanceArchitecture.class, architecture));
    }

    public static boolean isNonNegativeIntLtEqValue(String string, int maxValue) {
        if (!isNonNullNonEmptyString(string)) {
            return false;
        }
        return parseAndCheckInteger(string, num -> num >= 0 && num <= maxValue);
    }

    public static boolean isListWithMaxSize(String input, int maxSize) {
        List<String> values = SleeperPropertyValues.readList(input);
        return values.size() <= maxSize;
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

    public static <T extends Enum<T>> String describeEnumValuesInLowerCase(Class<T> cls) {
        return Stream.of(cls.getEnumConstants()).map(Enum::toString)
                .map(String::toLowerCase).collect(Collectors.toList()).toString();
    }

}
