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
package sleeper.configuration;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.EnumUtils;

import sleeper.configuration.properties.table.CompressionCodec;

import java.util.Set;

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
        return Integer.parseInt(integer) > 0;
    }

    public static boolean isPositiveLong(String value) {
        return Long.parseLong(value) > 0;
    }

    public static boolean isPositiveDouble(String value) {
        return Double.parseDouble(value) > 0;
    }

    public static boolean isNonNullNonEmptyString(String string) {
        return null != string && !string.isEmpty();
    }

    public static boolean isTrueOrFalse(String string) {
        return string.equalsIgnoreCase("true") || string.equalsIgnoreCase("false");
    }

    public static boolean isValidLambdaTimeout(String timeout) {
        int i = Integer.parseInt(timeout);
        return i <= 900 && i > 0;
    }

    public static boolean isValidFadvise(String fadvise) {
        return VALID_FADVISE_VALUES.contains(fadvise);
    }

    public static boolean isValidCompressionCodec(String codec) {
        return EnumUtils.isValidEnumIgnoreCase(CompressionCodec.class, codec);
    }

    public static boolean isValidLogRetention(String logRetention) {
        return VALID_LOG_RETENTION_VALUES.contains(logRetention);
    }

    public static boolean isValidNumberOfBytes(String numberOfBytes) {
        if (!isNonNullNonEmptyString(numberOfBytes)) {
            return false;
        }
        return numberOfBytes.matches("[0-9]+[KMG]?");
    }

    public static boolean isValidEbsSize(String ebsSizeInGb) {
        if (!isNonNullNonEmptyString(ebsSizeInGb)) {
            return false;
        }
        int ebsSizeInGbInt = Integer.parseInt(ebsSizeInGb);
        // From source code to software.amazon.awscdk.services.emr.CfnCluster.VolumeSpecificationProperty.Builder:
        // "This can be a number from 1 - 1024. If the volume type is EBS-optimized, the minimum value is 10."
        return ebsSizeInGbInt >= 10 && ebsSizeInGbInt <= 1024;
    }

    public static boolean isValidEbsVolumeType(String ebsVolumeType) {
        return VALID_EBS_VOLUME_TYPES.contains(ebsVolumeType);
    }

    public static boolean isPositiveIntLtEqValue(String string, int maxValue) {
        if (!isNonNullNonEmptyString(string)) {
            return false;
        }
        int stringAsInt = Integer.parseInt(string);
        return stringAsInt >= 1 && stringAsInt <= maxValue;
    }

}
