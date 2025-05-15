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

import java.util.ArrayList;
import java.util.List;

/**
 * A utility to format numbers as human readable strings.
 */
public class NumberFormatUtils {
    private NumberFormatUtils() {
    }

    private static final long K_COUNT = 1_000;
    private static final long M_COUNT = 1_000_000;
    private static final long G_COUNT = 1_000_000_000;
    private static final long T_COUNT = 1_000_000_000_000L;

    /**
     * Formats a number of bytes as a string, followed by a human readable representation.
     *
     * @param  fileSize the number of bytes to format
     * @return          a string representation of the number of bytes
     */
    public static String formatBytes(long fileSize) {
        if (fileSize < K_COUNT) {
            return fileSize + "B";
        } else if (fileSize < M_COUNT) {
            return String.format("%dB (%.1fKB)", fileSize, fileSize / (double) K_COUNT);
        } else if (fileSize < G_COUNT) {
            return String.format("%dB (%.1fMB)", fileSize, fileSize / (double) M_COUNT);
        } else if (fileSize < T_COUNT) {
            return String.format("%dB (%.1fGB)", fileSize, fileSize / (double) G_COUNT);
        } else {
            return fileSize + "B (" + countWithCommas(Math.round((double) fileSize / T_COUNT)) + "TB)";
        }
    }

    /**
     * Formats a number of bytes as a human readable string.
     *
     * @param  fileSize the number of bytes to format
     * @return          a human readable string representing the number of bytes
     */
    public static String formatBytesAsHumanReadableString(long fileSize) {
        if (fileSize < K_COUNT) {
            return fileSize + "B";
        } else if (fileSize < M_COUNT) {
            return String.format("%.1fKB", fileSize / (double) K_COUNT);
        } else if (fileSize < G_COUNT) {
            return String.format("%.1fMB", fileSize / (double) M_COUNT);
        } else if (fileSize < T_COUNT) {
            return String.format("%.1fGB", fileSize / (double) G_COUNT);
        } else {
            return countWithCommas(Math.round((double) fileSize / T_COUNT)) + "TB";
        }
    }

    /**
     * Formats a number as a human readable string by inserting commas.
     *
     * @param  count the number to format
     * @return       a human readable string representing the number
     */
    public static String countWithCommas(long count) {
        return splitNonDecimalIntoParts("" + count);
    }

    /**
     * Formats a decimal number as a human readable string by inserting commas.
     *
     * @param  decimal the decimal number to format
     * @return         a human readable string representing the decimal number
     */
    public static String formatDecimal2dp(double decimal) {
        if (!Double.isFinite(decimal)) {
            return "" + decimal;
        }
        String str = String.format("%.2f", decimal);
        String prefix = "";
        String suffix = "";
        if (decimal < 0) {
            prefix = "-";
            str = str.substring(1);
        }
        int decimalIndex = str.indexOf('.');
        if (decimalIndex > 0) {
            suffix = str.substring(decimalIndex);
            str = str.substring(0, decimalIndex);
        }
        return prefix + splitNonDecimalIntoParts(str) + suffix;
    }

    private static String splitNonDecimalIntoParts(String str) {
        int length = str.length();
        int firstPartEnd = length % 3;

        List<String> parts = new ArrayList<>();
        if (firstPartEnd != 0) {
            parts.add(str.substring(0, firstPartEnd));
        }
        for (int i = firstPartEnd; i < length; i += 3) {
            parts.add(str.substring(i, i + 3));
        }
        return String.join(",", parts);
    }
}
