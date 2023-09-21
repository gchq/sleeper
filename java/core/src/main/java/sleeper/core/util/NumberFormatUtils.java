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

package sleeper.core.util;

import java.util.ArrayList;
import java.util.List;

public class NumberFormatUtils {
    private NumberFormatUtils() {
    }

    private static final long K_COUNT = 1_000;
    private static final long M_COUNT = 1_000_000;
    private static final long G_COUNT = 1_000_000_000;
    private static final long T_COUNT = 1_000_000_000_000L;

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
            return countWithCommas(Math.round((double) fileSize / T_COUNT)) + "TB";
        }
    }

    public static String countWithCommas(long count) {
        return splitNonDecimalIntoParts("" + count);
    }

    public static String decimalWithCommas(String formatStr, double decimal) {
        String str = String.format(formatStr, decimal);
        int decimalIndex = str.indexOf('.');
        if (decimalIndex > 0) {
            return splitNonDecimalIntoParts(str.substring(0, decimalIndex)) + str.substring(decimalIndex);
        } else {
            return splitNonDecimalIntoParts(str);
        }
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
