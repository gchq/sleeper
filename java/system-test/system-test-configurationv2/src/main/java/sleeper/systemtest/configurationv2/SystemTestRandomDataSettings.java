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
package sleeper.systemtest.configurationv2;

import java.util.Objects;

import static sleeper.systemtest.configurationv2.SystemTestProperty.MAX_ENTRIES_RANDOM_LIST;
import static sleeper.systemtest.configurationv2.SystemTestProperty.MAX_ENTRIES_RANDOM_MAP;
import static sleeper.systemtest.configurationv2.SystemTestProperty.MAX_RANDOM_INT;
import static sleeper.systemtest.configurationv2.SystemTestProperty.MAX_RANDOM_LONG;
import static sleeper.systemtest.configurationv2.SystemTestProperty.MIN_RANDOM_INT;
import static sleeper.systemtest.configurationv2.SystemTestProperty.MIN_RANDOM_LONG;
import static sleeper.systemtest.configurationv2.SystemTestProperty.RANDOM_BYTE_ARRAY_LENGTH;
import static sleeper.systemtest.configurationv2.SystemTestProperty.RANDOM_STRING_LENGTH;

public class SystemTestRandomDataSettings {

    private final int minInt;
    private final int maxInt;
    private final long minLong;
    private final long maxLong;
    private final int stringLength;
    private final int byteArrayLength;
    private final int maxMapEntries;
    private final int maxListEntries;

    private SystemTestRandomDataSettings(Builder builder) {
        minInt = builder.minInt;
        maxInt = builder.maxInt;
        minLong = builder.minLong;
        maxLong = builder.maxLong;
        stringLength = builder.stringLength;
        byteArrayLength = builder.byteArrayLength;
        maxMapEntries = builder.maxMapEntries;
        maxListEntries = builder.maxListEntries;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static SystemTestRandomDataSettings fromProperties(SystemTestPropertyValues properties) {
        return builder()
                .minInt(properties.getInt(MIN_RANDOM_INT))
                .maxInt(properties.getInt(MAX_RANDOM_INT))
                .minLong(properties.getLong(MIN_RANDOM_LONG))
                .maxLong(properties.getLong(MAX_RANDOM_LONG))
                .stringLength(properties.getInt(RANDOM_STRING_LENGTH))
                .byteArrayLength(properties.getInt(RANDOM_BYTE_ARRAY_LENGTH))
                .maxMapEntries(properties.getInt(MAX_ENTRIES_RANDOM_MAP))
                .maxListEntries(properties.getInt(MAX_ENTRIES_RANDOM_LIST))
                .build();
    }

    public static SystemTestRandomDataSettings fromDefaults() {
        return builder().build();
    }

    public int getMinInt() {
        return minInt;
    }

    public int getMaxInt() {
        return maxInt;
    }

    public long getMinLong() {
        return minLong;
    }

    public long getMaxLong() {
        return maxLong;
    }

    public int getStringLength() {
        return stringLength;
    }

    public int getByteArrayLength() {
        return byteArrayLength;
    }

    public int getMaxMapEntries() {
        return maxMapEntries;
    }

    public int getMaxListEntries() {
        return maxListEntries;
    }

    @Override
    public String toString() {
        return "SystemTestRandomDataSettings{minInt=" + minInt + ", maxInt=" + maxInt + ", minLong=" + minLong + ", maxLong=" + maxLong + ", stringLength=" + stringLength + ", byteArrayLength="
                + byteArrayLength + ", maxMapEntries=" + maxMapEntries + ", maxListEntries=" + maxListEntries + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(minInt, maxInt, minLong, maxLong, stringLength, byteArrayLength, maxMapEntries, maxListEntries);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SystemTestRandomDataSettings)) {
            return false;
        }
        SystemTestRandomDataSettings other = (SystemTestRandomDataSettings) obj;
        return minInt == other.minInt && maxInt == other.maxInt && minLong == other.minLong && maxLong == other.maxLong && stringLength == other.stringLength
                && byteArrayLength == other.byteArrayLength && maxMapEntries == other.maxMapEntries && maxListEntries == other.maxListEntries;
    }

    public static class Builder {
        private int minInt = defaultInt(MIN_RANDOM_INT);
        private int maxInt = defaultInt(MAX_RANDOM_INT);
        private long minLong = defaultLong(MIN_RANDOM_LONG);
        private long maxLong = defaultLong(MAX_RANDOM_LONG);
        private int stringLength = defaultInt(RANDOM_STRING_LENGTH);
        private int byteArrayLength = defaultInt(RANDOM_BYTE_ARRAY_LENGTH);
        private int maxMapEntries = defaultInt(MAX_ENTRIES_RANDOM_MAP);
        private int maxListEntries = defaultInt(MAX_ENTRIES_RANDOM_LIST);

        private Builder() {
        }

        public Builder minInt(int minInt) {
            this.minInt = minInt;
            return this;
        }

        public Builder maxInt(int maxInt) {
            this.maxInt = maxInt;
            return this;
        }

        public Builder minLong(long minLong) {
            this.minLong = minLong;
            return this;
        }

        public Builder maxLong(long maxLong) {
            this.maxLong = maxLong;
            return this;
        }

        public Builder stringLength(int stringLength) {
            this.stringLength = stringLength;
            return this;
        }

        public Builder byteArrayLength(int byteArrayLength) {
            this.byteArrayLength = byteArrayLength;
            return this;
        }

        public Builder maxMapEntries(int maxMapEntries) {
            this.maxMapEntries = maxMapEntries;
            return this;
        }

        public Builder maxListEntries(int maxListEntries) {
            this.maxListEntries = maxListEntries;
            return this;
        }

        public SystemTestRandomDataSettings build() {
            return new SystemTestRandomDataSettings(this);
        }
    }

    private static int defaultInt(SystemTestProperty property) {
        return Integer.parseInt(property.getDefaultValue());
    }

    private static long defaultLong(SystemTestProperty property) {
        return Long.parseLong(property.getDefaultValue());
    }
}
