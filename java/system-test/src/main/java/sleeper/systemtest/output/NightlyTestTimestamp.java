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

package sleeper.systemtest.output;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class NightlyTestTimestamp {
    private static final DateTimeFormatter S3_PREFIX_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
            .withZone(ZoneId.of("UTC"));
    private final Instant startTime;

    private NightlyTestTimestamp(Instant startTime) {
        this.startTime = startTime;
    }

    public static NightlyTestTimestamp from(String commandLineArgument) {
        return from(Instant.ofEpochSecond(Long.parseLong(commandLineArgument)));
    }

    public static NightlyTestTimestamp from(Instant startTime) {
        return new NightlyTestTimestamp(startTime);
    }

    public String getS3FolderName() {
        return S3_PREFIX_FORMAT.format(startTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NightlyTestTimestamp that = (NightlyTestTimestamp) o;
        return Objects.equals(startTime, that.startTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTime);
    }

    @Override
    public String toString() {
        return startTime.toString();
    }
}
