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
package sleeper.systemtest.nightly;

import org.junit.jupiter.api.Test;

import sleeper.systemtest.drivers.nightly.NightlyTestTimestamp;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class NightlyTestTimestampTest {

    @Test
    void shouldFormatStartTimeInS3Folder() {
        Instant startTime = Instant.parse("2023-05-04T09:35:00Z");
        NightlyTestTimestamp nightlyTestTimestamp = NightlyTestTimestamp.from(startTime);
        assertThat(nightlyTestTimestamp.getS3FolderName())
                .isEqualTo("20230504_093500");
    }

    @Test
    void shouldReadEpochSecondsFromCommandLineArgument() {
        assertThat(NightlyTestTimestamp.from("0"))
                .isEqualTo(NightlyTestTimestamp.from(Instant.EPOCH));
    }

    @Test
    void shouldReadNonZeroEpochSecondsFromCommandLineArgument() {
        assertThat(NightlyTestTimestamp.from("1683192900"))
                .isEqualTo(NightlyTestTimestamp.from(Instant.parse("2023-05-04T09:35:00Z")));
    }
}
