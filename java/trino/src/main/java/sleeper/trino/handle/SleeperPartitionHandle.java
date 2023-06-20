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
package sleeper.trino.handle;

import io.trino.spi.connector.ConnectorPartitionHandle;

import sleeper.core.key.Key;

import java.util.Objects;

/**
 * This class holds the details about a single Trino partition.
 * <p>
 * In this implementation, a Trino partition is described by its lower-bound.
 */
public class SleeperPartitionHandle extends ConnectorPartitionHandle {
    private final Key trinoPartitionMin; // Inclusive

    public SleeperPartitionHandle(Key trinoPartitionMin) {
        this.trinoPartitionMin = trinoPartitionMin;
    }

    public Key getTrinoPartitionMin() {
        return trinoPartitionMin;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SleeperPartitionHandle that = (SleeperPartitionHandle) o;
        return Objects.equals(trinoPartitionMin, that.trinoPartitionMin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trinoPartitionMin);
    }

    @Override
    public String toString() {
        return "SleeperPartitionHandle{" +
                "trinoPartitionMin=" + trinoPartitionMin +
                '}';
    }
}
